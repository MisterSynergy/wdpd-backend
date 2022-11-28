import logging
import logging.config
from os.path import expanduser
from time import time

import pandas as pd

logging.config.fileConfig('./logging.conf')

import wdpd.query as query
import wdpd.plot as plot
import wdpd.dump as dump
from wdpd.config import DEBUG, PLOT_WINDOW_DAYS
from wdpd.helper import dump_update_timestamp, get_actions, init_directories, df_info


LOG = logging.getLogger()


def main() -> None:
    LOG.info('Script execution started')

    #### Aux variables
    start_timestamp = time()
    actions=get_actions()
    init_directories()

    #### Query data
    LOG.info('Start querying data')
    change_tags = query.query_change_tags()
    ores_scores = query.query_ores_scores()
    unpatrolled_changes = query.get_unpatrolled_changes(change_tags, ores_scores, actions)
    top_patrollers = query.query_top_patrollers(int(unpatrolled_changes['time'].min().strftime('%Y%m%d%H%M%S')))
    wdcm_toplist = query.retrieve_highly_used_item_list()
    rfd_links = query.retrieve_wdrfd_links()
    unpatrolled_changes_not_ns0 = query.query_unpatrolled_changes_outside_main_namespace()
    translation_pages = query.query_translation_pages()

    patrol_progress = query.compile_patrol_progress(unpatrolled_changes, top_patrollers)

    #### debugging
    if DEBUG is True:
        dataframes = [unpatrolled_changes, change_tags, ores_scores, top_patrollers, wdcm_toplist, unpatrolled_changes_not_ns0, patrol_progress]
        for dataframe in dataframes:
            df_info(dataframe)

    #### plot variables
    LOG.info('Start plotting data')
    filter_window = (unpatrolled_changes['time']>=pd.Timestamp.today().floor('D')-pd.to_timedelta(f'{PLOT_WINDOW_DAYS:d} days')) \
        & (unpatrolled_changes['time']<pd.Timestamp.today().floor('D'))
    xticks_window = [ pd.Timestamp.today().floor('D') - pd.to_timedelta(f'{days:d} days') for days in range(PLOT_WINDOW_DAYS, -1, -7) ]
    xticklabels_window = [ pd.Timestamp.strftime(pd.Timestamp.today().floor('D') - pd.to_timedelta(f'{days:d} days'), '%Y-%m-%d') for days in range(PLOT_WINDOW_DAYS, -1, -7) ]

    plot_params:plot.PlotParamsDict = {
        'filter_window' : filter_window,
        'xticks_window' : xticks_window,
        'xticklabels_window' : xticklabels_window,
    }

    #### Plotting
    ymax = plot.plot_edits_by_date(unpatrolled_changes, plot_params)
    plot.plot_edits_by_weekday(unpatrolled_changes, plot_params, ymax)
    plot.plot_edits_by_hour(unpatrolled_changes, plot_params)

    ymax = plot.plot_patrol_status_by_date(unpatrolled_changes, plot_params)
    plot.plot_patrol_status_by_weekday(unpatrolled_changes, plot_params, ymax)
    plot.plot_patrol_status_by_hour(unpatrolled_changes, plot_params)

    ymax = plot.plot_editor_status_by_date(unpatrolled_changes, plot_params)
    plot.plot_editor_status_by_weekday(unpatrolled_changes, plot_params, ymax)
    plot.plot_editor_status_by_hour(unpatrolled_changes, plot_params)

    # technical edit characteristics
    plot.plot_unpatrolled_actions_by_date(unpatrolled_changes, plot_params)
    plot.plot_reverted_by_date(unpatrolled_changes, change_tags, plot_params)
    plot.plot_qid_bin_by_revisions(unpatrolled_changes)
    plot.plot_qid_bin_by_item(unpatrolled_changes)

    # editorial edit characteristics
    plot.plot_broad_action_by_date(unpatrolled_changes, plot_params)
    plot.plot_broad_action_by_patrol_status(unpatrolled_changes, plot_params)
    plot.plot_language_by_patrol_status(unpatrolled_changes, actions['terms'])
    plot.plot_property_by_patrol_status(unpatrolled_changes, actions['allclaims'])
    plot.plot_sitelink_by_patrol_status(unpatrolled_changes, actions['allsitelinks'])
    plot.plot_other_actions_by_patrol_status(
        unpatrolled_changes,
        actions['editentity']+actions['linktitles']+actions['merge']+actions['revert']+actions['none']
    )

    # ORES correlation histograms
    plot.plot_ores_hist_by_editor_type(unpatrolled_changes)
    plot.plot_ores_hist_by_action(unpatrolled_changes)
    plot.plot_ores_hist_by_reverted(unpatrolled_changes)
    plot.plot_ores_hist_by_language(unpatrolled_changes, actions['terms'])
    plot.plot_ores_hist_by_term_type(unpatrolled_changes)
    plot.plot_ores_heatmaps(unpatrolled_changes)

    # worklist
    plot.plot_remaining_by_date(unpatrolled_changes, plot_params)

    #### Dump worklists
    LOG.info('Start dumping data')
    filt_today = (unpatrolled_changes['time']>=pd.Timestamp.today().floor('D'))
    filt_3d_rolling = (unpatrolled_changes['time']>=pd.Timestamp.today()-pd.to_timedelta('3 days'))
    filt_7d_rolling = (unpatrolled_changes['time']>=pd.Timestamp.today()-pd.to_timedelta('7 days'))
    filt_14d_rolling = (unpatrolled_changes['time']>=pd.Timestamp.today()-pd.to_timedelta('14 days'))
    filt_all = (unpatrolled_changes['time']>=pd.Timestamp.today()-pd.to_timedelta('31 days'))
    filt_suggested = unpatrolled_changes['suggested_edit'].notna()

    dump.dump_worklist(unpatrolled_changes, filt_today, 'today')
    dump.dump_worklist(unpatrolled_changes, filt_3d_rolling, '3d')
    dump.dump_worklist(unpatrolled_changes, filt_7d_rolling, '7d')
    dump.dump_worklist(unpatrolled_changes, filt_14d_rolling, '14d')
    dump.dump_worklist(unpatrolled_changes, filt_all, 'all')
    dump.dump_worklist(unpatrolled_changes, filt_suggested, 'suggested-edit')

    dump.dump_ores_worklist_unregistered(unpatrolled_changes)
    dump.dump_ores_worklist_registered(unpatrolled_changes)
    dump.dump_items_with_many_revisions(unpatrolled_changes)
    dump.dump_users_with_many_creations(unpatrolled_changes)
    dump.dump_highly_used_items(unpatrolled_changes, wdcm_toplist)
    dump.term_dump_processor(unpatrolled_changes, actions['terms'])
    dump.term_in_editentity_dump_processor(unpatrolled_changes)
    dump.term_in_editentity_create_dump_processor(unpatrolled_changes)
    dump.project_sitelinks_dump_processor(unpatrolled_changes, actions['sitelink'])
    dump.project_pagemoves_dump_processor(unpatrolled_changes, actions['sitelinkmove'])
    dump.project_pageremovals_dump_processor(unpatrolled_changes, actions['sitelinkmove'])
    dump.editentity_dump_processor(unpatrolled_changes, actions['editentity'])
    dump.dump_uncategorizable_editsummaries(unpatrolled_changes)
    dump.dump_top_patrollers(unpatrolled_changes, top_patrollers)
    dump.dump_change_tags_list(change_tags)
    dump.dump_rfd_linked_items(unpatrolled_changes, rfd_links)
    dump.dump_actions(actions)

    #### Patrol progress statistics
    dump.make_all_patrol_progress_stats(patrol_progress)
    plot.make_all_patrol_progress_stats(patrol_progress)

    #### Not ns0
    dump.make_not_ns0_stats(unpatrolled_changes_not_ns0, translation_pages)

    # this is relatively expensive:
    dump.property_dump_processor(unpatrolled_changes, actions['allclaims'])

    dump_update_timestamp(start_timestamp)


if __name__ == '__main__':
    main()
