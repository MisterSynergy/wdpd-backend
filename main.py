from time import strftime, time
import pandas as pd

import patrolquery
import patrolplot
import patroldump


def df_info(dataframe:pd.DataFrame) -> None:
    print(dataframe.shape)
    print(dataframe.head())
    print()


def dump_update_timestamp(timestmp:float) -> None:
    update_file = '/data/project/wdpd/data/update.txt'
    with open(update_file, mode='w', encoding='utf8') as file_handle:
        file_handle.write(str(round(timestmp)))


def get_actions() -> dict[str, list[str]]:
    actions = {
        'claim' : ['wbsetclaim', 'wbsetclaim-create', 'wbsetclaim-update', 'wbsetclaimvalue',
                   'wbcreateclaim-create', 'wbcreateclaim-novalue', 'wbcreateclaim-somevalue',
                   'wbremoveclaims', 'wbremoveclaims-remove', 'wbsetclaim-update-rank',
                   'wbsetstatementrank', 'wbsetstatementrank-deprecated',
                   'wbsetstatementrank-normal', 'wbsetstatementrank-preferred'],
        'qualifier' : ['wbsetqualifier', 'wbsetqualifier-add', 'wbsetqualifier-update',
                       'wbsetclaim-update-qualifiers', 'wbremovequalifiers'],
        'reference' : ['wbsetreference', 'wbsetreference-add', 'wbsetclaim-update-references',
                       'wbremovereferences', 'wbremovereferences-remove'],
        'sitelink' : ['wbsetsitelink-add', 'wbsetsitelink-remove', 'wbsetsitelink-set'],
        'sitelinkmove' : ['clientsitelink-update', 'clientsitelink-remove'],
        'label' : ['wbsetlabel-add', 'wbsetlabel-remove', 'wbsetlabel-set'],
        'description' : ['wbsetdescription-add', 'wbsetdescription-remove',
                         'wbsetdescription-set' ],
        'alias' : ['wbsetaliases-add', 'wbsetaliases-remove', 'wbsetaliases-set',
                   'wbsetaliases-update'],
        'anyterms' : ['wbsetlabeldescriptionaliases'],
        'linktitles' : ['wblinktitles-create', 'wblinktitles-connect'],
        'editentity' : ['wbeditentity-create', 'wbeditentity-create-item',
                        'wbeditentity-override', 'wbeditentity-update',
                        'wbeditentity-update-languages', 'wbeditentity-update-languages-short',
                        'wbeditentity-update-languages-and-other-short'],
        'merge' : ['wbmergeitems-to', 'wbmergeitems-from', 'wbcreateredirect'],
        'revert' : ['undo', 'restore'],
        'none' : ['None']
    }

    actions['allclaims'] = actions['claim'] + actions['qualifier'] + actions['reference']
    actions['terms'] = actions['label'] + actions['description'] + actions['alias'] \
        + actions['anyterms']
    actions['allsitelinks'] = actions['sitelink'] + actions['sitelinkmove']

    return actions


def main() -> None:
    #### Aux variables
    start_timestamp = time()
    print(f'Script last executed: {strftime("%Y-%m-%d, %H:%M:%S (%Z)")}')
    debugging = False # True: prints some dataframe information
    actions=get_actions()

    #### Query data
    change_tags = patrolquery.query_change_tags()
    ores_scores = patrolquery.query_ores_scores()
    unpatrolled_changes = patrolquery.get_unpatrolled_changes(change_tags, ores_scores, actions)
    top_patrollers = patrolquery.query_top_patrollers(unpatrolled_changes['rc_timestamp'].min())
    wdcm_toplist = patrolquery.retrieve_highly_used_item_list()
    rfd_links = patrolquery.retrieve_wdrfd_links()
    unpatrolled_changes_not_ns0 = patrolquery.query_unpatrolled_changes_outside_main_namespace()
    translation_pages = patrolquery.query_translation_pages()

    patrol_progress = patrolquery.compile_patrol_progress(unpatrolled_changes, top_patrollers)

    #### debugging
    if debugging is True:
        dataframes = [unpatrolled_changes, change_tags, ores_scores, top_patrollers, wdcm_toplist]
        for dataframe in dataframes:
            df_info(dataframe)

    #### plot variables
    plot_window_days = 28
    filter_window = (unpatrolled_changes['time']>=pd.Timestamp.today().floor('D')-pd.to_timedelta(f'{plot_window_days:d} days')) \
        & (unpatrolled_changes['time']<pd.Timestamp.today().floor('D'))
    xticks_window = [ pd.Timestamp.today().floor('D') - pd.to_timedelta(f'{days:d} days') for days in range(28, -1, -7) ]
    xticklabels_window = [ pd.Timestamp.strftime(pd.Timestamp.today().floor('D') - pd.to_timedelta(f'{days:d} days'), '%Y-%m-%d') for days in range(28, -1, -7) ]

    plot_params:patrolplot.PlotParamsDict = {
        'plot_window_days' : plot_window_days,
        'filter_window' : filter_window,
        'xticks_window' : xticks_window,
        'xticklabels_window' : xticklabels_window,
        'plot_path' : '/data/project/wdpd/plots/',
        'figsize_standard' : (6, 4),
        'figsize_tall' : (6, 8),
        'figsize_wide' : (9, 4),
        'figsize_heatmap' : (6, 4.4),
        'qid_bin_size' : 1000000,
        'qid_max' : 120000000
    }

    ores_models = [ 'oresc_damaging', 'oresc_goodfaith' ]

    #### Plotting
    ymax = patrolplot.plot_edits_by_date(unpatrolled_changes, plot_params)
    patrolplot.plot_edits_by_weekday(unpatrolled_changes, plot_params, ymax)
    patrolplot.plot_edits_by_hour(unpatrolled_changes, plot_params)

    ymax = patrolplot.plot_patrol_status_by_date(unpatrolled_changes, plot_params)
    patrolplot.plot_patrol_status_by_weekday(unpatrolled_changes, plot_params, ymax)
    patrolplot.plot_patrol_status_by_hour(unpatrolled_changes, plot_params)

    ymax = patrolplot.plot_editor_status_by_date(unpatrolled_changes, plot_params)
    patrolplot.plot_editor_status_by_weekday(unpatrolled_changes, plot_params, ymax)
    patrolplot.plot_editor_status_by_hour(unpatrolled_changes, plot_params)

    # technical edit characteristics
    patrolplot.plot_unpatrolled_actions_by_date(unpatrolled_changes, plot_params)
    patrolplot.plot_reverted_by_date(unpatrolled_changes, change_tags, plot_params)
    patrolplot.plot_qid_bin_by_revisions(unpatrolled_changes, plot_params)
    patrolplot.plot_qid_bin_by_item(unpatrolled_changes, plot_params)

    # editorial edit characteristics
    patrolplot.plot_broad_action_by_date(unpatrolled_changes, plot_params)
    patrolplot.plot_broad_action_by_patrol_status(unpatrolled_changes, plot_params)
    patrolplot.plot_language_by_patrol_status(unpatrolled_changes, plot_params, actions['terms'])
    patrolplot.plot_property_by_patrol_status(unpatrolled_changes, plot_params, actions['allclaims'])
    patrolplot.plot_sitelink_by_patrol_status(unpatrolled_changes, plot_params, actions['allsitelinks'])
    patrolplot.plot_other_actions_by_patrol_status(
        unpatrolled_changes,
        plot_params,
        actions['editentity']+actions['linktitles']+actions['merge']+actions['revert']+actions['none']
    )

    # ORES correlation histograms
    patrolplot.plot_ores_hist_by_editor_type(unpatrolled_changes, plot_params, ores_models)
    patrolplot.plot_ores_hist_by_action(unpatrolled_changes, plot_params, ores_models)
    patrolplot.plot_ores_hist_by_reverted(unpatrolled_changes, plot_params, ores_models)
    patrolplot.plot_ores_hist_by_language(unpatrolled_changes, plot_params, ores_models, actions['terms'])
    patrolplot.plot_ores_hist_by_term_type(unpatrolled_changes, plot_params, ores_models)
    patrolplot.plot_ores_heatmaps(unpatrolled_changes, plot_params)

    # worklist
    patrolplot.plot_remaining_by_date(unpatrolled_changes, plot_params)

    #### Dump worklists
    filt_today = (unpatrolled_changes['time']>=pd.Timestamp.today().floor('D'))
    filt_3d_rolling = (unpatrolled_changes['time']>=pd.Timestamp.today()-pd.to_timedelta('3 days'))
    filt_7d_rolling = (unpatrolled_changes['time']>=pd.Timestamp.today()-pd.to_timedelta('7 days'))
    filt_14d_rolling = (unpatrolled_changes['time']>=pd.Timestamp.today()-pd.to_timedelta('14 days'))
    filt_all = (unpatrolled_changes['time']>=pd.Timestamp.today()-pd.to_timedelta('31 days'))
    filt_suggested = unpatrolled_changes['suggested_edit'].notna()

    patroldump.dump_worklist(unpatrolled_changes, filt_today, 'today')
    patroldump.dump_worklist(unpatrolled_changes, filt_3d_rolling, '3d')
    patroldump.dump_worklist(unpatrolled_changes, filt_7d_rolling, '7d')
    patroldump.dump_worklist(unpatrolled_changes, filt_14d_rolling, '14d')
    patroldump.dump_worklist(unpatrolled_changes, filt_all, 'all')
    patroldump.dump_worklist(unpatrolled_changes, filt_suggested, 'suggested-edit')

    patroldump.dump_ores_worklist_unregistered(unpatrolled_changes, 0.9, 10)
    patroldump.dump_ores_worklist_registered(unpatrolled_changes, 0.7, 10)
    patroldump.dump_items_with_many_revisions(unpatrolled_changes, 107000000)
    patroldump.dump_users_with_many_creations(unpatrolled_changes)
    patroldump.dump_highly_used_items(unpatrolled_changes, wdcm_toplist, 500)
    patroldump.term_dump_processor(unpatrolled_changes, actions['terms'])
    patroldump.term_in_editentity_dump_processor(unpatrolled_changes)
    patroldump.term_in_editentity_create_dump_processor(unpatrolled_changes)
    patroldump.project_sitelinks_dump_processor(unpatrolled_changes, actions['sitelink'])
    patroldump.project_pagemoves_dump_processor(unpatrolled_changes, actions['sitelinkmove'])
    patroldump.project_pageremovals_dump_processor(unpatrolled_changes, actions['sitelinkmove'])
    patroldump.editentity_dump_processor(unpatrolled_changes, actions['editentity'])
    patroldump.dump_uncategorizable_editsummaries(unpatrolled_changes)
    patroldump.dump_top_patrollers(unpatrolled_changes, top_patrollers)
    patroldump.dump_change_tags_list(change_tags)
    patroldump.dump_rfd_linked_items(unpatrolled_changes, rfd_links)
    patroldump.dump_actions(actions)

    #### Patrol progress statistics
    patroldump.make_all_patrol_progress_stats(patrol_progress)
    patrolplot.make_all_patrol_progress_stats(patrol_progress, plot_params)

    #### Not ns0
    patroldump.make_not_ns0_stats(unpatrolled_changes_not_ns0, translation_pages)

    # this is relatively expensive:
    patroldump.property_dump_processor(unpatrolled_changes, actions['allclaims'])

    dump_update_timestamp(start_timestamp)


if __name__ == '__main__':
    main()
