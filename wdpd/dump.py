from glob import glob
import logging

from numpy import mean
import pandas as pd

from .config import DATAPATH
from .helper import delete_file, wdqs_query


LOG = logging.getLogger(__name__)


#### functions not for export
def dump_dataframe(dataframe:pd.DataFrame, filename:str, head_limit:int=50) -> None:
    dataframe.to_csv(DATAPATH + filename.format(mode='full'), sep='\t')
    dataframe.head(head_limit).to_csv(DATAPATH + filename.format(mode='head'), sep='\t')

    LOG.info(f'Dumped DataFrame to "{filename.format(mode="full|head")}"')


def dump_terms(unpatrolled_changes:pd.DataFrame, term_actions:list[str], language:str) -> None:
    filt = (unpatrolled_changes['rc_patrolled']==0) \
        & (unpatrolled_changes['editsummary-magic-action'].isin(term_actions)) \
        & (unpatrolled_changes['editsummary-magic-param1'] == language)
    fields = ['rc_id', 'rc_timestamp', 'rc_title', 'rc_this_oldid', 'actor_name',
              'oresc_damaging', 'oresc_goodfaith', 'editsummary-magic-action']
    filename = f'term/worklist-{language}-terms-{{mode}}.tsv'
    dump_dataframe(unpatrolled_changes.loc[filt, fields].sort_values(by='actor_name'), filename)

    LOG.info(f'Dumped terms for language "{language}"')


def dump_terms_in_editentity(unpatrolled_changes:pd.DataFrame, language:str) -> None:
    filt = (unpatrolled_changes['rc_patrolled']==0) \
        & (unpatrolled_changes['editsummary-magic-action-broad']=='editentity') \
        & (unpatrolled_changes['editsummary-magic-param2'].notna()) \
        & (unpatrolled_changes['editsummary-magic-param2'].apply(
            lambda x : (language in str(x).split(', '))
        ))
    fields = ['rc_id', 'rc_timestamp', 'rc_title', 'rc_this_oldid', 'actor_name',
              'oresc_damaging', 'oresc_goodfaith', 'editsummary-magic-action']
    filename = f'termee/worklist-{language}-terms-in-editentity-{{mode}}.tsv'
    dump_dataframe(unpatrolled_changes.loc[filt, fields].sort_values(by='actor_name'), filename)

    LOG.info(f'Dumped terms in editentity for language "{language}"')


def dump_terms_in_editentity_create(unpatrolled_changes:pd.DataFrame, language:str) -> None:
    filt = (unpatrolled_changes['rc_patrolled']==0) \
        & (unpatrolled_changes['editsummary-magic-action']=='wbeditentity-create') \
        & (unpatrolled_changes['editsummary-magic-param1'] == language)
    fields = ['rc_id', 'rc_timestamp', 'rc_title', 'rc_this_oldid', 'actor_name',
              'oresc_damaging', 'oresc_goodfaith', 'editsummary-magic-action']
    filename = f'termeec/worklist-{language}-terms-in-editentity-create-{{mode}}.tsv'
    dump_dataframe(unpatrolled_changes.loc[filt, fields].sort_values(by='actor_name'), filename)

    LOG.info(f'Dumped terms in editentity creations for language "{language}"')


def dump_project_sitelink_changes(unpatrolled_changes:pd.DataFrame, \
                                  sitelink_actions:list[str], project:str) -> None:
    filt = (unpatrolled_changes['rc_patrolled']==0) \
        & (unpatrolled_changes['editsummary-magic-action'].isin(sitelink_actions)) \
        & (unpatrolled_changes['editsummary-magic-param1'] == project)
    fields = ['rc_id', 'rc_timestamp', 'rc_title', 'rc_this_oldid', 'actor_name',
              'oresc_damaging', 'oresc_goodfaith', 'editsummary-magic-action']
    filename = f'page/worklist-{project}-page-{{mode}}.tsv'
    dump_dataframe(unpatrolled_changes.loc[filt, fields], filename)

    LOG.info(f'Dumped sitelink changes for project "{project}"')


def dump_project_pagemoves(unpatrolled_changes:pd.DataFrame, sitelink_actions:list[str], \
                           project:str) -> None:
    filt = (unpatrolled_changes['rc_patrolled']==0) \
        & (unpatrolled_changes['editsummary-magic-action'].isin(sitelink_actions)) \
        & (unpatrolled_changes['editsummary-magic-param1'] == project)
    fields = ['rc_id', 'rc_timestamp', 'rc_title', 'rc_this_oldid', 'actor_name',
              'oresc_damaging', 'oresc_goodfaith', 'editsummary-magic-action']
    filename = f'pagemove/worklist-{project}-pagemove-{{mode}}.tsv'
    dump_dataframe(unpatrolled_changes.loc[filt, fields], filename)

    LOG.info(f'Dumped page moves changes for project "{project}"')


def dump_project_pageremovals(unpatrolled_changes:pd.DataFrame, sitelink_actions:list[str], \
                              project:str) -> None:
    filt = (unpatrolled_changes['rc_patrolled']==0) \
        & (unpatrolled_changes['editsummary-magic-action'].isin(sitelink_actions)) \
        & (unpatrolled_changes['editsummary-magic-param1'].isna()) \
        & (unpatrolled_changes['editsummary-magic-param2'] == project)
    fields = ['rc_id', 'rc_timestamp', 'rc_title', 'rc_this_oldid', 'actor_name',
              'oresc_damaging', 'oresc_goodfaith', 'editsummary-magic-action']
    filename = f'pageremoval/worklist-{project}-pageremoval-{{mode}}.tsv'
    dump_dataframe(unpatrolled_changes.loc[filt, fields], filename)

    LOG.info(f'Dumped pagelink removals for project "{project}"')


def dump_editentity_changes(unpatrolled_changes:pd.DataFrame, magic_action:str) -> None:
    filt = (unpatrolled_changes['rc_patrolled']==0) \
        & (unpatrolled_changes['editsummary-magic-action']==magic_action)
    fields = ['rc_id', 'rc_timestamp', 'rc_title', 'rc_this_oldid', 'actor_name',
              'oresc_damaging', 'oresc_goodfaith', 'editsummary-magic-action']
    filename = f'editentity/worklist-{magic_action}-{{mode}}.tsv'
    dump_dataframe(unpatrolled_changes.loc[filt, fields], filename)

    LOG.info(f'Dumped editentity changes for action "{magic_action}"')


def dump_property_changes(unpatrolled_changes:pd.DataFrame, claim_actions:list[str], prop:str) -> None:
    filt = (unpatrolled_changes['rc_patrolled']==0) \
        & (unpatrolled_changes['editsummary-magic-action'].isin(claim_actions)) \
        & (unpatrolled_changes['editsummary-free-property']==prop)
    fields = ['rc_id', 'rc_timestamp', 'rc_title', 'rc_this_oldid', 'actor_name',
              'oresc_damaging', 'oresc_goodfaith', 'editsummary-magic-action']
    filename = f'property/worklist-{prop}-{{mode}}.tsv'
    dump_dataframe(unpatrolled_changes.loc[filt, fields], filename)

    LOG.info(f'Dumped property changes for property "{prop}"')


#### functions for export
def dump_worklist(unpatrolled_changes:pd.DataFrame, filt:pd.Series, name:str='') -> None:
    fields_tmp = ['actor_name', 'rc_patrolled', 'reverted']
    tmp = unpatrolled_changes.loc[filt, fields_tmp].groupby(
        by=['actor_name', 'rc_patrolled']
    ).size().reset_index(name='edits')

    patrol_done = tmp.loc[tmp['rc_patrolled']==1, ['actor_name', 'edits']].sort_values(
        by='edits', ascending=False
    )
    patrol_missing = tmp.loc[tmp['rc_patrolled']==0, ['actor_name', 'edits']].sort_values(
        by='edits', ascending=False
    )

    filt_reverted = (~unpatrolled_changes['reverted'].isna())
    fields_reverted = ['actor_name', 'rc_id']
    reverted_cnt = unpatrolled_changes.loc[filt_reverted, fields_reverted].groupby(
        by='actor_name'
    ).count()
    reverted_cnt.rename(columns={'rc_id' : 'reverted'}, inplace=True)

    filt_new_item = (unpatrolled_changes['rc_source']=='mw.new')
    fields_new_item = ['actor_name', 'rc_id']
    new_item_cnt = unpatrolled_changes.loc[filt_new_item, fields_new_item].groupby(
        by='actor_name'
    ).count()
    new_item_cnt.rename(columns={'rc_id' : 'created'}, inplace=True)

    patrol_stats = patrol_done.merge(
        right=patrol_missing,
        on='actor_name',
        how='outer',
        suffixes=('_patr', '_unpatr')
    ).merge(
        right=reverted_cnt,
        on='actor_name',
        how='left'
    ).merge(
        right=new_item_cnt,
        on='actor_name',
        how='left'
    ).fillna(0).sort_values(by=['edits_unpatr', 'edits_patr'], ascending=False)
    patrol_stats['edits'] = patrol_stats['edits_patr'] + patrol_stats['edits_unpatr']
    patrol_stats['patrol_ratio'] = round(patrol_stats['edits_patr']/patrol_stats['edits']*100, 2)
    patrol_stats['reverted_ratio'] = round(patrol_stats['reverted']/patrol_stats['edits']*100, 2)

    if name!='':
        name = '-' + name

    filename = f'worklist-{{mode}}{name}.tsv'
    dump_dataframe(patrol_stats, filename)

    LOG.info(f'Dumped worklist {name}')


def dump_ores_worklist_unregistered(unpatrolled_changes:pd.DataFrame, min_ores_score:float=0.9, \
                                    min_edits:int=10) -> None:
    filt = (unpatrolled_changes['rc_patrolled']==0) & (unpatrolled_changes['actor_user'].isna())
    fields = ['rc_id', 'actor_name', 'oresc_damaging']

    damaging_highscores = unpatrolled_changes.loc[filt, fields].groupby(
        by='actor_name'
    ).agg(
        func={ 'rc_id' : len, 'oresc_damaging' : lambda x : x.sum() / x.size }
    ).sort_values(by='oresc_damaging', ascending=False)

    filter_highscore = (damaging_highscores['oresc_damaging']>=min_ores_score) \
        & (damaging_highscores['rc_id']>=min_edits)

    filename = 'worklist-ores-{mode}.tsv'
    dump_dataframe(damaging_highscores.loc[filter_highscore], filename)

    LOG.info('Dumped ORES worklist for unregistered users')


def dump_ores_worklist_registered(unpatrolled_changes:pd.DataFrame, min_ores_score:float=0.7, \
                                  min_edits:int=10) -> None:
    filt = (unpatrolled_changes['rc_patrolled']==0) & (unpatrolled_changes['actor_user'].notna())
    fields = ['rc_id', 'actor_name', 'oresc_damaging']

    damaging_highscores = unpatrolled_changes.loc[filt, fields].groupby(
        by='actor_name'
    ).agg(
        func={ 'rc_id' : len, 'oresc_damaging' : lambda x : x.sum() / x.size }
    ).sort_values(by='oresc_damaging', ascending=False)

    filter_highscore = (damaging_highscores['oresc_damaging']>=min_ores_score) \
        & (damaging_highscores['rc_id']>=min_edits)

    filename = 'worklist-ores-{mode}-registered.tsv'
    dump_dataframe(damaging_highscores.loc[filter_highscore], filename)

    LOG.info('Dumped ORES worklist for registered users')


def dump_items_with_many_revisions(unpatrolled_changes:pd.DataFrame, \
                                   max_num_title:int=None) -> None:
    if max_num_title is None:
        filt = (unpatrolled_changes['rc_patrolled']==0)
    else:
        filt = (unpatrolled_changes['rc_patrolled']==0) \
            & (unpatrolled_changes['num_title']<max_num_title)
    fields = ['rc_title', 'rc_patrolled', 'reverted']
    many_revisions = unpatrolled_changes.loc[filt, fields].groupby(
        by=['rc_title']
    ).size().reset_index(name='edits').sort_values(by='edits', ascending=False)
    filename = 'worklist-items-many-revisions-{mode}.tsv'
    dump_dataframe(many_revisions, filename)

    LOG.info('Dumped items with many revisions')


def dump_users_with_many_creations(unpatrolled_changes:pd.DataFrame) -> None:
    filt = (unpatrolled_changes['rc_patrolled']==0) & (unpatrolled_changes['rc_source']!='mw.edit')
    fields = ['actor_name', 'rc_id']
    many_creations = unpatrolled_changes.loc[filt, fields].groupby(
        by=['actor_name']
    ).count().sort_values(by='rc_id', ascending=False)
    filename = 'worklist-users-with-many-creations-{mode}.tsv'
    dump_dataframe(many_creations, filename)

    LOG.info('Dumped users with many creations')


def dump_highly_used_items(unpatrolled_changes:pd.DataFrame, wdcm_toplist:pd.DataFrame, \
                           min_entity_usage_count:int=500) -> None:
    filt = (unpatrolled_changes['rc_patrolled']==0)
    toplist_unpatrolled_changes = unpatrolled_changes.loc[filt].merge(
        right=wdcm_toplist.loc[wdcm_toplist['entity_usage_count']>=min_entity_usage_count],
        how='inner',
        left_on='rc_title',
        right_on='qid'
    ).value_counts(subset=['rc_title', 'entity_usage_count'], sort=True, ascending=False)
    filename = 'worklist-highly-used-items-{mode}.tsv'
    dump_dataframe(toplist_unpatrolled_changes.sort_index(level=1, ascending=False), filename)

    LOG.info('Dumped highly used items with edits')


def dump_uncategorizable_editsummaries(unpatrolled_changes:pd.DataFrame) -> None:
    filt = (unpatrolled_changes['editsummary-magic-action-broad']=='NO_CAT')
    fields = ['rc_id', 'rc_timestamp', 'rc_title', 'rc_this_oldid', 'actor_name']
    filename = 'worklist-uncategorizable-editsummaries-{mode}.tsv'
    dump_dataframe(unpatrolled_changes.loc[filt, fields], filename)

    LOG.info('Dumped edits with uncategorizable edit summaries')


def dump_top_patrollers(unpatrolled_changes:pd.DataFrame, top_patrollers:pd.DataFrame) -> None:
    filt = (top_patrollers['rc_curid']>=unpatrolled_changes['rc_this_oldid'].min())
    patrolled_revisions = top_patrollers.loc[filt].shape[0]
    total_revisions = unpatrolled_changes.shape[0]

    filt_today = (unpatrolled_changes['time']>=pd.Timestamp.today().floor('D'))
    filt_patrolled = (unpatrolled_changes['rc_patrolled']==1)
    today_patrolled_revisions = unpatrolled_changes.loc[filt_today & filt_patrolled].shape[0]
    today_total_revisions = unpatrolled_changes.loc[filt_today].shape[0]

    progress = f'Currently {patrolled_revisions} out of {total_revisions} revisions are patrolled' \
               f' ({patrolled_revisions/total_revisions*100:.1f}%); {total_revisions-patrolled_revisions}' \
                ' revisions are not yet patrolled'
    open(DATAPATH + 'progress.txt', mode='w', encoding='utf8').write(progress)
    open(DATAPATH + 'progressRaw.txt', mode='w', encoding='utf8').write(f'{patrolled_revisions}\t{total_revisions}')

    today_progress = f'Today {today_patrolled_revisions} out of {today_total_revisions} revisions' \
                     f' are patrolled ({today_patrolled_revisions/today_total_revisions*100:.1f}%);' \
                     f' {today_total_revisions-today_patrolled_revisions} revisions are not yet patrolled'
    open(DATAPATH + 'todayProgress.txt', mode='w', encoding='utf8').write(today_progress)
    open(DATAPATH + 'todayProgressRaw.txt', mode='w', encoding='utf8').write(f'{today_patrolled_revisions}\t{today_total_revisions}')

    top_patrollers_grouped = top_patrollers.loc[filt, ['log_id', 'actor_name']].groupby(
        by='actor_name'
    ).size().reset_index(name='patrols')
    top_patrollers_grouped['patrols_relative'] = round(
        top_patrollers_grouped['patrols'] / top_patrollers_grouped['patrols'].sum() * 100,
        1
    )
    top_patrollers_grouped.sort_values(by='patrols', ascending=False, inplace=True)

    dump_dataframe(top_patrollers_grouped, 'top-patrollers-{mode}.tsv', 20)

    LOG.info('Dumped top patrollers')


def dump_change_tags_list(change_tags:pd.DataFrame) -> None:
    change_tags[['ctd_name']].value_counts().to_csv(DATAPATH + 'change-tags.tsv', sep='\t')

    LOG.info('Dumped change tag list')


def dump_rfd_linked_items(unpatrolled_changes:pd.DataFrame, rfdlinks:list[str]) -> None:
    filt = unpatrolled_changes['rc_title'].isin(rfdlinks)
    rfd_linked = unpatrolled_changes.loc[filt, ['rc_title', 'rc_patrolled']].groupby(
        by='rc_title'
    ).agg(
        {'rc_patrolled': mean, 'rc_title' : len}
    )
    rfd_linked.rename(columns={ 'rc_title' : 'cnt' }, inplace=True)

    ### output to file
    query = f"""SELECT
  ?wditem
  ?itemLabel
  ?statements
  ?identifiers
  ?sitelinks
  (COUNT(DISTINCT ?backlink) AS ?backlinks)
WHERE {{
  VALUES ?item {{ wd:{" wd:".join(rfd_linked.index.tolist())} }}
  ?item wikibase:statements ?statements;
        wikibase:identifiers ?identifiers;
        wikibase:sitelinks ?sitelinks .
  OPTIONAL {{
    ?backlink ?any ?item .
    FILTER(?any NOT IN (schema:about, owl:sameAs)) .
  }}
  BIND(STRAFTER(STR(?item), 'entity/') AS ?wditem) .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language 'en' }}
}} GROUP BY ?wditem ?itemLabel ?statements ?identifiers ?sitelinks"""

    wdqs_data = wdqs_query(query)

    rfd_linked = rfd_linked.merge(right=wdqs_data, left_on='rc_title', right_on='wditem')
    rfd_linked.to_csv(DATAPATH + 'wdrfd-linked-full.tsv', sep='\t')

    LOG.info('Dumped items linked from WD:RfD')


def dump_actions(actions:dict[str, list[str]]) -> None:
    with open(DATAPATH + 'actions.txt', mode='w', encoding='utf8') as file_handle:
        for key, value in actions.items():
            file_handle.write(f'{key}\t{", ".join(value)}\n')

    LOG.info('Dumped actions')


#### dump processors for export
def term_dump_processor(unpatrolled_changes:pd.DataFrame, term_actions:list[str]) -> None:
    existing_dumps = glob(DATAPATH + 'term/worklist-*-terms-head.tsv')
    filt = (unpatrolled_changes['rc_patrolled']==0) \
        & (unpatrolled_changes['editsummary-magic-action'].isin(term_actions))
    languages = unpatrolled_changes.loc[filt, 'editsummary-magic-param1'].drop_duplicates().tolist()

    for existing_dump in existing_dumps:
        language_code = existing_dump[38:-15]
        if language_code not in languages: # hacky
            delete_file(existing_dump)
            delete_file(existing_dump.replace('head.tsv', 'full.tsv'))

    for language in languages:
        dump_terms(unpatrolled_changes, term_actions, language)

    LOG.info('Dumped term edits')


def term_in_editentity_dump_processor(unpatrolled_changes:pd.DataFrame) -> None:
    existing_dumps = glob(DATAPATH + 'termee/worklist-*-terms-in-editentity-head.tsv')
    filt = (unpatrolled_changes['rc_patrolled']==0) \
        & (unpatrolled_changes['editsummary-magic-action-broad']=='editentity')
    fields = 'editsummary-magic-param2'
    lang_combis = unpatrolled_changes.loc[filt, fields].drop_duplicates().tolist()

    languages = []
    for lang_combi in lang_combis:
        languages.extend(str(lang_combi).split(', '))
    languages = list(set(languages))

    for existing_dump in existing_dumps:
        language_code = existing_dump[40:-29]
        if language_code not in languages: # hacky
            delete_file(existing_dump)
            delete_file(existing_dump.replace('head.tsv', 'full.tsv'))

    for language in languages:
        dump_terms_in_editentity(unpatrolled_changes, language)

    LOG.info('Dumped term in editentity edits')


def term_in_editentity_create_dump_processor(unpatrolled_changes:pd.DataFrame) -> None:
    existing_dumps = glob(DATAPATH + 'termeec/worklist-*-terms-in-editentity-create-head.tsv')
    filt = (unpatrolled_changes['rc_patrolled']==0) \
        & (unpatrolled_changes['editsummary-magic-action']=='wbeditentity-create')
    languages = unpatrolled_changes.loc[filt, 'editsummary-magic-param1'].drop_duplicates().tolist()

    for existing_dump in existing_dumps:
        language_code = existing_dump[41:-36]
        if language_code not in languages: # hacky
            delete_file(existing_dump)
            delete_file(existing_dump.replace('head.tsv', 'full.tsv'))

    for language in languages:
        dump_terms_in_editentity_create(unpatrolled_changes, language)

    LOG.info('Dumped term in editentity creations')


def project_sitelinks_dump_processor(unpatrolled_changes:pd.DataFrame, \
                                     sitelink_actions:list[str]) -> None:
    existing_dumps = glob(DATAPATH + 'page/worklist-*-page-head.tsv')
    filt = (unpatrolled_changes['rc_patrolled']==0) \
        & (unpatrolled_changes['editsummary-magic-action'].isin(sitelink_actions))
    projects = unpatrolled_changes.loc[filt, 'editsummary-magic-param1'].drop_duplicates().tolist()

    for existing_dump in existing_dumps:
        project_code = existing_dump[38:-14]
        if project_code not in projects: # hacky
            delete_file(existing_dump)
            delete_file(existing_dump.replace('head.tsv', 'full.tsv'))

    for project in projects:
        dump_project_sitelink_changes(unpatrolled_changes, sitelink_actions, project)

    LOG.info('Dumped sitelink edits')


def project_pagemoves_dump_processor(unpatrolled_changes:pd.DataFrame, \
                                     sitelink_move_actions:list[str]) -> None:
    existing_dumps = glob(DATAPATH + 'pagemove/worklist-*-pagemove-head.tsv')
    filt = (unpatrolled_changes['rc_patrolled']==0) \
        & (unpatrolled_changes['editsummary-magic-action'].isin(sitelink_move_actions)) \
        & (unpatrolled_changes['editsummary-magic-param1'].notna())
    projects = unpatrolled_changes.loc[filt, 'editsummary-magic-param1'].drop_duplicates().tolist()

    for existing_dump in existing_dumps:
        project_code = existing_dump[42:-18]
        if project_code not in projects: # hacky
            delete_file(existing_dump)
            delete_file(existing_dump.replace('head.tsv', 'full.tsv'))

    for project in projects:
        dump_project_pagemoves(unpatrolled_changes, sitelink_move_actions, project)

    LOG.info('Dumped pagemove edits')


def project_pageremovals_dump_processor(unpatrolled_changes:pd.DataFrame, \
                                        sitelink_move_actions:list[str]) -> None:
    existing_dumps = glob(DATAPATH + 'pageremoval/worklist-*-pageremoval-head.tsv')
    filt = (unpatrolled_changes['rc_patrolled']==0) \
        & (unpatrolled_changes['editsummary-magic-action'].isin(sitelink_move_actions)) \
        & (unpatrolled_changes['editsummary-magic-param1'].isna())
    projects = unpatrolled_changes.loc[filt, 'editsummary-magic-param2'].drop_duplicates().tolist()

    for existing_dump in existing_dumps:
        project_code = existing_dump[45:-21]
        if project_code not in projects: # hacky
            delete_file(existing_dump)
            delete_file(existing_dump.replace('head.tsv', 'full.tsv'))

    for project in projects:
        dump_project_pageremovals(unpatrolled_changes, sitelink_move_actions, project)

    LOG.info('Dumped page removal edits')


def editentity_dump_processor(unpatrolled_changes:pd.DataFrame, editentity_actions:list[str]) -> None:
    existing_dumps = glob(DATAPATH + 'editentity/worklist-*-head.tsv')
    filt = (unpatrolled_changes['rc_patrolled']==0) \
        & (unpatrolled_changes['editsummary-magic-action'].isin(editentity_actions))
    actions = unpatrolled_changes.loc[filt, 'editsummary-magic-action'].drop_duplicates().tolist()

    for existing_dump in existing_dumps:
        magic_action = existing_dump[44:-9]
        if magic_action not in actions: # hacky
            delete_file(existing_dump)
            delete_file(existing_dump.replace('head.tsv', 'full.tsv'))

    for action in actions:
        dump_editentity_changes(unpatrolled_changes, action)

    LOG.info('Dumped editentity edits')


def property_dump_processor(unpatrolled_changes:pd.DataFrame, claim_actions:list[str]) -> None:
    existing_dumps = glob(DATAPATH + 'property/worklist-*-head.tsv')
    filt = (unpatrolled_changes['rc_patrolled']==0) \
        & (unpatrolled_changes['editsummary-magic-action'].isin(claim_actions))
    properties = unpatrolled_changes.loc[filt, 'editsummary-free-property'].drop_duplicates().tolist()

    for existing_dump in existing_dumps:
        prop = existing_dump[44:-9]
        if prop not in properties: # hacky
            delete_file(existing_dump)
            delete_file(existing_dump.replace('head.tsv', 'full.tsv'))

    for prop in properties:
        dump_property_changes(unpatrolled_changes, claim_actions, prop)

    LOG.info('Dumped property edits')


def print_patrol_progress_patrollers(patrol_progress:pd.DataFrame) -> None:
    languages = list(patrol_progress['editsummary-magic-param1'].unique())

    existing_dumps = glob(DATAPATH + 'progress_patrollers_by_lang/patrollers-*-head.tsv')
    for existing_dump in existing_dumps:
        lang = existing_dump[65:-9]
        if lang not in languages: # hacky
            delete_file(existing_dump)
            delete_file(existing_dump.replace('head.tsv', 'full.tsv'))

    for language in languages:
        filt = (patrol_progress['editsummary-magic-param1']==language)
        filename = f'progress_patrollers_by_lang/patrollers-{language}-{{mode}}.tsv'
        dump_dataframe(patrol_progress.loc[filt].value_counts(subset='actor_name_y'), filename)

    LOG.info('Dumped patrol progress patrollers')


def print_patrol_progress_unpatrolled(patrol_progress:pd.DataFrame) -> None:
    languages = list(patrol_progress['editsummary-magic-param1'].unique())

    existing_dumps = glob(DATAPATH + 'progress_by_lang/unpatrolled-*.tsv')
    for existing_dump in existing_dumps:
        lang = existing_dump[55:-4]
        if lang not in languages: # hacky
            delete_file(existing_dump)

    for language in languages:
        filt = (patrol_progress['editsummary-magic-param1']==language)
        filename = f'progress_by_lang/unpatrolled-{language}.tsv'
        with open(DATAPATH + filename, mode='w', encoding='utf8') as file_handle:
            file_handle.write(str(patrol_progress.loc[filt & (patrol_progress['patrol_delay'].isna())].shape[0]))

    LOG.info('Dumped patrol progress unpatrolled edits')


def print_patrol_progress_describe(patrol_progress:pd.DataFrame) -> None:
    languages = list(patrol_progress['editsummary-magic-param1'].unique())

    existing_dumps = glob(DATAPATH + 'progress_by_lang/describe-*.tsv')
    for existing_dump in existing_dumps:
        lang = existing_dump[52:-4]
        if lang not in languages: # hacky
            delete_file(existing_dump)

    for language in languages:
        filt = (patrol_progress['editsummary-magic-param1']==language)
        filename = f'progress_by_lang/describe-{language}.tsv'
        with open(DATAPATH + filename, mode='w', encoding='utf8') as file_handle:
            file_handle.write(patrol_progress.loc[filt, 'patrol_delay_seconds'].describe().to_string())

    LOG.info('Dumped patrol progress describe')


def make_all_patrol_progress_stats(patrol_progress:pd.DataFrame) -> None:
    print_patrol_progress_patrollers(patrol_progress)
    print_patrol_progress_unpatrolled(patrol_progress)
    print_patrol_progress_describe(patrol_progress)


def make_not_ns0_stats(unpatrolled_changes:pd.DataFrame, translation_pages:list) -> None:
    namespaces = list(unpatrolled_changes.loc[(unpatrolled_changes['rc_patrolled']==0) \
            & (unpatrolled_changes['rc_this_oldid']!=0), 'namespace'].unique())

    existing_dumps = glob(DATAPATH + 'not_ns0/worklist-*-head.tsv')
    for existing_dump in existing_dumps:
        namespace = existing_dump[40:-9].replace('_', ' ')
        if namespace not in namespaces: # hacky
            delete_file(existing_dump)
            delete_file(existing_dump.replace('head.tsv', 'full.tsv'))

    unpatrolled_changes['full_page_title'] = unpatrolled_changes[['namespace', 'rc_title']].apply(
        axis=1,
        func=lambda x : f'{x.namespace}:{x.rc_title}'
    )

    for namespace in namespaces:
        if namespace in [ 'Topic', 'Translations' ]:
            continue

        filt = (unpatrolled_changes['namespace']==namespace) \
            & (unpatrolled_changes['rc_patrolled']==0) \
            & (unpatrolled_changes['rc_this_oldid']!=0) \
            & (~unpatrolled_changes['full_page_title'].isin(translation_pages))

        if unpatrolled_changes.loc[filt].shape[0] == 0:
            continue

        filename = f'not_ns0/worklist-{namespace.replace(" ", "_")}-{{mode}}.tsv'
        fields = ['rc_id', 'rc_timestamp', 'rc_title', 'rc_this_oldid', 'actor_name', 'namespace', 'rc_source']
        dump_dataframe(unpatrolled_changes.loc[filt, fields], filename)

        LOG.info(f'Dumped not-ns0 changes for namespace "{namespace}"')
