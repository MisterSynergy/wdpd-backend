import logging
from json import JSONDecodeError
from typing import Any, Optional

import mariadb
import pandas as pd
import requests

from .config import WIKIDATA_API_ENDPOINT, USER_AGENT, WDCM_TOPLIST_URL, REPLICA_PARAMS


LOG = logging.getLogger(__name__)


#### internal functions
class Replica:
    def __init__(self) -> None:
        self.replica = mariadb.connect(**REPLICA_PARAMS)
        self.cursor = self.replica.cursor(dictionary=True)


    def __enter__(self) -> mariadb.connection.cursor:
        return self.cursor


    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.replica.close()


def _query_mediawiki(query:str, params:Optional[tuple[Any]]=None) -> list[dict[str, Any]]:
    with Replica() as cursor:
        if params is None:
            cursor.execute(query)
        else:
            cursor.execute(query, params)
        result = cursor.fetchall()

    return result


def _query_mediawiki_to_dataframe(query:str, params:Optional[tuple[Any]]=None) -> pd.DataFrame:
    df = pd.DataFrame(
        data=_query_mediawiki(query, params),
    )

    for column in df.columns:
        if not pd.api.types.is_string_dtype(df[column]):
            continue
        try:
            df[column] = df[column].str.decode('utf8')
        except AttributeError as exception:
            LOG.warning(f'Cannot convert column {column} to string due to exception: {exception}')

    return df


def _edit_summary_broad_category(magic_action:str, actions:dict[str, list[str]]) -> str:
    generic_actions = ['allclaims', 'terms', 'allsitelinks']
    for key in actions:
        if magic_action in actions[key] and magic_action not in generic_actions:
            return key
    return 'NO_CAT'


def _query_ores_model_ids(ores_model_name:str) -> list[int]:
    sql = f"""SELECT
  oresm_id
FROM
  ores_model
WHERE
  oresm_name=?
  AND oresm_is_current=1"""
    params = ( ores_model_name, )

    query_result = _query_mediawiki(sql, params)
    ores_model_ids = [ int(row['oresm_id']) for row in query_result ]

    return ores_model_ids


#### export functions
def get_unpatrolled_changes(change_tags:pd.DataFrame, ores_scores:pd.DataFrame, \
                                actions:dict[str, list[str]]) -> pd.DataFrame:
    unpatrolled_changes = query_unpatrolled_changes()

    unpatrolled_changes = _amend_change_tags(unpatrolled_changes, change_tags)
    unpatrolled_changes = _amend_edit_summaries(unpatrolled_changes, actions)
    unpatrolled_changes = _amend_ores_scores(unpatrolled_changes, ores_scores)

    return unpatrolled_changes


def query_unpatrolled_changes() -> pd.DataFrame:
    sql = """SELECT
      rc_id,
      rc_timestamp,
      rc_title,
      rc_source,
      rc_patrolled,
      rc_new_len,
      rc_old_len,
      rc_this_oldid,
      actor_user,
      actor_name,
      comment_text
    FROM
      recentchanges
        JOIN actor_recentchanges ON rc_actor=actor_id
        JOIN comment_recentchanges ON rc_comment_id=comment_id
    WHERE
      rc_patrolled IN (0, 1)
      AND rc_namespace=0"""

    unpatrolled_changes = _query_mediawiki_to_dataframe(sql)

    try:
        unpatrolled_changes['time'] = pd.to_datetime(
            arg=unpatrolled_changes['rc_timestamp'],
            format='%Y%m%d%H%M%S'
        )
    except ValueError as exception:
        LOG.warning('ValueError', exception)

    try:
        unpatrolled_changes['len_diff'] = unpatrolled_changes['rc_new_len'] \
            - unpatrolled_changes['rc_old_len']
    except ValueError as exception:
        LOG.warning('ValueError', exception)

    try:
        unpatrolled_changes['num_title'] = pd.to_numeric(
            unpatrolled_changes['rc_title'].str.slice(1)
        )
    except ValueError as exception:
        LOG.warning('ValueError', exception)

    LOG.info('Queried unpatrolled changes')

    return unpatrolled_changes


def query_change_tags() -> pd.DataFrame:
    sql = """SELECT
      rc_id,
      ct_id,
      ctd_name
    FROM
      recentchanges
        LEFT JOIN change_tag ON rc_id=ct_rc_id
        LEFT JOIN change_tag_def ON ct_tag_id=ctd_id
    WHERE
      rc_patrolled IN (0, 1)
      AND rc_namespace=0
      AND ct_id IS NOT NULL"""
    change_tags = _query_mediawiki_to_dataframe(sql)

    LOG.info('Queried change tags')

    return change_tags


def query_top_patrollers(min_timestamp:int) -> pd.DataFrame:
    sql = f"""SELECT
      log_id,
      log_timestamp,
      log_params,
      actor_name
    FROM
      logging
        JOIN actor_logging ON log_actor=actor_id
    WHERE
      log_action='patrol'
      AND log_type='patrol'
      AND log_namespace=0
      AND log_timestamp>=?
    ORDER BY
      log_timestamp ASC"""
    params = ( min_timestamp, )

    top_patrollers = _query_mediawiki_to_dataframe(sql, params)

    top_patrollers = top_patrollers.merge(
        right=top_patrollers['log_params'].str.extract(
            pat=r's:8:"4::curid";s:10:"(\d+)";?',
            expand=True
        ).astype(int),
        how='left',
        left_index=True,
        right_index=True
    )

    top_patrollers.rename(
        columns={
            0 : 'rc_curid'
        },
        inplace=True
    )

    top_patrollers.drop(labels='log_params', axis=1, inplace=True)

    LOG.info('Queried top patrollers')

    return top_patrollers


def query_ores_scores() -> pd.DataFrame:
    sql = """SELECT
      oresc_rev,
      oresc_model,
      oresc_class,
      oresc_probability,
      oresc_is_predicted
    FROM
      ores_classification
        JOIN recentchanges ON oresc_rev=rc_this_oldid
    WHERE
      rc_patrolled IN (0, 1)"""
    ores_scores = _query_mediawiki_to_dataframe(sql)

    ores_scores['oresc_probability'] = ores_scores['oresc_probability'].astype(float)

    LOG.info('Queried ORES scores')

    return ores_scores


def query_unpatrolled_changes_outside_main_namespace() -> pd.DataFrame:
    sql = """SELECT
      rc_id,
      rc_timestamp,
      rc_namespace,
      rc_title,
      rc_source,
      rc_patrolled,
      rc_new_len,
      rc_old_len,
      rc_this_oldid,
      actor_user,
      actor_name,
      comment_text
    FROM
      recentchanges
        JOIN actor_recentchanges ON rc_actor=actor_id
        JOIN comment_recentchanges ON rc_comment_id=comment_id
    WHERE
      rc_patrolled IN (0, 1)
      AND rc_namespace!=0"""
    unpatrolled_changes = _query_mediawiki_to_dataframe(sql)

    try:
        unpatrolled_changes['time'] = pd.to_datetime(
            arg=unpatrolled_changes['rc_timestamp'],
            format='%Y%m%d%H%M%S'
        )
    except ValueError as exception:
        LOG.warning('ValueError', exception)
    try:
        unpatrolled_changes['len_diff'] = unpatrolled_changes['rc_new_len'] \
            - unpatrolled_changes['rc_old_len']
    except ValueError as exception:
        LOG.warning('ValueError', exception)

    namespaces = retrieve_namespace_resolver()
    unpatrolled_changes['namespace'] = unpatrolled_changes['rc_namespace'].apply(
        func=lambda x : namespaces.get(x)  # pylint: disable=unnecessary-lambda
    )

    LOG.info('Queried unpatrolled changes outside main namespace')

    return unpatrolled_changes


def query_translation_pages() -> list[str]:
    sql = """SELECT
      page_title
    FROM
      revtag
        JOIN page ON rt_page=page_id
        JOIN recentchanges ON rc_this_oldid=rt_revision
    WHERE
      rc_patrolled=0
      AND page_namespace=1198"""
    translation_pages = _query_mediawiki_to_dataframe(sql)

    translation_pages['translatable_page'] = translation_pages['page_title'].apply(
        func=lambda x : '/'.join(x.split('/')[:-2])
    )
    translation_pages['section'] = translation_pages['page_title'].apply(
        func=lambda x : x.split('/')[-2]
    )
    translation_pages['lang'] = translation_pages['page_title'].apply(
        func=lambda x : x.split('/')[-1]
    )
    translation_pages['translation_page'] = translation_pages[['translatable_page', 'lang']].apply(
        axis=1,
        func=lambda x : f'{x.translatable_page}/{x.lang}'
    )

    LOG.info('Queried translation pages')

    return translation_pages['translation_page'].unique().tolist()


def retrieve_namespace_resolver() -> dict[int, str]:
    response = requests.get(
        url=WIKIDATA_API_ENDPOINT,
        params={
            'action' : 'query',
            'meta' : 'siteinfo',
            'siprop' : 'namespaces',
            'formatversion' : '2',
            'format' : 'json'
        },
        headers={ 'User-Agent': USER_AGENT }
    )

    if response.status_code not in [ 200 ]:
        raise RuntimeError('Cannot retrieve namespaces from Wikidata API; HTTP status ' \
                           f'code {response.status_code}')

    try:
        payload = response.json()
    except JSONDecodeError as exception:
        raise RuntimeError('Cannot parse JSON response') from exception

    namespaces = {}
    for namespace, data in payload.get('query', {}).get('namespaces', {}).items():
        namespaces[int(namespace)] = data.get('name')

    LOG.info('Retrieved namespace resolver')

    return namespaces


def retrieve_highly_used_item_list() -> pd.DataFrame:
    wdcm_toplist = pd.read_csv(
        WDCM_TOPLIST_URL,
        names=[
            'qid',
            'entity_usage_count'
        ],
        dtype={
            'qid' : 'str',
            'entity_usage_count' : 'float'
        },
        header=0
    )

    LOG.info('Retrieved highly used item list')

    return wdcm_toplist


def retrieve_wdrfd_links() -> list[str]:
    response = requests.post(
        url=WIKIDATA_API_ENDPOINT,
        data={
            'action' : 'query',
            'prop' : 'links',
            'titles' : 'Wikidata:Requests for deletions',
            'plnamespace' : '0',
            'pllimit' : 'max',
            'format' : 'json'
        },
        headers={ 'User-Agent': USER_AGENT }
    )
    payload = response.json()

    linked_items = []
    for page_info_dict in payload.get('query', {}).get('pages', {}).values():
        for elem in page_info_dict.get('links', []):
            linked_items.append(elem.get('title'))

    LOG.info('Retrieved items linked from WD:RfD')

    return linked_items


def _amend_change_tags(unpatrolled_changes:pd.DataFrame, change_tags:pd.DataFrame) -> pd.DataFrame:
    reverted = [ 'mw-reverted' ]
    unpatrolled_changes = unpatrolled_changes.merge(
        right=change_tags.loc[change_tags['ctd_name'].isin(reverted), ['rc_id', 'ctd_name']],
        how='left',
        on='rc_id'
    )
    unpatrolled_changes.rename(columns={'ctd_name' : 'reverted'}, inplace=True)

    suggested_edit = [ 'apps-suggested-edits' ]
    unpatrolled_changes = unpatrolled_changes.merge(
        right=change_tags.loc[change_tags['ctd_name'].isin(suggested_edit), ['rc_id', 'ctd_name']],
        how='left',
        on='rc_id'
    )
    unpatrolled_changes.rename(columns={'ctd_name' : 'suggested_edit'}, inplace=True)

    LOG.info('Amended changed tags to unpatrolled changes')

    return unpatrolled_changes


def _amend_edit_summaries(unpatrolled_changes:pd.DataFrame, actions:dict[str, list[str]]) -> pd.DataFrame:
    unpatrolled_changes = unpatrolled_changes.merge(
        right=unpatrolled_changes['comment_text'].str.extract(
            pat=r'^\/\* ((?<!\*\/).+?) \*\/ ?(.*)?',
            expand=True
        ),
        how='left',
        left_index=True,
        right_index=True
    )
    unpatrolled_changes.rename(
        columns={
            0 : 'editsummary-magic',
            1 : 'editsummary-free'
        },
        inplace=True
    )

    unpatrolled_changes = unpatrolled_changes.merge(
        right=unpatrolled_changes['editsummary-magic'].str.extract(
            pat=r'^([a-z\-]+):(.*)',
            expand=True
        ),
        how='left',
        left_index=True,
        right_index=True
    )
    unpatrolled_changes.rename(
        columns={
            0 : 'editsummary-magic-action',
            1 : 'editsummary-magic-rest'
        },
        inplace=True
    )

    unpatrolled_changes = unpatrolled_changes.merge(
        right=unpatrolled_changes['editsummary-magic-rest'].str.extract(
            pat=r'^([\d]+)\|([^\|]+)?[\|]?([^\|]+)?[\|]?([^\|]+)?',
            expand=True
        ),
        how='left',
        left_index=True,
        right_index=True
    )
    unpatrolled_changes.rename(
        columns={
            0 : 'editsummary-magic-param0',
            1 : 'editsummary-magic-param1',
            2 : 'editsummary-magic-param2',
            3 : 'editsummary-magic-param3'
        },
        inplace=True
    )

    filt_merge = (unpatrolled_changes['editsummary-magic-action'].isin(actions['allclaims']))
    unpatrolled_changes = unpatrolled_changes.merge(
        right=unpatrolled_changes.loc[filt_merge, 'editsummary-free'].str.extract(
            pat=r'^\[\[Property:(P\d+)]]: (.*)$',
            expand=True
        ),
        how='left',
        left_index=True,
        right_index=True
    )
    unpatrolled_changes.rename(
        columns={
            0 : 'editsummary-free-property',
            1 : 'editsummary-free-value'
        },
        inplace=True
    )

    unpatrolled_changes['editsummary-magic-action-broad'] = \
        unpatrolled_changes['editsummary-magic-action'].apply(
            func=_edit_summary_broad_category,
            args=(actions,)
        )

    LOG.info('Amended edit summary details to unpatrolled changes')

    return unpatrolled_changes


def _amend_ores_scores(unpatrolled_changes:pd.DataFrame, ores_scores:pd.DataFrame) -> pd.DataFrame:
    ores_model_names = [ 'damaging', 'goodfaith' ]

    for ores_model_name in ores_model_names:
        ores_model_ids = _query_ores_model_ids(ores_model_name)
        filt_merge = (ores_scores['oresc_model'].isin(ores_model_ids))
        unpatrolled_changes = unpatrolled_changes.merge(
            right=ores_scores.loc[filt_merge, ['oresc_rev', 'oresc_probability']],
            how='left',
            left_on='rc_this_oldid',
            right_on='oresc_rev'
        )
        unpatrolled_changes.rename(
            columns={
                'oresc_probability' : f'oresc_{ores_model_name}'
            },
            inplace=True
        )
        unpatrolled_changes.drop(labels='oresc_rev', axis=1, inplace=True)

    LOG.info('Amended ORES scores to unpatrolled changes')

    return unpatrolled_changes


def compile_patrol_progress(unpatrolled_changes:pd.DataFrame, \
                            top_patrollers:pd.DataFrame) -> pd.DataFrame:
    action_filter = unpatrolled_changes['editsummary-magic-action-broad'].isin(
        ['label', 'description', 'alias', 'anyterms']
    )

    patrol_progress = unpatrolled_changes.loc[action_filter].merge(
        right=top_patrollers,
        how='left',
        left_on='rc_this_oldid',
        right_on='rc_curid'
    )

    try:
        patrol_progress['log_time'] = pd.to_datetime(
            arg=patrol_progress['log_timestamp'],
            format='%Y%m%d%H%M%S'
        )
    except ValueError as exception:
        LOG.warning('ValueError', exception)
        return None

    patrol_progress['patrol_delay'] = patrol_progress['log_time'] \
        - patrol_progress['time']
    patrol_progress['patrol_delay_seconds'] = patrol_progress['patrol_delay'].astype(
        'timedelta64[s]'
    )
    patrol_progress['patrol_delay_hours'] = patrol_progress['patrol_delay'].astype(
        'timedelta64[s]'
    ) / 3600

    LOG.info('Compiled patrol progress dataframe')

    return patrol_progress
