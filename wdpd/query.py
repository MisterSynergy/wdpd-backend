from json import JSONDecodeError

from mysql.connector import MySQLConnection
import pandas as pd
import requests

from .config import WIKIDATA_API_ENDPOINT, USER_AGENT, WDCM_TOPLIST_URL, REPLICA_PARAMS


#### internal functions
class Replica:
    def __init__(self):
        self.replica = MySQLConnection(**REPLICA_PARAMS)
        self.cursor = self.replica.cursor()


    def __enter__(self):
        return self.cursor


    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.replica.close()


def query_mediawiki(query:str) -> list[tuple]:
    with Replica() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()

    return result


def edit_summary_broad_category(magic_action:str, actions:dict[str, list[str]]) -> str:
    generic_actions = ['allclaims', 'terms', 'allsitelinks']
    for key in actions:
        if magic_action in actions[key] and magic_action not in generic_actions:
            return key
    return 'NO_CAT'


def query_ores_model_ids(ores_model_name:str) -> list[int]:
    sql = f"""SELECT
  oresm_id
FROM
  ores_model
WHERE
  oresm_name='{ores_model_name}'
  AND oresm_is_current=1"""

    query_result = query_mediawiki(sql)
    ores_model_ids = [ int(row[0]) for row in query_result ]

    return ores_model_ids


#### export functions
def get_unpatrolled_changes(change_tags:pd.DataFrame, ores_scores:pd.DataFrame, \
                                actions:dict[str, list[str]]) -> pd.DataFrame:
    unpatrolled_changes = query_unpatrolled_changes()

    unpatrolled_changes = amend_change_tags(unpatrolled_changes, change_tags)
    unpatrolled_changes = amend_edit_summaries(unpatrolled_changes, actions)
    unpatrolled_changes = amend_ores_scores(unpatrolled_changes, ores_scores)

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
    query_result = query_mediawiki(sql)

    unpatrolled_changes = pd.DataFrame(
        data={
            'rc_id' : [ int(row[0]) for row in query_result ],
            'rc_timestamp' : [ int(row[1]) for row in query_result ],
            'rc_title' : [ row[2].decode('utf8') for row in query_result ],
            'rc_source' : [ row[3].decode('utf8') for row in query_result ],
            'rc_patrolled' : [ int(row[4]) for row in query_result ],
            'rc_new_len' : [ int(row[5]) for row in query_result ],
            'rc_old_len' : [ int(row[6]) for row in query_result ],
            'rc_this_oldid' : [ int(row[7]) for row in query_result ],
            'actor_user' : [ row[8] for row in query_result ],
            'actor_name' : [ row[9].decode('utf8') for row in query_result ],
            'comment_text' : [ row[10].decode('utf8') for row in query_result ],
        },
        columns=[
            'rc_id',
            'rc_timestamp',
            'rc_title',
            'rc_source',
            'rc_patrolled',
            'rc_new_len',
            'rc_old_len',
            'rc_this_oldid',
            'actor_user',
            'actor_name',
            'comment_text'
        ]
    )

    try:
        unpatrolled_changes['time'] = pd.to_datetime(
            arg=unpatrolled_changes['rc_timestamp'],
            format='%Y%m%d%H%M%S'
        )
    except ValueError as exception:
        print('ValueError', exception)

    try:
        unpatrolled_changes['len_diff'] = unpatrolled_changes['rc_new_len'] \
            - unpatrolled_changes['rc_old_len']
    except ValueError as exception:
        print('ValueError', exception)

    try:
        unpatrolled_changes['num_title'] = pd.to_numeric(
            unpatrolled_changes['rc_title'].str.slice(1)
        )
    except ValueError as exception:
        print('ValueError', exception)

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
    query_result = query_mediawiki(sql)

    change_tags = pd.DataFrame(
        data={
            'rc_id' : [ int(row[0]) for row in query_result ],
            'ct_id' : [ int(row[1]) for row in query_result ],
            'ctd_name' : [ row[2].decode('utf8') for row in query_result ]
        },
        columns=[
            'rc_id',
            'ct_id',
            'ctd_name'
        ]
    )

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
      AND log_timestamp>={min_timestamp}
    ORDER BY
      log_timestamp ASC"""
    query_result = query_mediawiki(sql)

    top_patrollers = pd.DataFrame(
        data={
            'log_id' : [ int(row[0]) for row in query_result ],
            'log_timestamp' : [ int(row[1]) for row in query_result ],
            'log_params' : [ row[2].decode('utf8') for row in query_result ],
            'actor_name' : [ row[3].decode('utf8') for row in query_result ]
        },
        columns=[
            'log_id',
            'log_timestamp',
            'log_params',
            'actor_name'
        ]
    )

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
    query_result = query_mediawiki(sql)

    ores_scores = pd.DataFrame(
        data={
            'oresc_rev' : [ int(row[0]) for row in query_result ],
            'oresc_model' : [ int(row[1]) for row in query_result ],
            'oresc_class' : [ int(row[2]) for row in query_result ],
            'oresc_probability' : [ float(row[3]) for row in query_result ],
            'oresc_is_predicted' : [ int(row[4]) for row in query_result ]
        },
        columns=[
            'oresc_rev',
            'oresc_model',
            'oresc_class',
            'oresc_probability',
            'oresc_is_predicted'
        ]
    )

    return ores_scores


def query_unpatrolled_changes_outside_main_namespace() -> pd.DataFrame:
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
      comment_text,
      rc_namespace
    FROM
      recentchanges
        JOIN actor_recentchanges ON rc_actor=actor_id
        JOIN comment_recentchanges ON rc_comment_id=comment_id
    WHERE
      rc_patrolled IN (0, 1)
      AND rc_namespace!=0"""
    query_result = query_mediawiki(sql)

    unpatrolled_changes = pd.DataFrame(
        data={
            'rc_id' : [ int(row[0]) for row in query_result ],
            'rc_timestamp' : [ int(row[1]) for row in query_result ],
            'rc_namespace' : [ int(row[11]) for row in query_result ],
            'rc_title' : [ row[2].decode('utf8') for row in query_result ],
            'rc_source' : [ row[3].decode('utf8') for row in query_result ],
            'rc_patrolled' : [ int(row[4]) for row in query_result ],
            'rc_new_len' : [ int(row[5]) for row in query_result ],
            'rc_old_len' : [ int(row[6]) for row in query_result ],
            'rc_this_oldid' : [ int(row[7]) for row in query_result ],
            'actor_user' : [ row[8] for row in query_result ],
            'actor_name' : [ row[9].decode('utf8') for row in query_result ],
            'comment_text' : [ row[10].decode('utf8') for row in query_result ],
        },
        columns=[
            'rc_id',
            'rc_timestamp',
            'rc_namespace',
            'rc_title',
            'rc_source',
            'rc_patrolled',
            'rc_new_len',
            'rc_old_len',
            'rc_this_oldid',
            'actor_user',
            'actor_name',
            'comment_text'
        ]
    )

    try:
        unpatrolled_changes['time'] = pd.to_datetime(
            arg=unpatrolled_changes['rc_timestamp'],
            format='%Y%m%d%H%M%S'
        )
    except ValueError as exception:
        print('ValueError', exception)
    try:
        unpatrolled_changes['len_diff'] = unpatrolled_changes['rc_new_len'] \
            - unpatrolled_changes['rc_old_len']
    except ValueError as exception:
        print('ValueError', exception)

    namespaces = retrieve_namespace_resolver()
    unpatrolled_changes['namespace'] = unpatrolled_changes['rc_namespace'].apply(
        func=lambda x : namespaces.get(x)  # pylint: disable=unnecessary-lambda
    )

    return unpatrolled_changes


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

    return linked_items


def amend_change_tags(unpatrolled_changes:pd.DataFrame, change_tags:pd.DataFrame) -> pd.DataFrame:
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

    return unpatrolled_changes


def amend_edit_summaries(unpatrolled_changes:pd.DataFrame, actions:dict[str, list[str]]) -> pd.DataFrame:
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
            func=edit_summary_broad_category,
            args=(actions,)
        )

    return unpatrolled_changes


def amend_ores_scores(unpatrolled_changes:pd.DataFrame, ores_scores:pd.DataFrame) -> pd.DataFrame:
    ores_model_names = [ 'damaging', 'goodfaith' ]

    for ores_model_name in ores_model_names:
        ores_model_ids = query_ores_model_ids(ores_model_name)
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
        print('ValueError', exception)
        return None

    patrol_progress['patrol_delay'] = patrol_progress['log_time'] \
        - patrol_progress['time']
    patrol_progress['patrol_delay_seconds'] = patrol_progress['patrol_delay'].astype(
        'timedelta64[s]'
    )
    patrol_progress['patrol_delay_hours'] = patrol_progress['patrol_delay'].astype(
        'timedelta64[s]'
    ) / 3600

    return patrol_progress


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
    query_result = query_mediawiki(sql)

    translation_pages = pd.DataFrame(
        data={
            'page_title' : [ row[0].decode('utf8') for row in query_result ]
        },
        columns=[
            'page_title'
        ]
    )

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

    return translation_pages['translation_page'].unique().tolist()
