from io import StringIO
import logging
from os import mkdir, remove
from os.path import isdir
from time import perf_counter

import pandas as pd
import requests

from .config import WDQS_ENDPOINT, USER_AGENT, PLOTPATH, DATAPATH, DUMP_UPDATE_FILE


LOG = logging.getLogger(__name__)


def delete_file(filename:str) -> None:
    try:
        remove(filename)
    except FileNotFoundError:
        pass

    LOG.info(f'Tried to delete file {filename}')


def wdqs_query(query:str) -> pd.DataFrame:
    t_query_start = perf_counter()

    response = requests.post(
        url=WDQS_ENDPOINT,
        data={
            'query' : query
        },
        headers={
            'Accept' : 'text/csv',
            'User-Agent' : USER_AGENT
        }
    )

    t_query_end = perf_counter()

    df = pd.read_csv(
        StringIO(
            response.text
        )
    )

    LOG.info(f'Queried WDQS to dataframe; query time {t_query_end - t_query_start:.1f} sec')

    return df


def dump_update_timestamp(timestmp:float) -> None:
    with open(DUMP_UPDATE_FILE, mode='w', encoding='utf8') as file_handle:
        file_handle.write(f'{timestmp:.0f}')

    LOG.info(f'dumped update timestamp {timestmp:.0f}')


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


def init_directories() -> None:
    required_directories = [
        DATAPATH,
        PLOTPATH
    ]

    for required_directory in required_directories:
        if isdir(required_directory):
            continue
        mkdir(required_directory)

    LOG.info('Initialized directories')


def df_info(dataframe:pd.DataFrame) -> None:
    LOG.info(dataframe.shape)

    str_buffer = StringIO()
    dataframe.info(buf=str_buffer)
    LOG.info(str_buffer.getvalue())

    LOG.info(dataframe.head())
