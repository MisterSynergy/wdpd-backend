from os.path import expanduser
from requests.utils import default_headers


USER_AGENT:str = f'{default_headers()["User-Agent"]} (Wikidata bot' \
              ' by User:MisterSynergy; mailto:mister.synergy@yahoo.com)'

WIKIDATA_API_ENDPOINT:str = 'https://www.wikidata.org/w/api.php'
WDQS_ENDPOINT:str = 'https://query.wikidata.org/sparql'
WDCM_TOPLIST_URL:str = 'https://analytics.wikimedia.org/published/datasets/wmde-analytics-' \
    'engineering/wdcm/etl/wdcm_topItems.csv'

DATAPATH:str = '/data/project/wdpd/data/'
REPLICA_PARAMS:dict[str, str] = {
    'host' : 'wikidatawiki.analytics.db.svc.wikimedia.cloud',
    'database' : 'wikidatawiki_p',
    'default_file' : f'{expanduser("~")}/replica.my.cnf'
}
