from os.path import expanduser
from requests.utils import default_user_agent


USER_AGENT:str = f'{default_user_agent()} (Wikidata bot' \
              ' by User:MisterSynergy; mailto:mister.synergy@yahoo.com)'

WIKIDATA_API_ENDPOINT:str = 'https://www.wikidata.org/w/api.php'
WDQS_ENDPOINT:str = 'https://query.wikidata.org/sparql'
HIGHLY_USED_ITEMS_URL:str = 'https://tools-static.wmflabs.org/kvasir1/entityusage_topitems.tsv.gz'

DATAPATH:str = f'{expanduser("~")}/data/'
PLOTPATH:str = f'{expanduser("~")}/plots/'
DUMP_UPDATE_FILE:str = f'{expanduser("~")}/data/update.txt'

REPLICA_PARAMS:dict[str, str] = {
    'host' : 'wikidatawiki.analytics.db.svc.wikimedia.cloud',
    'database' : 'wikidatawiki_p',
    'default_file' : f'{expanduser("~")}/replica.my.cnf'
}

DEBUG:bool = False  # True: adds some dataframe information to logfile

PLOT_WINDOW_DAYS:int = 28
FIGSIZE_STANDARD = (6, 4)
FIGSIZE_TALL = (6, 8)
FIGSIZE_WIDE = (9, 4)
FIGSIZE_HEATMAP = (6, 4.4)
QID_BIN_SIZE = 1_000_000
QID_BIN_MAX = 150_000_000

ORES_MODELS = [
    'oresc_damaging',
    'oresc_goodfaith'
]
ORES_TRIGGER_UNREGISTERED_SCORE = 0.9
ORES_TRIGGER_UNREGISTERED_EDITS = 10
ORES_TRIGGER_REGISTERED_SCORE = 0.7
ORES_TRIGGER_REGISTERED_EDITS = 10

MAX_QID_NUM = 150_000_000
MIN_ENTITY_USAGE = 500