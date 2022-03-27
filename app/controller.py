import requests, json, logging, re, random, string
from requests.exceptions import Timeout

from app.config_reader import load_config
config = load_config("config/bot.ini")
logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

def processOp(bps, request_id):
    logger.info(f"to processing operation by: {request_id}")
    string = bps + 'bpsApi/do.PROCESS_REQUEST/param={ "REQUEST_ID" : "' + request_id + '" }'
    logger.info(f"{string}")
    response = requests.post(string)
    logger.info(f"{response.content}, {response.status_code}")
    
    return response.content


def get_credentials():
    bad_prod = ('test122', 'dev122','vashbonus', 'dev248', 'ppk', 'newtest', 'lyvkitchen', 'st-web', 'st-mob')
    try:
        tmp_credles = requests.get(f"{config.tg_bot.conf_url}/api/configs/database", timeout=5)
    except Timeout:
        logger.error(f"{config.tg_bot.conf_url}/api/configs/database is tooo long waiting for")
    else:
        dirty_credles = json.loads(tmp_credles.content)
    LIST_PROJECT = {}
    for item in (x for x in dirty_credles if x.get('name') not in bad_prod):
        LIST_PROJECT[item.get('name')] = item
        
    return LIST_PROJECT


def get_bps(name):
    try:
        tmp_credles = requests.get(f"{config.tg_bot.conf_url}/liquiprocessing", timeout=5)
    except Timeout:
        logger.error(f"{config.tg_bot.conf_url}/liquiprocessing is tooo long waiting for")
    else:
        dirty_credles = json.loads(tmp_credles.content)
    for item in (x for x in dirty_credles if x.get('dataBase') == name):
        return item['processingInt']


def get_nt(items):
    """Get hd."""
    netw={}
    for val in items:
        netw[val[1]]=val[0]

    return netw


def check_phone(phone):
    new = re.sub(r'^8','7', re.sub(r'\D','', phone))
    return re.findall('^7..........$', new)

def check_summ(bill_summ):
    if len(re.findall(r'[^0-9/^.]', re.sub(r',','.', bill_summ))) > 0:
        return

    new = re.sub(r',','.', bill_summ)

    return float(new)

def check_datetime(string):
    result = re.findall(r'\d{2}.\d{2}.\d{4} \d{2}:\d{2}', string)
    return result

def check_process(string):
    result = json.loads(string)
    if result['BpsResponse']['state'] == 'ERROR':
        return str(result['BpsResponse'])


def get_employee_code():
    LOWERCASE_CHARACTERS = string.ascii_lowercase + string.ascii_uppercase
    temp_pwd = random.sample(LOWERCASE_CHARACTERS, 6)
    random.shuffle(temp_pwd)
    str = "".join(temp_pwd)
    
    return 'EQUALIZING_' + str
