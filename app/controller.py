import requests, json, logging, re, os
from requests.exceptions import Timeout

from app.config_reader import load_config
config = load_config("config/bot.ini")
logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

def processOp(bps, request_id, operation):
    logger.info(f"to {operation} operation by: {request_id}")
    if bps == 'http://192.168.4.231:50180/mono007/':
        string = 'http://192.168.4.231:50183/mono006/bpsApi/do.' + operation + '/param={ "REQUEST_ID" : "' + request_id + '" }'
    else:
        string = bps + 'bpsApi/do.' + operation + '/param={ "REQUEST_ID" : "' + request_id + '" }'
    logger.info(f"{string}")
    response = requests.post(string)
    logger.info(f"{response.content}, {response.status_code}")
    
    return response.content


def get_credentials():
    bad_prod = ('test122', 'dev122','vashbonus', 'dev248', 'ppk', 'lyvkitchen', 'st-web', 'st-mob')
    try:
        tmp_credles = requests.get(f"{config.tg_bot.conf_url}/api/configs/database", timeout=15)
    except Timeout:
        logger.error(f"{config.tg_bot.conf_url}/api/configs/database is tooo long waiting for")
        return
    else:
        dirty_credles = json.loads(tmp_credles.content)
    LIST_PROJECT = {}
    for item in (x for x in dirty_credles if x.get('name') not in bad_prod):
        LIST_PROJECT[item.get('name')] = item
        
    return LIST_PROJECT


def get_all_bps():
    bad_prod = ('test122', 'dev122', 'vashbonus', 'dev248', 'ppk', 'st-web', 'st-mob')
    try:
        tmp_credles = requests.get(f"{config.tg_bot.conf_url}/liquiprocessing", timeout=15)
    except Timeout:
        logger.error(f"{config.tg_bot.conf_url}/liquiprocessing is tooo long waiting for")
        return
    else:
        dirty_credles = json.loads(tmp_credles.content)
    LIST_PROJECT = {}
    for item in (x for x in dirty_credles if x.get('dataBase') not in bad_prod):
        LIST_PROJECT[item.get('dataBase')] = item
        
    return LIST_PROJECT


def get_bps(name):
    try:
        tmp_credles = requests.get(f"{config.tg_bot.conf_url}/liquiprocessing", timeout=15)
    except Timeout:
        logger.error(f"{config.tg_bot.conf_url}/liquiprocessing is tooo long waiting for")
        return
    else:
        dirty_credles = json.loads(tmp_credles.content)
    for item in (x for x in dirty_credles if x.get('dataBase') == name):
        return item['processingInt']


def get_bps_token(name):
    try:
        configs = requests.get(f"{config.tg_bot.conf_url}/api/configs/bps/{name}", timeout=15)
    except Timeout:
        logger.error(f"{config.tg_bot.conf_url}/configs is tooo long waiting for")
        return
    else:
        dirty_configs = json.loads(configs.content)
    
    return dirty_configs['bpsToken']
    
    
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

def check_bonus(bonus_info, value):
    general_max, premium_max, general_min, premium_min, gold_min, gold_max = bonus_info
    if general_max >= value >= general_min:
        return 'GENERAL'
    elif premium_max >= value >= premium_min:
        return 'GENERAL_PREMIUM'
    elif gold_max >= value >= gold_min:
        return 'GENERAL_GOLD'
    else: 
        return

def check_datetime(string):
    result = re.findall(r'\d{2}.\d{2}.\d{4} \d{2}:\d{2}', string)
    return result

def check_process(string):
    result = json.loads(string)
    if result['BpsResponse']['state'] == 'ERROR':
        return str(result['BpsResponse'])

def get_cli_info(cli):
    for i,v in cli.items():
        cli_name, cli_id = (i,v)
    
    return cli_name, cli_id

def get_refresh(bps_token, processingExt):
    url= f'{processingExt}login'
    url_refresh = f'{processingExt}refreshActions'
    user_agent_val = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'

    session = requests.Session()
    r = session.get(url, headers = {
        'User-Agent': user_agent_val
    })

    session.headers.update({'Referer':url})
    session.headers.update({'User-Agent':user_agent_val})
    
    post_request = session.post(url, {
        'username': 'admin',
        'password': bps_token
    })
    logger.info(f"login_response: {post_request}")
    response = session.get(url_refresh)
    logger.info(f"refresh_response: {response.content}")

    return response.content

def rm_docs():
    files = os.listdir(r'./documents/')
    for file in files:
        os.remove(f'./documents/{file}')