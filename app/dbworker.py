import cx_Oracle, requests, logging
from sqlalchemy import create_engine


logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)


def create_session_oracle(item):
    user=item['user']
    password= item['password']
    dsn = item['connectString']
    engine_oracle = create_engine(f'oracle+cx_oracle://{user}:{password}@{dsn}',max_identifier_length=128)
    return engine_oracle.connect()


def get_networks(item):
    cursor_oracle = create_session_oracle(item)
    networks = cursor_oracle.execute("""SELECT RETAIL_NETWORK_ID, title FROM retail_networks where is_delete=0""")
    
    return networks.fetchall()

def get_points(item, network_id):
    cursor_oracle = create_session_oracle(item)
    points = cursor_oracle.execute(f"""SELECT RETAIL_POINT_ID, TITLE FROM RETAIL_POINTS where network_id={network_id} and is_delete=0""")
    
    return points.fetchall()

def get_terminals(item, point_id):
    cursor_oracle = create_session_oracle(item)
    terminals = cursor_oracle.execute(f"""SELECT terminal_id, CODE ||' ( '|| to_char(LAST_PING, 'dd.mm.yyyy') ||' )' code FROM terminals rp where retail_point_id={point_id} and is_delete=0 and is_locked=0""")
    
    return terminals.fetchall()

def get_cli_by_phone(item, phone_mobile):
    cursor_oracle = create_session_oracle(item)
    cli = cursor_oracle.execute(f"""select CLI_ID, FIRST_NAME ||' '|| LAST_NAME from PHYSICAL_CLIENTS pc WHERE  PHONE_MOBILE = '{phone_mobile}' and is_delete=0""")
    
    return cli.fetchall()


def get_card(item, cli_id):
    cursor_oracle = create_session_oracle(item)
    card = cursor_oracle.execute(f"""SELECT CARD_ID, card_num FROM cards c WHERE c.CLI_ID = {cli_id} AND c.IS_LOCKED = 0 AND c.IS_DELETE = 0""")
    
    return card.fetchone()    


def get_persent(item, card_id, point_id, network_id):
    cursor_oracle = create_session_oracle(item)
    persent = cursor_oracle.execute(f"""SELECT TO_NUMBER(nvl(ch.CURRENT_BONUS, bruv.VALUE)) bonus  FROM 
        RETAIL_POINT_RULE_UNITS_RELS rpr 
        JOIN BONUS_RULE_UNITS bru ON rpr.UNIT_ID = bru.unit_id AND rpr.RETAIL_POINT_ID = {point_id}
        JOIN BONUS_RULE_UNIT_VALUES bruv ON bruv.UNIT_ID =bru.UNIT_ID
        JOIN (SELECT DECODE(c.card_type ,'GENERAL', 17,'GENERAL_PREMIUM', 18, 'GENERAL_GOLD', 19) PARAMETER_ID 
                from cards c WHERE card_id = {card_id}) c ON c.PARAMETER_ID=bruv.PARAMETER_ID 
        LEFT JOIN CARD_HISTORIES ch ON ch.CARD_ID = {card_id} AND ch.NETWORK_ID = {network_id}
        WHERE bru.APP_CODES_REGEX != 'lyvkit'""")
    
    return persent.fetchone()

def get_org_fee(item, card_id, point_id):
    cursor_oracle = create_session_oracle(item)

    fee = cursor_oracle.execute(f"""SELECT TO_NUMBER(br.AMOUNT) FROM BONUS_RULE_UNITS bru
        JOIN RETAIL_POINT_RULE_UNITS_RELS rp ON rp.UNIT_ID = bru.UNIT_ID 
        JOIN BONUS_RULE_UNIT_REWARDS br ON br.UNIT_ID  = bru.UNIT_ID 
        WHERE bru.TEMPLATE_ID = 22 AND rp.RETAIL_POINT_ID = {point_id} AND APP_CODES_REGEX !='lyvkit'
        AND br.CARD_TYPE = (SELECT card_type FROM cards WHERE card_id = {card_id})""")
    
    return fee.fetchone()


def get_balance(item, cli_id):
    cursor_oracle = create_session_oracle(item)
    balance = cursor_oracle.execute(f"""SELECT TO_NUMBER(balance) FROM accounts WHERE cli_id = {cli_id} and account_type='GLOBAL'""")
    
    return balance.fetchone()


def set_request(cursor_oracle, request_date, card_id, terminal_id, employee_code):
    cursor_oracle.execute(f"""insert into requests (request_id, request_date, request_state, request_type, card_id, terminal_id, request_state_code, employee_code, ins_date, upd_date)
                select LOWER(REGEXP_REPLACE(SYS_GUID(), '(.{8})(.{4})(.{4})(.{4})(.{12})', '\\1-\\2-\\3-\\4-\\5')) MSSQL_GUID, to_date('{request_date}','dd.mm.yyyy hh24:mi'), 'READY', 'PAYMENT_AND_CONFIRM',
                {card_id}, {terminal_id}, 'OK', '{employee_code}', current_date, current_date from dual""")

    return

def get_requst_id(cursor_oracle, employee_code, card_id):
    request = cursor_oracle.execute(f"""SELECT REQUEST_ID FROM REQUESTS r WHERE r.CARD_ID = {card_id} AND employee_code='{employee_code}'""")

    return request.fetchone()

def set_bills(cursor_oracle, employee_code, request_date, bill_sum, request_id):
    logger.info(f"setting bills")
    cursor_oracle.execute(f"""insert into bills(bill_id, bill_code, bill_date, bill_length, bill_sum, is_spend_bonus, request_id, ins_date, upd_date, is_processed, state)
                select bills$seq.nextval, '{employee_code}', to_date('{request_date}', 'dd.mm.yyyy hh24:mi'), 1, {bill_sum},
                (select(case when 0 > 0 then 1 else 0 end) from dual), '{request_id}', current_date,
                current_date, 0, 'PROCESSED' from dual""")

    return

def set_goods(cursor_oracle, request_id, bill_sum, employee_code):
    logger.info(f"setting goods")
    cursor_oracle.execute(f"""insert into goods(good_id, bill_id, good_code, amount, good_title, is_discount_available, order_num, position_price, ins_date, upd_date, good_type, state)
                select goods$seq.nextval, (SELECT BILL_ID FROM BILLS b WHERE b.REQUEST_ID = '{request_id}'), '{employee_code}', 1, '{employee_code}', 0, 1, {bill_sum}, current_date,
                current_date, 'GOOD', 'CONFIRMED' from dual""")

    return

def set_transactions_debit(cursor_oracle, bonus_sum, cli_id, card_id, request_id, retail_point_id):
    logger.info(f"setting transactions_debit with: {request_id}")
    cursor_oracle.execute(f"""insert into transactions(transaction_id, operation_type, transaction_state, amount, account_id, good_id, card_id, bill_id, ins_date, upd_date, transaction_kind, request_id, retail_point_id)
                select transactions$seq.nextval, 'DEBIT', 'READY', {bonus_sum}, (select account_id from accounts where account_type='GLOBAL' and cli_id={cli_id}),
                (SELECT GOOD_ID FROM GOODS WHERE BILL_ID=(SELECT BILL_ID FROM BILLS WHERE REQUEST_ID = '{request_id}')),
                {card_id}, (SELECT BILL_ID FROM BILLS b WHERE b.REQUEST_ID = '{request_id}'),
                current_date, current_date, 'PAYMENT_DEBIT', '{request_id}',
                {retail_point_id} from dual where {bonus_sum} > 0""")

    return

def set_transactions_fee(cursor_oracle, org_fee, card_id, cli_id, request_id, retail_point_id):
    cursor_oracle.execute(f"""insert into transactions(transaction_id, operation_type, transaction_state, amount, account_id, good_id, card_id, bill_id, ins_date, upd_date, transaction_kind, request_id, retail_point_id)
                select transactions$seq.nextval, 'CREDIT', 'READY', {org_fee}, (select account_id from accounts where account_type='GLOBAL' and cli_id={cli_id}),
                (SELECT GOOD_ID FROM GOODS WHERE BILL_ID=(SELECT BILL_ID FROM BILLS WHERE REQUEST_ID = '{request_id}')),
                {card_id}, (SELECT BILL_ID FROM BILLS b WHERE b.REQUEST_ID = '{request_id}'),
                current_date, current_date, 'ORGANIZER_FEE', '{request_id}',
                {retail_point_id} from dual where {org_fee} > 0""")

    return

def set_transactions_credit(cursor_oracle, card_id, cli_id, request_id, retail_point_id, credit):
    cursor_oracle.execute(f"""insert into transactions(transaction_id, operation_type, transaction_state, amount, account_id, good_id, card_id, bill_id, ins_date, upd_date, transaction_kind, request_id, retail_point_id)
                select transactions$seq.nextval, 'CREDIT', 'READY', {credit}, (select account_id from accounts where account_type='GLOBAL' and cli_id={cli_id}),
                (SELECT GOOD_ID FROM GOODS WHERE BILL_ID=(SELECT BILL_ID FROM BILLS WHERE REQUEST_ID = '{request_id}')),
                {card_id}, (SELECT BILL_ID FROM BILLS b WHERE b.REQUEST_ID = '{request_id}'),
                current_date, current_date, 'PAYMENT_CREDIT', '{request_id}',
                {retail_point_id} from dual where {credit} > 0""")

    return


def set_upd_requests(cursor_oracle, request_id):
    logger.info(f"updating request: {request_id}")
    cursor_oracle.execute(f"update REQUESTS set EXT_REQUEST_ID = '{request_id}' where REQUEST_ID = '{request_id}'")

    return

def update_account(cursor_oracle, bonus_sum, org_fee, credit, cli_id):
    cursor_oracle.execute(f"""update accounts set amount=amount + {bonus_sum} - {org_fee}, balance=balance - {credit},
                locked_amount = locked_amount + {bonus_sum} - {org_fee} + {credit}, upd_date=current_date where account_type = 'GLOBAL'
                and cli_id = {cli_id}""")

    return


def get_operation(item, request_id):
    cursor_oracle = create_session_oracle(item)
    result = cursor_oracle.execute(f"""select 'Гостю ' ||fio|| ' с номером телефона: '|| PHONE_MOBILE|| ' было начислено: '|| bonus_sum ||', за чек на сумму: '|| bill_sum|| ' от '|| TO_CHAR(request_date, 'dd.mm.yyyy') ||',' || RETAIL_POINT_TITLE ||'/rn Статус операции:' || REQUEST_STATE
            from PAYMENT_OPERATIONS po WHERE REQUEST_ID ='{request_id}'""")

    return result.fetchone()

def main_aqualizing(user_data):
    conn = user_data['project']
    with cx_Oracle.connect(conn['user'], conn['password'], conn['connectString']) as connection:
        try:
            credit_sum = user_data['credit_sum']
            point_id = user_data['point_id']
            cli_id = user_data['cli_id']
            terminal_id = user_data['terminal_id']
            bill_sum = user_data['bill_summ']
            card_id = user_data['card_id']
            org_fee = user_data['org_fee']
            bonus_sum = user_data['bonus_sum']
            request_date = user_data['request_date']
            employee_code = user_data['employee_code']
            cursor = connection.cursor()
            
            #!!!!!!!!!!!

            # --Записываем запрос
            logger.info(f"Starting request: {cursor}, {request_date}, {card_id}, {terminal_id}, {employee_code}")
            set_request(cursor, request_date, card_id, terminal_id, employee_code)
            request_id, = get_requst_id(cursor, employee_code, card_id)
            set_upd_requests(cursor, request_id)
            set_bills(cursor, employee_code, request_date, bill_sum, request_id)
            set_goods(cursor, request_id, bill_sum, employee_code)
            set_transactions_debit(cursor, bonus_sum, cli_id, card_id, request_id, point_id)
            set_transactions_fee(cursor, org_fee, card_id, cli_id, request_id, point_id)
            set_transactions_credit(cursor, card_id, cli_id, request_id, point_id, credit_sum)
            update_account(cursor, bonus_sum, org_fee, credit_sum, cli_id)
            logger.info("commiting")
            connection.commit()

            return request_id
        except Exception as e:
            print(e)
            logger.error(e)
            connection.rollback()
            exit(1)
    