import cx_Oracle, logging

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
    terminals = cursor_oracle.execute(f"""SELECT TERMINAL_ID, CODE ||' ( '|| to_char(LAST_PING, 'dd.mm.yyyy') ||' )' AS code FROM ( 
        SELECT TERMINAL_ID, code, LAST_PING
        ,ROW_NUMBER() OVER (PARTITION BY TYPE ORDER BY LAST_PING DESC) rowes
        FROM terminals rp where retail_point_id={point_id} and is_delete=0 and is_locked=0
        ) WHERE rowes<3""")
    
    return terminals.fetchall()

def get_cli_by_phone(item, phone_mobile):
    cursor_oracle = create_session_oracle(item)
    cli = cursor_oracle.execute(f"""select CLI_ID, FIRST_NAME ||' '|| LAST_NAME from PHYSICAL_CLIENTS pc WHERE  PHONE_MOBILE = '{phone_mobile}' and is_delete=0""")
    
    return cli.fetchall()

def get_point_by_term(item, code):
    cursor_oracle = create_session_oracle(item)
    point = cursor_oracle.execute(f"""SELECT retail_point_id FROM terminals  WHERE code = '{code}' AND IS_LOCKED = 0 AND IS_DELETE = 0""")
    
    return point.fetchone() 

def get_network_by_point(item, point_id):
    cursor_oracle = create_session_oracle(item)
    network = cursor_oracle.execute(f"""SELECT NETWORK_ID FROM RETAIL_POINTS rp WHERE RETAIL_POINT_ID  = {point_id} AND IS_DELETE = 0""")
 
    return network.fetchone() 

def get_card(item, cli_id):
    cursor_oracle = create_session_oracle(item)
    card = cursor_oracle.execute(f"""SELECT CARD_ID, card_num FROM cards c WHERE c.CLI_ID = {cli_id} AND c.IS_LOCKED = 0 AND c.IS_DELETE = 0""")
    
    return card.fetchone()    

def get_card_info(item, cli_id):
    cursor_oracle = create_session_oracle(item)
    card = cursor_oracle.execute(f"""SELECT CARD_ID, card_num, card_type FROM cards c WHERE c.CLI_ID = {cli_id} AND c.IS_LOCKED = 0 AND c.IS_DELETE = 0""")
    
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
        WHERE bru.APP_CODES_REGEX IS null OR bru.APP_CODES_REGEX != 'lyvkit'"""  )
    
    return persent.fetchone()


def get_percent_network(item, card_id, network_id):
    cursor_oracle = create_session_oracle(item)
    persent = cursor_oracle.execute(f"""SELECT DISTINCT nvl(ch.CURRENT_BONUS, bruv.VALUE) FROM 
		RETAIL_POINTS rp  
        JOIN RETAIL_POINT_RULE_UNITS_RELS rpr ON rp.RETAIL_POINT_ID =rpr.RETAIL_POINT_ID
        JOIN BONUS_RULE_UNITS bru ON rpr.UNIT_ID = bru.unit_id
        JOIN BONUS_RULE_UNIT_VALUES bruv ON bruv.UNIT_ID =bru.UNIT_ID
        JOIN (SELECT DECODE(c.card_type ,'GENERAL', 17,'GENERAL_PREMIUM', 18, 'GENERAL_GOLD', 19) PARAMETER_ID 
                from CARDS c WHERE CARD_ID = {card_id}) c ON c.PARAMETER_ID=bruv.PARAMETER_ID 
        LEFT JOIN CARD_HISTORIES ch ON ch.CARD_ID = {card_id} AND (ch.app_code IS NULL or ch.app_code != 'lyvkit') AND ch.NETWORK_ID = {network_id}
        WHERE (bru.APP_CODES_REGEX IS null OR bru.APP_CODES_REGEX != 'lyvkit')
        AND rpr.IS_DELETE = 0 AND bru.IS_DELETE = 0 AND rp.IS_DELETE = 0 AND bru.END_DATE > CURRENT_DATE 
        AND rp.NETWORK_ID = {network_id}"""  )
    
    return persent.fetchone()


def get_bonus_info(item, network_id):
    cursor_oracle = create_session_oracle(item)
    bonus = cursor_oracle.execute(f"""SELECT * FROM
        (
        SELECT DISTINCT parameter_id, TO_NUMBER(value) value FROM 
                RETAIL_POINTS rp  
                JOIN RETAIL_POINT_RULE_UNITS_RELS rpr ON rp.RETAIL_POINT_ID =rpr.RETAIL_POINT_ID 
                JOIN BONUS_RULE_UNITS bru ON rpr.UNIT_ID = bru.unit_id 
                JOIN BONUS_RULE_UNIT_VALUES bruv ON bruv.UNIT_ID =bru.UNIT_ID
                WHERE bruv.PARAMETER_ID IN (17, 19, 18, 1, 20, 4)
                AND rpr.IS_DELETE = 0 AND bru.IS_DELETE = 0 AND rp.IS_DELETE = 0 AND bru.END_DATE > CURRENT_DATE 
                AND  bru.TEMPLATE_ID = 1 AND rp.NETWORK_ID = {network_id}
                AND (bru.APP_CODES_REGEX IS NULL OR bru.APP_CODES_REGEX != 'lyvkit')
                ORDER BY 1 asc 
        )
        PIVOT
        (
        max(value)
        FOR parameter_id IN (1,4,17,18,19,20)
        )
        ORDER BY 1"""  )
    
    return bonus.fetchone()



def get_org_fee(item, card_id, point_id):
    cursor_oracle = create_session_oracle(item)

    fee = cursor_oracle.execute(f"""SELECT TO_NUMBER(br.AMOUNT) FROM BONUS_RULE_UNITS bru
        JOIN RETAIL_POINT_RULE_UNITS_RELS rp ON rp.UNIT_ID = bru.UNIT_ID 
        JOIN BONUS_RULE_UNIT_REWARDS br ON br.UNIT_ID  = bru.UNIT_ID 
        WHERE bru.TEMPLATE_ID = 22 AND rp.RETAIL_POINT_ID = {point_id} AND ( APP_CODES_REGEX is null or APP_CODES_REGEX !='lyvkit')
        AND br.CARD_TYPE = (SELECT card_type FROM cards WHERE card_id = {card_id})""")
    
    org_fee = fee.fetchone()
    
    if not org_fee:
        return 0,
    
    return org_fee


def get_balance(item, cli_id):
    cursor_oracle = create_session_oracle(item)
    balance = cursor_oracle.execute(f"""SELECT TO_NUMBER(balance) FROM accounts WHERE cli_id = {cli_id} and is_delete = 0 and account_type = 'GLOBAL'""")
    
    return balance.fetchone()


def get_card_by_rrn(item, rrn):
    cursor_oracle = create_session_oracle(item)
    card_id = cursor_oracle.execute(f"""select card_num from cards where card_id = (SELECT card_id FROM requests WHERE ext_request_id = '{rrn}')""")
    
    return card_id.fetchone()

def get_card_by_card(item, card_num):
    cursor_oracle = create_session_oracle(item)
    card = cursor_oracle.execute(f"""SELECT card_id, cli_id FROM cards WHERE card_num = '{card_num}' and is_delete = 0 and is_locked = 0""")

    return card.fetchone()

def get_card_by_cli(item, card_num):
    cursor_oracle = create_session_oracle(item)
    card = cursor_oracle.execute(f"""SELECT card_id, cli_id FROM cards WHERE card_num = '{card_num}' and is_delete = 0 and is_locked = 0""")
    
    return card.fetchone()


def check_terminal(item, code):
    cursor_oracle = create_session_oracle(item)
    terminal = cursor_oracle.execute(f"""select is_locked, is_delete, terminal_id from terminals where code = '{code}'""")
    
    return terminal.fetchone()



def set_request(cursor_oracle, request_id, request_date, card_id, terminal_id, type, status):
    logger.info(f"setting request")
    cursor_oracle.execute(f"""insert into requests(request_id, request_date, request_state, request_type, card_id, terminal_id, request_state_code, employee_code, ins_date, upd_date, ext_request_id)
                select '{request_id}', to_date('{request_date}','dd.mm.yyyy hh24:mi'), '{status}', '{type}',
                {card_id}, {terminal_id}, 'OK', 'EQUALIZING', systimestamp, systimestamp, '{request_id}' from dual""")

    return

def set_bills(cursor_oracle, request_date, bill_sum, request_id):
    logger.info(f"setting bills")
    cursor_oracle.execute(f"""insert into bills(bill_id, bill_code, bill_date, bill_length, bill_sum, is_spend_bonus, request_id, ins_date, upd_date, is_processed, state)
                select bills$seq.nextval, 'EQUALIZING', to_date('{request_date}', 'dd.mm.yyyy hh24:mi'), 1, {bill_sum},
                (select(case when 0 > 0 then 1 else 0 end) from dual), '{request_id}', current_date,
                current_date, 0, 'PROCESSED' from dual""")

    return

def set_goods(cursor_oracle, request_id, bill_sum):
    logger.info(f"setting goods")
    cursor_oracle.execute(f"""insert into goods(good_id, bill_id, good_code, amount, good_title, is_discount_available, order_num, position_price, ins_date, upd_date, good_type, state)
                select goods$seq.nextval, (SELECT BILL_ID FROM BILLS b WHERE b.REQUEST_ID = '{request_id}'), 'EQUALIZING', 1, 'EQUALIZING', 0, 1, {bill_sum}, current_date,
                current_date, 'GOOD', 'CONFIRMED' from dual""")

    return

def set_transactions_debit(cursor_oracle, bonus_sum, cli_id, card_id, request_id, retail_point_id, status):
    logger.info(f"setting transactions_debit with: {request_id}")
    cursor_oracle.execute(f"""insert into transactions(transaction_id, operation_type, transaction_state, amount, account_id, good_id, card_id, bill_id, ins_date, upd_date, transaction_kind, request_id, retail_point_id)
                select transactions$seq.nextval, 'DEBIT', '{status}', {bonus_sum}, (select account_id from accounts where account_type='GLOBAL' and cli_id={cli_id}),
                (SELECT GOOD_ID FROM GOODS WHERE BILL_ID=(SELECT BILL_ID FROM BILLS WHERE REQUEST_ID = '{request_id}')),
                {card_id}, (SELECT BILL_ID FROM BILLS b WHERE b.REQUEST_ID = '{request_id}'),
                current_date, current_date, 'PAYMENT_DEBIT', '{request_id}',
                {retail_point_id} from dual where {bonus_sum} > 0""")

    return

def set_transactions_fee(cursor_oracle, org_fee, card_id, cli_id, request_id, retail_point_id, status):
    cursor_oracle.execute(f"""insert into transactions(transaction_id, operation_type, transaction_state, amount, account_id, good_id, card_id, bill_id, ins_date, upd_date, transaction_kind, request_id, retail_point_id)
                select transactions$seq.nextval, 'CREDIT', '{status}', {org_fee}, (select account_id from accounts where account_type='GLOBAL' and cli_id={cli_id}),
                (SELECT GOOD_ID FROM GOODS WHERE BILL_ID=(SELECT BILL_ID FROM BILLS WHERE REQUEST_ID = '{request_id}')),
                {card_id}, (SELECT BILL_ID FROM BILLS b WHERE b.REQUEST_ID = '{request_id}'),
                current_date, current_date, 'ORGANIZER_FEE', '{request_id}',
                {retail_point_id} from dual where {org_fee} > 0""")

    return

def set_transactions_credit(cursor_oracle, card_id, cli_id, request_id, retail_point_id, credit, status):
    cursor_oracle.execute(f"""insert into transactions(transaction_id, operation_type, transaction_state, amount, account_id, good_id, card_id, bill_id, ins_date, upd_date, transaction_kind, request_id, retail_point_id)
                select transactions$seq.nextval, 'CREDIT', '{status}', {credit}, (select account_id from accounts where account_type='GLOBAL' and cli_id={cli_id}),
                (SELECT GOOD_ID FROM GOODS WHERE BILL_ID=(SELECT BILL_ID FROM BILLS WHERE REQUEST_ID = '{request_id}')),
                {card_id}, (SELECT BILL_ID FROM BILLS b WHERE b.REQUEST_ID = '{request_id}'),
                current_date, current_date, 'PAYMENT_CREDIT', '{request_id}',
                {retail_point_id} from dual where {credit} > 0""")

    return


def update_account(cursor_oracle, bonus_sum, org_fee, credit, cli_id):
    logger.info(f"updating account")
    cursor_oracle.execute(f"""update accounts set amount=amount + {bonus_sum} - {org_fee}, balance=balance - {credit},
                locked_amount = locked_amount + {bonus_sum} - {org_fee} + {credit}, upd_date=current_date where account_type = 'GLOBAL'
                and cli_id = {cli_id}""")

    return


def update_account_balance(cursor_oracle, bonus_sum, org_fee, credit, cli_id):
    logger.info(f"updating account")
    cursor_oracle.execute(f"""update accounts set amount=amount + {bonus_sum} - {org_fee}, balance=balance - {credit} + {bonus_sum} - {org_fee},
                upd_date=current_date where account_type = 'GLOBAL'
                and cli_id = {cli_id}""")


def get_operation(item, request_id):
    logger.info(f"getting operation")
    cursor_oracle = create_session_oracle(item)
    result = cursor_oracle.execute(f"""select 'Гостю ' ||fio|| ' с номером телефона: '|| PHONE_MOBILE|| ' было начислено: '|| bonus_sum ||', списано: '|| bonus_credit_sum ||', за чек на сумму: '|| bill_sum|| ' от '|| TO_CHAR(request_date, 'dd.mm.yyyy') ||',' || RETAIL_POINT_TITLE ||', rrn:'|| ext_request_id ||' Статус операции:' || REQUEST_STATE
                from PAYMENT_OPERATIONS po WHERE REQUEST_ID ='{request_id}'""")

    return result.fetchone()


def get_request_state(item, request_id):
    cursor_oracle = create_session_oracle(item)
    result = cursor_oracle.execute(f"""select REQUEST_STATE
                from PAYMENT_OPERATIONS po WHERE REQUEST_ID ='{request_id}'""")

    return result.fetchone()


def check_request_id(item, ext_request_id):
    cursor_oracle = create_session_oracle(item)
    result = cursor_oracle.execute(f"""select request_id,  ext_request_id ||', '|| request_date ||', '|| 'Сумма счета: ' || bill_sum || ', ' || 'Накоплено: ' || bonus_sum  || ', ' || 'Потрачено: ' || bonus_credit_sum || ', ' || 'Статус: ' || request_state
                from PAYMENT_OPERATIONS po WHERE ext_REQUEST_ID ='{ext_request_id}'""")
    
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
            cursor = connection.cursor()
            request_id = user_data['request_id']
            #!!!!!!!!!!!

            # --Записываем запрос
            logger.info(f"Starting request: {cursor}, {request_date}, {card_id}, {terminal_id}")
            set_request(cursor, request_id, request_date, card_id, terminal_id, 'PAYMENT_AND_CONFIRM', 'READY')
            set_bills(cursor, request_date, bill_sum, request_id)
            set_goods(cursor, request_id, bill_sum)
            set_transactions_debit(cursor, bonus_sum, cli_id, card_id, request_id, point_id, 'READY')
            set_transactions_fee(cursor, org_fee, card_id, cli_id, request_id, point_id, 'READY')
            set_transactions_credit(cursor, card_id, cli_id, request_id, point_id, credit_sum, 'READY')
            update_account(cursor, bonus_sum, org_fee, credit_sum, cli_id)
            logger.info("commiting")
            connection.commit()
            logger.info(f"request_id: {request_id}")
            return request_id
        except Exception as e:
            print(e)
            logger.error(e)
            connection.rollback()
    

def main_deposit(user_data):
    conn = user_data['project_dep']
    with cx_Oracle.connect(conn['user'], conn['password'], conn['connectString']) as connection:
        try:
            point_id = user_data['point_id_dep']
            cli_id = user_data['cli_id_dep']
            terminal_id = user_data['terminal_id_dep']
            bill_sum = user_data['bill_summ']
            card_id = user_data['card_id_dep']
            request_date = user_data['request_date']
            cursor = connection.cursor()
            request_id = user_data['request_id_dep']
            #!!!!!!!!!!!

            # --Записываем запрос
            logger.info(f"Starting request: {cursor}, {request_date}, {card_id}, {terminal_id}")
            set_request(cursor, request_id, request_date, card_id, terminal_id, 'DEPOSIT', 'PROCESSED')
            set_transactions_debit(cursor, bill_sum, cli_id, card_id, request_id, point_id, 'PROCESSED')
            update_account_balance(cursor, bill_sum, 0, 0, cli_id)
            logger.info("commiting")
            connection.commit()
            logger.info(f"request_id: {request_id}")
            return request_id
        except Exception as e:
            logger.error(e)
            connection.rollback()
            return

def set_bonuses(user_data):
    conn = user_data['project']
    with cx_Oracle.connect(conn['user'], conn['password'], conn['connectString']) as connection:
        try:
            card_id = user_data['card_id']
            new_bonus_type = user_data['new_bonus_type']
            new_bonus_percent = user_data['new_cli_percent']
            network_id = user_data['network_id']
            cursor = connection.cursor()
            logger.info(f"Starting change percents: {card_id}, {new_bonus_type}, {new_bonus_percent}, {network_id}")

            histores_id = get_card_histories(cursor, card_id, network_id)
            logger.info(f"history_id: {histores_id}")

            if not histores_id:
                set_history(cursor, card_id, network_id, new_bonus_percent)
            
            else:
                history_id, = histores_id
                set_bonus_type(cursor, card_id, new_bonus_type)
                set_bonus_percent(cursor, history_id, new_bonus_percent)
            
            logger.info("commiting")
            connection.commit()
        except Exception as e:
            print(e)
            logger.error(e)
            connection.rollback()
            
            
            
def set_bonus_type(cursor_oracle, card_id, new_bonus_type):
    logger.info(f"updating card type")
    cursor_oracle.execute(f"""update cards set card_type = '{new_bonus_type}' WHERE card_id = {card_id}""")

    return


def set_bonus_percent(cursor_oracle, history_id, new_bonus_percent):
    logger.info(f"updating card_histories")
    cursor_oracle.execute(f"""update card_histories set current_bonus = {new_bonus_percent} WHERE history_id = {history_id}""")
    
    return

def get_card_histories(cursor_oracle, card_id, network_id):
    history_id = cursor_oracle.execute(f"""SELECT history_id FROM CARD_HISTORIES ch WHERE card_id = {card_id} and network_id={network_id} and ( app_code is null or app_code != 'lyvkit')""")
    return history_id.fetchone()
    
    
def set_history(cursor_oracle, card_id, network_id, current_bonus):
    logger.info(f"inserting card_histories")
    cursor_oracle.execute(f"""
    INSERT INTO CARD_HISTORIES VALUES (CARD_HISTORIES$SEQ.nextval, {card_id}, {network_id}, {current_bonus}, 0, 0, 0, CURRENT_DATE, CURRENT_DATE, 0, 0, (
        SELECT DISTINCT app_codes_regex FROM 
			RETAIL_POINTS rp  
	        JOIN RETAIL_POINT_RULE_UNITS_RELS rpr ON rp.RETAIL_POINT_ID =rpr.RETAIL_POINT_ID
	        JOIN BONUS_RULE_UNITS bru ON rpr.UNIT_ID = bru.unit_id AND bru.TEMPLATE_ID = 1
	        WHERE (bru.APP_CODES_REGEX IS null OR bru.APP_CODES_REGEX != 'lyvkit')
	        AND rpr.IS_DELETE = 0 AND bru.IS_DELETE = 0 AND rp.IS_DELETE = 0 AND bru.END_DATE > CURRENT_DATE 
	        AND rp.NETWORK_ID = {network_id}
        ))""")
    
    return

def get_buh_row(cursor_oracle, request_id):
    logger.info(f"Getting buh_row:")
    result = cursor_oracle.execute(f"""SELECT epr.request_id, brd.EXT_REQUEST_ID, brd.REQUEST_DATE, brd.REQUEST_STATE,  brd.fio,  brd.BILL_SUM, brd.BONUS_SUM, brd.BONUS_CREDIT_SUM, 
        brd.EXTRA_AMOUNT_WITHDRAW, brd.EXTRA_AMOUNT_WITHDRAW_FEE, 
        brd.RETAIL_NETWORK_TITLE, brd.RETAIL_POINT_TITLE, brd.LEGACY_TITLE, bc.TITLE 
        FROM EXTRA_PAYMENT_REQUESTS epr 
        LEFT JOIN PAYMENT_OPERATIONS brd ON epr.request_id=brd.REQUEST_ID 
        LEFT JOIN RETAIL_NETWORKS rn ON rn.RETAIL_NETWORK_ID = brd.RETAIL_NETWORK_ID 
        LEFT JOIN BONUS_CLUBS bc ON bc.CLUB_ID = rn.CLUB_ID 
        WHERE epr.REQUEST_ID  = '{request_id}'""")
    
    return result.fetchall()


def get_buch_request(cursor_oracle, request_id):
    logger.info(f"Getting buch_request:")
    result = cursor_oracle.execute(f"""SELECT request_id FROM EXTRA_PAYMENT_REQUESTS epr WHERE epr.request_id = '{request_id}'""")
    
    return result.fetchone()


def finder_main(LIST_PROJECT, requests):
    result = []
    for key in LIST_PROJECT:
        project = LIST_PROJECT[key]
        logger.info(f"Starting search: {key}")
        with cx_Oracle.connect(project['user'], project['password'], project['connectString']) as connection:
            cursor = connection.cursor()
            rows = []
            for request in requests:
                logger.info(f"Request_id: {request}")
                buh_row = get_buh_row(cursor, request)
                if buh_row:
                    rows.append(buh_row[0])
            
            if len(rows) > 0:
                for i in rows:
                    result.append(i)
    
                
    return result