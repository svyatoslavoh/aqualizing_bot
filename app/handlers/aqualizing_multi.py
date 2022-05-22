from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from .. import dbworker, controller
import logging, uuid, os, datetime
import pandas as pd
from app.config_reader import load_config

config = load_config("config/bot.ini")
logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

class DataEquMulti(StatesGroup):
    credles = State()
    project = State()
    terminal_id = State()
    request_date = State()
    cli_id = State()
    card_id = State()
    cli_persent = State()
    balance = State()
    bill_summ = State()
    credit_sum = State()
    org_fee = State()
    request_id = State()
    bonus_sum = State()


async def aqualizing_multi_start(message: types.Message, state: FSMContext):
    logger.info("Authentifications...")
    if message.from_user.id not in [int(i) for i in config.tg_bot.admin_id]:
        await message.answer("Permission denied", reply_markup=types.ReplyKeyboardRemove())
        return

    credles = controller.get_credentials()
    await state.update_data(credles=credles)
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    
    for name in credles:
        keyboard.add(name)

    await message.answer("Выберите проект:", reply_markup=keyboard)

    await DataEquMulti.project.set()

async def data_chosen(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    credles = user_data['credles']
    if message.text not in credles:
        await message.answer("Пожалуйста, ВЫБЕРИТЕ проект:")
        return

    project=credles[message.text]
    await state.update_data(project=project)
    logger.info(f"project: {project}")
    now = datetime.datetime.now()
    data_example = now.strftime( '%d.%m.%Y %H:%M')
    
    await message.answer(f"Введите дату и время корректировки в формате {data_example}", reply_markup=types.ReplyKeyboardRemove() )
    await DataEquMulti.request_date.set()


async def get_file(message: types.Message, state: FSMContext):
    datetime = controller.check_datetime(message.text)
    if len(datetime) != 1:
        await message.answer("Пожалуйста, Дату:")
        return

    await state.update_data(request_date=datetime[0])
    logger.info(f"request_date: {datetime}")
    
    doc = open(r'./multi_aqu_doc/example_multi_aqua.xlsx', 'rb')
    await message.answer("Жду файл в формате:") 
    await message.answer_document(doc)

    await DataEquMulti.cli_id.set()


async def process(message: types.Message, state: FSMContext):
    
    await message.document.download()
    
    file = os.listdir(r'./documents/')
    marks = pd.read_excel(io='./documents/' + file[0], 
    dtype={'phone':'string', 'card_num': 'string', 'bill_sum':'int', 'code':'string', 'credit':'int'})
    marks.head()
    controller.rm_docs()
    report = []
    for cli in marks.itertuples():
        try:
            phone = cli.phone
            card_num = cli.card_num
            bill_sum = cli.bill_sum
            terminal_code = cli.code
            credit = cli.credit
            bonus_sum = 0
            
            card_num = cli.card_num
            
            logger.info(f"card_num, phone: {card_num}, {phone}")
            if pd.isnull(cli.card_num) and pd.isnull(cli.phone):
                raise Exception('Empty user data')
            
            user_data = await state.get_data()
            
            if pd.notnull(cli.card_num):
                print(str(card_num))
                card_id, cli_id= dbworker.get_card_by_card(user_data['project'], card_num)
                logger.info(f"card_id, cli_id: {card_id, cli_id}")
            else: 
                phone_mobile = controller.check_phone(str(phone) if pd.notnull(phone) else '')
                logger.info(f"phone_mobile: {phone_mobile}")
                
                dyrty_cli = dbworker.get_cli_by_phone(user_data['project'], phone_mobile[0])
                client = controller.get_nt(dyrty_cli)
                logger.info(f"cli: {client}")
            
                if len(client) > 1:
                    raise Exception("С таким телефона найдено больше одного гостя.")
                if len(client) == 0:
                    raise Exception("Гость с таким телефоном не найден.")

                for i,v in client.items():
                    cli_name, cli_id = (i,v)
                    
                card = dbworker.get_card(user_data['project'], cli_id)
                card_id, card_num = card
                
            bill_sum = cli.bill_sum
            terminal_code = cli.code
            
            cur_term = dbworker.check_terminal(user_data['project'], terminal_code)
            logger.info(f"Cur_term with {terminal_code}: {cur_term}")

            if len(cur_term) == 0:
                raise Exception(f"terminal {terminal_code} is not found")
            elif cur_term[0] == 1 or cur_term[1] == 1:
                raise Exception(f"terminal {terminal_code} is " + str('locked' if cur_term[1] == 0 else 'deleted'))

            terminal_id = cur_term[2]
            point_id, = dbworker.get_point_by_term(user_data['project'], terminal_code)
            network_id, = dbworker.get_network_by_point(user_data['project'], point_id)

            credit_sum = cli.credit
            
            cli_persent, = dbworker.get_persent(user_data['project'], card_id, point_id, network_id)
            balance, = dbworker.get_balance(user_data['project'], cli_id)
            logger.info(f"cli_persent, balance: {cli_persent}, {balance}")
            
            fee, =  dbworker.get_org_fee(user_data['project'], card_id, point_id)
            bonus_sum = round((bill_sum - credit_sum) / 100 * cli_persent, 2)
            org_fee = round((bill_sum - credit_sum) / 100 * fee, 2)
            
            if credit_sum > balance:
                logger.error(f"Credit sum({credit_sum}) > balance({balance})")
                raise Exception(f"Не хватает средств на счете({balance}).")
            
            logger.info(f"Credit sum is: {credit_sum}")
            
            
            fee, =  dbworker.get_org_fee(user_data['project'], card_id, point_id)
            bonus_sum = round((bill_sum - credit_sum) / 100 * cli_persent, 2)
            org_fee = round((bill_sum - credit_sum) / 100 * fee, 2)
            request_id = str(uuid.uuid4())
            logger.info(f"request_id: {request_id}")
            
            await state.update_data(credit_sum = credit_sum)
            await state.update_data(org_fee=org_fee)
            await state.update_data(request_id=request_id)
            await state.update_data(bonus_sum=bonus_sum)
            await state.update_data(point_id=point_id)
            await state.update_data(terminal_id=terminal_id)
            await state.update_data(bill_summ=bill_sum)
            await state.update_data(cli_id=cli_id)
            await state.update_data(card_id=card_id)
            user_data = await state.get_data()
            
            logger.info(f"start main_aqualizing... with: {user_data}")
            request_id = dbworker.main_aqualizing(user_data)
            bps = controller.get_bps(user_data['project']['name'])
            answer_bps = controller.processOp(bps, request_id)
            final_result, = dbworker.get_operation(user_data['project'], request_id)
            status = controller.check_process(answer_bps)

            request_state = dbworker.get_request_state(user_data['project'], request_id)
            report.append({'phone': phone, 'card_num': card_num, 'bill sum': bill_sum, 'bonus_sum': bonus_sum, 'bonus_credit_sum': credit, 'terminal_code': terminal_code, 'status': {request_state}, 'detailes': ''})

        except Exception as e:
            report.append({'phone': phone, 'card_num': card_num, 'bill sum': bill_sum, 'bonus_sum': bonus_sum, 'bonus_credit_sum': credit, 'terminal_code': terminal_code, 'status': 'error', 'detailes': e})
    
        
    df = pd.DataFrame(report)
    df.to_excel (r'./multi_aqu_doc/create_docs/result.xlsx', index = False, header=True)
    doc = open(r'./multi_aqu_doc/create_docs/result.xlsx', 'rb')
    
    await message.answer_document(doc)
    await state.finish()


def register_handlers_multi(dp: Dispatcher):
    dp.register_message_handler(aqualizing_multi_start, commands="many", state="*")
    dp.register_message_handler(data_chosen, state=DataEquMulti.project)
    dp.register_message_handler(get_file, state=DataEquMulti.request_date)
    dp.register_message_handler(process,content_types=[types.ContentType.DOCUMENT], state=DataEquMulti.cli_id)


