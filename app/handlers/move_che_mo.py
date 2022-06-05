from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from .. import dbworker, controller
import logging, uuid, datetime
from app.config_reader import load_config

config = load_config("config/bot.ini")
logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

class DataMove(StatesGroup):
    credles = State()
    project = State()
    project_dep = State()
    network_id = State()
    network_id_dep = State()
    point_id = State()
    point_id_dep = State()
    point_title = State()
    point_title_dep = State()
    terminal_id = State()
    terminal_id_dep = State()
    phone_mobile = State()
    request_date = State()
    cli_id = State()
    cli_id_dep = State()
    cli_name = State()
    cli_name_dep = State()
    card_num = State()
    card_num_dep = State()
    cli_name = State()
    cli_name_dep = State()
    balance = State()
    balance_dep = State()
    card_id = State()
    card_id_dep = State()
    bill_summ = State()
    answer = State()
    request_id = State()
    org_fee = State()
    request_id_dep = State()
    bonus_sum = State()
    credit_sum = State()
    status = State()

async def move_start(message: types.Message, state: FSMContext):
    logger.info("Authentifications...")
    if message.from_user.id not in [int(i) for i in config.tg_bot.admin_id]:
        await message.answer("Permission denied", reply_markup=types.ReplyKeyboardRemove())
        return

    credles = controller.get_credentials()
    await state.update_data(credles=credles)
    await state.update_data(project_dep=credles['che'])
    await state.update_data(project=credles['chemo'])
    await state.update_data(network_id=28206)
    await state.update_data(network_id_dep=28227)    

    await state.update_data(point_id=29532)
    await state.update_data(point_id_dep=29617)
    logger.info(f"point_id, point_id_dep: 29532, 29617")
    
    await state.update_data(point_title='Тестовая точка Mollie\'s')
    await state.update_data(point_title_dep='Тестовая точка Привилегия')

    await state.update_data(terminal_id=32494)
    await state.update_data(terminal_id_dep=99481)
    logger.info(f"terminal_id, terminal_id_dep: 32494, 99481")

    now = datetime.datetime.now()
    data_example = now.strftime( '%d.%m.%Y %H:%M')
    
    await state.update_data(request_date=data_example)
    logger.info(f"request_date: {datetime}")
    
    await message.answer("Введите телефон гостя:", reply_markup=types.ReplyKeyboardRemove())
    await DataMove.phone_mobile.set()


async def bill_chosen(message: types.Message, state: FSMContext):
    phone_mobile = controller.check_phone(message.text)

    if len(phone_mobile) == 0:
        await message.answer("Не корректный номер телефона.")
        return
    logger.info(f"phone_mobile: {phone_mobile}")
    user_data = await state.get_data()
    
    dyrty_cli = dbworker.get_cli_by_phone(user_data['project'], phone_mobile[0])
    cli = controller.get_nt(dyrty_cli)
    logger.info(f"cli: {cli}")
    
    dyrty_cli_dep = dbworker.get_cli_by_phone(user_data['project_dep'], phone_mobile[0])
    cli_dep = controller.get_nt(dyrty_cli_dep)
    logger.info(f"cli_dep: {cli_dep}")
    
    if len(dyrty_cli) > 1 or len(dyrty_cli_dep) > 1:
        await message.answer("С таким телефона найдено больше одного гостя. Повторите попытку.")
        return

    if len(cli) == 0 or  len(cli_dep) == 0:
        await message.answer("Гость с таким телефоном не найден. Повторите попытку.")
        return

    cli_name, cli_id = controller.get_cli_info(cli) 
    cli_name_dep, cli_id_dep = controller.get_cli_info(cli_dep) 

    card = dbworker.get_card(user_data['project'], cli_id)
    card_id, card_num = card
    balance, = dbworker.get_balance(user_data['project'], cli_id)

    card_dep = dbworker.get_card(user_data['project_dep'], cli_id_dep)
    card_id_dep, card_num_dep = card_dep
    balance_dep, = dbworker.get_balance(user_data['project_dep'], cli_id_dep)
    
    
    await message.reply(f"Mo: {cli_name}, {card_num}, balance :{balance}")
    await message.reply(f"Che: {cli_name_dep}, {card_num_dep}, balance :{balance_dep}")

    await state.update_data(phone_mobile=phone_mobile[0])
    await state.update_data(cli_name=cli_name)
    await state.update_data(cli_name_dep=cli_name_dep)
    await state.update_data(cli_id=cli_id)
    await state.update_data(cli_id_dep=cli_id_dep)
    await state.update_data(card_id_dep=card_id_dep)
    await state.update_data(card_id=card_id)
    await state.update_data(card_num_dep=card_num_dep)
    await state.update_data(card_num=card_num)
    await state.update_data(balance=balance)
    await state.update_data(balance_dep=balance_dep)
    user_data = await state.get_data()
    
    await message.answer("Введите сумму переноса:", reply_markup=types.ReplyKeyboardRemove())
    await DataMove.bill_summ.set()


async def check_bill(message: types.Message, state: FSMContext):
    bill_summ = controller.check_summ(message.text)
    credit_sum = bill_summ
    if not bill_summ:
        await message.answer("Введите цифры.")
        logger.error(f"Bill sum is not valid")
        return
    
    logger.info(f"Bill sum is: {bill_summ}")
    

    
    await state.update_data(bill_summ=bill_summ)
    await state.update_data(credit_sum=credit_sum)
    user_data = await state.get_data()
    balance = user_data["balance"]
    
    if credit_sum > balance:
        await message.answer(f"Не хватает средств на счете({balance}).")
        logger.error(f"Credit sum({credit_sum}) > balance({balance})")
        return
    
    cli_name = user_data["cli_name"]
    cli_name_dep = user_data["cli_name_dep"]
    point_title = user_data["point_title"]
    point_title_dep = user_data["point_title_dep"]
    phone_mobile = user_data['phone_mobile']
    request_id = str(uuid.uuid4())
    request_id_dep = str(uuid.uuid4())
    
    bonus_sum = 0
    org_fee = 0
    
    logger.info(f"request_id_dep, request_id: {request_id_dep, request_id}")
    await state.update_data(request_id=request_id)
    await state.update_data(request_id_dep=request_id_dep)
    await state.update_data(bonus_sum=bonus_sum)
    await state.update_data(org_fee=org_fee)
    
    text = f'Гостю {cli_name}( {phone_mobile} ) в {point_title} будет списано: {bill_summ}, request_id: {request_id}; \
            \r\nГостю {cli_name_dep}( {phone_mobile} ) в {point_title_dep} будет начислено: {bill_summ}, request_id: {request_id_dep}'
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add('Да', '/cancel')
    
    await message.answer(text, reply_markup=keyboard)
    await DataMove.answer.set()


async def aqualizer_chosen(message: types.Message, state: FSMContext):
    
    if message.text != 'Да':
        await message.answer("Пожалуйста, скажите Да")
        logger.info(f"Cancel user")
        return
    await state.update_data(answer=message.text)
    user_data = await state.get_data()
    
    logger.info(f"start main_aqualizing...")
    request_id = dbworker.main_aqualizing(user_data)
    bps = controller.get_bps(user_data['project']['name'])
    answer_bps = controller.processOp(bps, request_id, 'PROCESS_REQUEST')
    final_result, = dbworker.get_operation(user_data['project'], request_id)
    status = controller.check_process(answer_bps)

    if status:
        await message.answer(status, reply_markup=types.ReplyKeyboardRemove())
        await message.answer(final_result)

    else:
        await message.answer(final_result, reply_markup=types.ReplyKeyboardRemove())

    await DataMove.status.set()

    logger.info(f"start main_debit...")
    request_id = dbworker.main_deposit(user_data)
    balance_from, = dbworker.get_balance(user_data['project'], user_data['cli_id'])
    balance, = dbworker.get_balance(user_data['project_dep'], user_data['cli_id_dep'])

    await message.answer(f'MO балланс: {balance_from},\r\nChe балланс: {balance}', reply_markup=types.ReplyKeyboardRemove())
    await state.finish()


def register_handlers_move(dp: Dispatcher):
    dp.register_message_handler(move_start, commands="move", state="*")
    dp.register_message_handler(bill_chosen, state=DataMove.phone_mobile)
    dp.register_message_handler(check_bill, state=DataMove.bill_summ)
    dp.register_message_handler(aqualizer_chosen, state=DataMove.answer)
    

