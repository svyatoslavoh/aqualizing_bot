from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from .. import dbworker, controller
import logging, uuid
from app.config_reader import load_config

config = load_config("config/bot.ini")
logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

class DataEqu(StatesGroup):
    credles = State()
    project = State()
    networks = State()
    network_id = State()
    points = State()
    point_id = State()
    point_title = State()
    terminals = State()
    terminal_id = State()
    phone_mobile = State()
    request_date = State()
    cli_id = State()
    cli_name = State()
    card_num = State()
    card_id = State()
    cli_persent = State()
    balance = State()
    bill_summ = State()
    credit_sum = State()
    answer = State()
    org_fee = State()
    request_id = State()
    bonus_sum = State()


async def aqualizing_start(message: types.Message, state: FSMContext):
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

    await DataEqu.project.set()


async def network_chosen(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    credles = user_data['credles']
    if message.text not in credles:
        await message.answer("Пожалуйста, ВЫБЕРИТЕ проект:")
        return

    await state.update_data(project=credles[message.text])
    user_data = await state.get_data()
    dyrty_networks = dbworker.get_networks(user_data['project'])
    network = controller.get_nt(dyrty_networks)
    
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    for value in network:
        keyboard.add(value)
    await state.update_data(networks=network)
    
    await message.answer("Выберите сеть:", reply_markup=keyboard)
    await DataEqu.network_id.set()
    
async def point_chosen(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    network = user_data['networks']

    if message.text not in network.keys():
        await message.answer("Пожалуйста, ВЫБИРИТЕ сеть:")
        return
    await state.update_data(network_id=network[message.text])
    logger.info(f"network: {message.text}")

    user_data = await state.get_data()
    dyrty_points = dbworker.get_points(user_data['project'], user_data['network_id'])
    points = controller.get_nt(dyrty_points)
    
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    for value in sorted(points.keys()):
        keyboard.add(value)
    await state.update_data(points=points)
    
    await message.answer("Выберите точку:", reply_markup=keyboard)
    await DataEqu.point_id.set()


async def terminal_chosen(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    points = user_data['points']

    if message.text not in points.keys():
        await message.answer("Пожалуйста, ВЫБИРИТЕ точку:")
        return

    await state.update_data(point_id=points[message.text])
    await state.update_data(point_title=message.text)
    logger.info(f"network: {message.text}")
    
    user_data = await state.get_data()
    dyrty_terminals = dbworker.get_terminals(user_data['project'], user_data['point_id'])
    terminals = controller.get_nt(dyrty_terminals)
    
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)

    for value in sorted(terminals.keys()):
        keyboard.add(value)

    await state.update_data(terminals=terminals)
    
    await message.answer("Выберите терминал:", reply_markup=keyboard)
    await DataEqu.terminal_id.set()
    user_data = await state.get_data()


async def data_chosen(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    terminals = user_data['terminals']

    if message.text not in terminals.keys():
        await message.answer("Пожалуйста, ВЫБИРИТЕ терминал:")
        return

    await state.update_data(terminal_id=terminals[message.text])
    logger.info(f"terminal: {message.text}")
    
    await message.answer("Введите дату и время корректировки в формате 24.02.2022 10:00:", reply_markup=types.ReplyKeyboardRemove() )
    await DataEqu.request_date.set()


async def phone_chosen(message: types.Message, state: FSMContext):
    datetime = controller.check_datetime(message.text)
    if len(datetime) != 1:
        await message.answer("Пожалуйста, Дату:")
        return

    await state.update_data(request_date=datetime[0])
    logger.info(f"request_date: {datetime}")
    
    await message.answer("Введите телефон гостя:", reply_markup=types.ReplyKeyboardRemove())
    await DataEqu.phone_mobile.set()
    
    message.text
    
    user_data = await state.get_data()

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
    
    if len(cli) > 1:
        await message.answer("С таким телефона найдено больше одного гостя. Повторите попытку.")
        return

    if len(cli) == 0:
        await message.answer("Гость с таким телефоном не найден. Повторите попытку.")
        return

    for i,v in cli.items():
        cli_name, cli_id = (i,v)
    

    card = dbworker.get_card(user_data['project'], cli_id)
    card_id, card_num = card
    cli_persent, = dbworker.get_persent(user_data['project'], card_id, user_data['point_id'], user_data['network_id'])
    balance, = dbworker.get_balance(user_data['project'], cli_id)
        
    await message.reply(f"{cli_name}, {card_num}, процентная ставка :{cli_persent}")
    
    await state.update_data(phone_mobile=phone_mobile[0])
    await state.update_data(cli_name=cli_name)
    await state.update_data(cli_id=cli_id)
    await state.update_data(card_id=card_id)
    await state.update_data(card_num=card_num)
    await state.update_data(cli_persent=cli_persent)
    await state.update_data(balance=balance)
    user_data = await state.get_data()
    
    await message.answer("Введите сумму счета:", reply_markup=types.ReplyKeyboardRemove())
    await DataEqu.bill_summ.set()


async def credit_chosen(message: types.Message, state: FSMContext):
    bill_summ = controller.check_summ(message.text)
    if not bill_summ:
        await message.answer("Введите цифры.")
        logger.error(f"Bill sum is not valid")
        return
    
    logger.info(f"Bill sum is: {bill_summ}")
    
    await state.update_data(bill_summ=bill_summ)
    await message.answer("Введите сумму траты:", reply_markup=types.ReplyKeyboardRemove())
    await DataEqu.credit_sum.set()


async def check_bill(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    credit_sum = controller.check_summ(message.text)
    
    if credit_sum is None:
        await message.answer("Введите цифры.")
        logger.error(f"Credit sum is not valid")
        return

    balance = user_data['balance']
    if credit_sum > balance:
        await message.answer(f"Не хватает средств на счете({balance}).")
        logger.error(f"Credit sum({credit_sum}) > balance({balance})")
        return
    
    logger.info(f"Credit sum is: {credit_sum}")
    await state.update_data(credit_sum=credit_sum)
    
    user_data = await state.get_data()

    card_id = user_data['card_id']
    credit_sum = user_data['credit_sum']
    phone_mobile = user_data['phone_mobile']
    point_id = user_data['point_id']
    point_title = user_data['point_title']
    bill_summ = user_data['bill_summ']
    cli_name = user_data['cli_name']
    cli_persent = user_data['cli_persent']
    
    fee, =  dbworker.get_org_fee(user_data['project'], card_id, point_id)
    bonus_sum = round((bill_summ - credit_sum) / 100 * cli_persent, 2)
    org_fee = round((bill_summ - credit_sum) / 100 * fee, 2)
    request_id = str(uuid.uuid4())
    logger.info(f"request_id: {request_id}")
    await state.update_data(org_fee=org_fee)
    await state.update_data(request_id=request_id)
    await state.update_data(bonus_sum=bonus_sum)
    
    text = f'Гостю {cli_name}( {phone_mobile} ) в {point_title} будет начислено: {bonus_sum} , списано: {credit_sum}, комиссия орг:{org_fee}, request_id: {request_id}'
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add('Да', '/cancel')
    
    await message.answer(text, reply_markup=keyboard)
    await DataEqu.answer.set()


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
    answer_bps = controller.processOp(bps, request_id)
    final_result, = dbworker.get_operation(user_data['project'], request_id)
    status = controller.check_process(answer_bps)

    if status:
        await message.answer(status, reply_markup=types.ReplyKeyboardRemove())
        await message.answer(final_result)

        await state.finish()
    else:
        await message.answer(final_result, reply_markup=types.ReplyKeyboardRemove())
        await state.finish()

# async def bill_chosen(message: types.Message, state: FSMContext):
#     if len(check_phone(message.text)) == 0:
#         await message.answer("Не корректный номер телефона.")
#         return
    
    
    
#     user_data = await state.get_data()
#     await message.answer(f"Вы заказали {user_data['chosen_food']} объёмом {message.text.lower()}.\n"
#                          f"Попробуйте теперь заказать еду: /food", reply_markup=types.ReplyKeyboardRemove())
#     await state.finish()


def register_handlers_aqualizing(dp: Dispatcher):
    dp.register_message_handler(aqualizing_start, commands="one", state="*")
    dp.register_message_handler(network_chosen, state=DataEqu.project)
    dp.register_message_handler(point_chosen, state=DataEqu.network_id)
    dp.register_message_handler(terminal_chosen, state=DataEqu.point_id)
    dp.register_message_handler(data_chosen, state=DataEqu.terminal_id)
    dp.register_message_handler(phone_chosen, state=DataEqu.request_date)
    dp.register_message_handler(bill_chosen, state=DataEqu.phone_mobile)
    dp.register_message_handler(credit_chosen, state=DataEqu.bill_summ)
    dp.register_message_handler(check_bill, state=DataEqu.credit_sum)
    dp.register_message_handler(aqualizer_chosen, state=DataEqu.answer)

