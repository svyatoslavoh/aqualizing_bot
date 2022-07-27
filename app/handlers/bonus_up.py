
from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from .. import dbworker, controller
import logging
from app.config_reader import load_config

config = load_config("config/bot.ini")
logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

class DataBonus(StatesGroup):
    credles = State()
    project = State()
    network_id = State()
    is_discount = State()
    phone_mobile = State()
    cli_name = State()
    cli_id = State()
    card_id = State()
    card_num = State()
    card_type = State()
    cli_percent = State()
    bonus_info = State()
    new_cli_percent = State()
    new_bonus_type = State()
    
async def bonus_up_start(message: types.Message, state: FSMContext):
    logger.info("Authentifications...")
    if message.from_user.id not in [int(i) for i in config.tg_bot.admin_id]:
        await message.answer("Permission denied", reply_markup=types.ReplyKeyboardRemove())
        return

    logger.info("Getting credles")
    credles = controller.get_credentials()
    await state.update_data(credles=credles)
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    
    for name in credles:
        keyboard.add(name)

    await message.answer("Выберите проект:", reply_markup=keyboard)

    await DataBonus.project.set()


async def network_chosen(message: types.Message, state: FSMContext):
    logger.info("Network getting")
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
    await DataBonus.network_id.set()


async def phone_chosen(message: types.Message, state: FSMContext):
    logger.info("Phone getting")
    user_data = await state.get_data()
    network = user_data['networks']

    if message.text not in network.keys():
        await message.answer("Пожалуйста, ВЫБИРИТЕ сеть:")
        return
    await state.update_data(network_id=network[message.text])
    user_data = await state.get_data()
    
    await message.answer("Введите телефон гостя:", reply_markup=types.ReplyKeyboardRemove())
    await DataBonus.phone_mobile.set()


async def change_chosen(message: types.Message, state: FSMContext):
    phone_mobile = controller.check_phone(message.text)
    user_data = await state.get_data()
    
    if len(phone_mobile) == 0:
        await message.answer("Не корректный номер телефона.")
        return
    
    dyrty_cli = dbworker.get_cli_by_phone(user_data['project'], phone_mobile[0])
    cli = controller.get_nt(dyrty_cli)

    if len(cli) > 1:
        await message.answer("С таким телефона найдено больше одного гостя. Повторите попытку.")
        return

    if len(cli) == 0:
        await message.answer("Гость с таким телефоном не найден. Повторите попытку.")
        return

    cli_name, cli_id = controller.get_cli_info(cli)

    card = dbworker.get_card_info(user_data['project'], cli_id)
    card_id, card_num, card_type = card
    bonus_info=dbworker.get_bonus_info(user_data['project'], user_data['network_id'])
    general_max, premium_max, general_min, premium_min, gold_min, gold_max, social_min, social_max = bonus_info
    text=f"""GENERAL: {general_min}-{general_max}; GENERAL_GOLD: {gold_min}-{gold_max}; 
    GENERAL_PREMIUM: {premium_min}-{premium_max}; SOCIAL: {social_min}-{social_max}"""
    is_discount = controller.check_discount(user_data['network_id'], user_data['project'], config)
    cli_percent, = dbworker.get_percent_network(user_data['project'], card_id, user_data['network_id'], is_discount)
    
    logger.info(f"is_discount: {is_discount}")

    await message.answer(text)
    
    await message.reply(f"{cli_name}, {card_num}, {card_type}; Процентная ставка: {cli_percent}")

    await state.update_data(phone_mobile=phone_mobile[0])
    await state.update_data(cli_name=cli_name)
    await state.update_data(cli_id=cli_id)
    await state.update_data(card_id=card_id)
    await state.update_data(card_num=card_num)
    await state.update_data(card_type=card_type)
    await state.update_data(cli_percent=cli_percent)
    await state.update_data(bonus_info=bonus_info)
    await state.update_data(is_discount=is_discount)
    
    user_data = await state.get_data()
    
    await message.answer("Введите ставку:", reply_markup=types.ReplyKeyboardRemove())
    await DataBonus.new_cli_percent.set()

async def change_percent(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    new_cli_percent = controller.check_summ(message.text)

    if not new_cli_percent:
        await message.answer("Введите цифры.")
        logger.error(f"percent is not valid")
        return

    new_bonus_type = controller.check_bonus(user_data['bonus_info'], new_cli_percent)
    logger.info(f"new_bonus_type: {new_bonus_type}")
    
    if not new_bonus_type:
        await message.answer("Вне диапазонов.")
        logger.error(f"percent is not in range")
        return

    await state.update_data(new_cli_percent=round(new_cli_percent))
    await state.update_data(new_bonus_type=new_bonus_type)

    user_data = await state.get_data()
    dbworker.set_bonuses(user_data)
    
    card = dbworker.get_card_info(user_data['project'], user_data['cli_id'])
    card_id, card_num, card_type = card
    cli_percent, = dbworker.get_percent_network(user_data['project'], card_id, user_data['network_id'], user_data['is_discount'])
    cli_name = user_data['cli_name']
    await message.answer(f"Данные изменены: {cli_name}, {card_num}, {card_type}; Процентная ставка: {cli_percent}")
    await state.finish()

def register_handlers_bonus_up(dp: Dispatcher):
    dp.register_message_handler(bonus_up_start, commands="bonus_up", state="*")
    dp.register_message_handler(network_chosen, state=DataBonus.project)
    dp.register_message_handler(phone_chosen, state=DataBonus.network_id)
    dp.register_message_handler(change_chosen, state=DataBonus.phone_mobile)
    dp.register_message_handler(change_percent, state=DataBonus.new_cli_percent)