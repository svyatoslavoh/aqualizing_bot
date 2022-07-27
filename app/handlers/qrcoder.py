from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from .. import dbworker, controller
import logging, qrcode
from app.config_reader import load_config

config = load_config("config/bot.ini")
logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

class DataQRcoder(StatesGroup):
    credles = State()
    is_che = State()
    project = State()
    card_num = State()
    rrn = State()


async def deposit_start(message: types.Message, state: FSMContext):
    logger.info("Authentifications...")
    if message.from_user.id not in [int(i) for i in config.tg_bot.admin_id+config.tg_bot.che]:
        await message.answer("Permission denied", reply_markup=types.ReplyKeyboardRemove())
        return


    credles = controller.get_credentials()
    await state.update_data(credles=credles)

    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    for name in credles:
        keyboard.add(name)

    if message.from_user.id in [int(i) for i in config.tg_bot.che]:
        keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
        keyboard.add('che')

    await message.answer("Выберите проект:", reply_markup=keyboard)

    await DataQRcoder.project.set()


async def get_request(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    credles = user_data['credles']
    if message.text not in credles:
        await message.answer("Пожалуйста, ВЫБЕРИТЕ проект:")
        return

    await state.update_data(project=credles[message.text])
    logger.info(f"Getting request...")
    await message.answer(f'Введите реквест:', reply_markup=types.ReplyKeyboardRemove())
    
    await DataQRcoder.rrn.set()

async def aqualizer_chosen(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    card_nums = dbworker.get_card_by_rrn(user_data['project'], (message.text).strip())
    if card_nums is None:
        await message.answer("RRN не найден")
        logger.info(f"RRN не найден: {message.text}")
        return
    card_num, = card_nums
    await state.update_data(rrn=(message.text).strip())
    await state.update_data(card_id=card_num)
    
    
    logger.info(f'rrn, card_num: {message.text}, {card_num}')

    url = qrcode.make(card_num) 
    url.save("./documents/card_num.png", scale = 8) 
    logger.info(f"QR created.")

    qr = open(r'./documents/card_num.png', 'rb')
    controller.rm_docs()
    await message.answer_photo(qr)
    
    await state.finish()


def register_handlers_qrcoder(dp: Dispatcher):
    dp.register_message_handler(deposit_start, commands="qrcoder", state="*")
    dp.register_message_handler(get_request, state=DataQRcoder.project)
    dp.register_message_handler(aqualizer_chosen, state=DataQRcoder.rrn)
    

