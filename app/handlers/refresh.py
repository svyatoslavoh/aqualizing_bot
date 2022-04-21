
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

class DataRefresh(StatesGroup):
    credles = State()
    project = State()
    
async def refresh_start(message: types.Message, state: FSMContext):
    logger.info("Authentifications...")
    if message.from_user.id not in [int(i) for i in config.tg_bot.admin_id]:
        await message.answer("Permission denied", reply_markup=types.ReplyKeyboardRemove())
        return

    logger.info("Getting credles")
    credles = controller.get_all_bps()
    print(credles)
    await state.update_data(credles=credles)
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    
    for name in credles:
        keyboard.add(name)

    await message.answer("Выберите проект:", reply_markup=keyboard)

    await DataRefresh.project.set()


async def refresh(message: types.Message, state: FSMContext):
    logger.info("Refresh start")
    
    user_data = await state.get_data()
    credles = user_data['credles']
    if message.text not in credles:
        await message.answer("Пожалуйста, ВЫБЕРИТЕ проект:")
        return

    await state.update_data(project=credles[message.text])
    user_data = await state.get_data()
    bps_token = controller.get_bps_token(user_data['project']['dataBase'])
    processingExt = user_data['project']['processingExt']
    
    logger.info(f"processingExt: {processingExt}")
    
    response = controller.get_refresh(bps_token, processingExt)
    
    await message.answer(f"Ответ: {response}", reply_markup=types.ReplyKeyboardRemove())
    await state.finish()

def register_handlers_refresh(dp: Dispatcher):
    dp.register_message_handler(refresh_start, commands="refresh", state="*")
    dp.register_message_handler(refresh, state=DataRefresh.project)