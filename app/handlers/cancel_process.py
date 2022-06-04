
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

class DataProcesCancel(StatesGroup):
    all_bps = State()
    bps = State()
    project = State()
    processing_int = State()
    request_id = State()
    answer = State()
    
async def proces_cancel_start(message: types.Message, state: FSMContext):
    logger.info("Authentifications...")
    if message.from_user.id not in [int(i) for i in config.tg_bot.admin_id]:
        await message.answer("Permission denied", reply_markup=types.ReplyKeyboardRemove())
        return

    logger.info("Getting credles")
    all_bps = controller.get_all_bps()
    credles = controller.get_credentials()
    logger.info(credles)
    await state.update_data(all_bps=all_bps)
    await state.update_data(credles=credles)
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    
    for name in all_bps:
        keyboard.add(name)

    await message.answer("Выберите проект:", reply_markup=keyboard)

    await DataProcesCancel.bps.set()


async def proces_cancel(message: types.Message, state: FSMContext):
    logger.info("Proces_cancel start")
    
    user_data = await state.get_data()
    all_bps = user_data['all_bps']
    credles = user_data['credles']
    
    if message.text not in all_bps:
        await message.answer("Пожалуйста, ВЫБЕРИТЕ проект:")
        return
    
    await state.update_data(bps=all_bps[message.text])
    await state.update_data(project=credles[message.text])
    
    user_data = await state.get_data()
    processing_int = user_data['bps']['processingInt']
    await state.update_data(processing_int=processing_int)
    logger.info(f"processing_int: {processing_int}")
    
    await message.answer("Введите RRN:", reply_markup=types.ReplyKeyboardRemove())

    await DataProcesCancel.request_id.set()
    

async def get_request_id(message: types.Message, state: FSMContext):
    user_data = await state.get_data()    
    request = dbworker.check_request_id(user_data['project'], message.text)

    if request is None:
        await message.answer("RRN не найден")
        logger.info(f"RRN не найден: {message.answer}")
        return
    else:
        request_id, request_info = request
        logger.info(f"request_id: {request_info}")
        await message.reply(f"Данные по операции: {request_info}")

    await state.update_data(request_id=request_id)
    
    text = "Выберите действие:"
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add('Подтвердить', 'Отменить')
    keyboard.add('/cancel')
    
    await message.answer(text, reply_markup=keyboard)
    await DataProcesCancel.answer.set()


async def proces_cancel_chosen(message: types.Message, state: FSMContext):
    if message.text not in ['Подтвердить', 'Отменить']:
        await message.answer("Пожалуйста, выбирите вариант из предложенных")
        return
    await state.update_data(answer=message.text)
    user_data = await state.get_data()
    answer = user_data['answer']
    logger.info(f"start main proces_cancel...")

    
    request_id = user_data['request_id']
    processing_int = user_data['processing_int']

    if answer == 'Подтвердить':
        operation = 'PROCESS_REQUEST'
    elif answer == 'Отменить':
        operation = 'CANCEL_REQUEST'

    answer_bps = controller.processOp(processing_int, request_id, operation)
    logger.info(f"answer_bps: {answer_bps}")

    await message.answer(answer_bps, reply_markup=types.ReplyKeyboardRemove())
    await state.finish()

def register_handlers_cancel_process(dp: Dispatcher):
    dp.register_message_handler(proces_cancel_start, commands="cancel_process", state="*")
    dp.register_message_handler(proces_cancel, state=DataProcesCancel.bps)
    dp.register_message_handler(get_request_id, state=DataProcesCancel.request_id)
    dp.register_message_handler(proces_cancel_chosen, state=DataProcesCancel.answer)