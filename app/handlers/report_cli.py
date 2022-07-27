from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from .. import dbworker, controller
import logging, qrcode
import pandas as pd, os

from app.config_reader import load_config

config = load_config("config/bot.ini")
logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

class DataReportCli(StatesGroup):
    credles = State()
    is_che = State()
    project = State()
    card_num = State()
    phone_mobile = State()
    cli_id = State()


async def report_start(message: types.Message, state: FSMContext):
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

    await DataReportCli.project.set()


async def get_phone(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    credles = user_data['credles']
    if message.text not in credles:
        await message.answer("Пожалуйста, ВЫБЕРИТЕ проект:")
        return

    await state.update_data(project=credles[message.text])
    await message.answer("Введите телефон гостя:", reply_markup=types.ReplyKeyboardRemove())
    await DataReportCli.phone_mobile.set()


async def set_report(message: types.Message, state: FSMContext):
    phone_mobile = controller.check_phone(message.text)

    if len(phone_mobile) == 0:
        await message.answer("Не корректный номер телефона.")
        return
    logger.info(f"phone_mobile: {phone_mobile}")
    user_data = await state.get_data()
    
    dyrty_cli = dbworker.get_cli_by_phone(user_data['project'], phone_mobile[0])
    cli = controller.get_nt(dyrty_cli)
    logger.info(f"cli: {cli}")
    
    
    if len(dyrty_cli) > 1:
        await message.answer("С таким телефона найдено больше одного гостя. Повторите попытку.")
        return

    if len(cli) == 0:
        await message.answer("Гость с таким телефоном не найден. Повторите попытку.")
        return

    cli_name, cli_id = controller.get_cli_info(cli)
    balance, = dbworker.get_balance(user_data['project'], cli_id)
    
    result = dbworker.get_report_cli(user_data['project'], phone_mobile[0])

    df = pd.DataFrame(result, columns=["ФИО", "Статус", "Дата", "Точка", "Сумма", "Накоплено",  "Трата", "Доплата с б/карты", "РРН"])

    df.to_excel (r'./create_docs/result_report_cli.xlsx', index = False, header=True)
    doc = open(r'./create_docs/result_report_cli.xlsx', 'rb')
    await message.answer(f"{cli_name}, Балланс: {balance}")
    await message.answer_document(doc)
    await state.finish()


def register_handlers_report_cli(dp: Dispatcher):
    dp.register_message_handler(report_start, commands="report_cli", state="*")
    dp.register_message_handler(get_phone, state=DataReportCli.project)
    dp.register_message_handler(set_report, state=DataReportCli.phone_mobile)

