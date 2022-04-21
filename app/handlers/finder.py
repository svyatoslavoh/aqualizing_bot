
import pandas as pd, os
from aiogram import Dispatcher, types
from icecream import ic
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from requests import request
from .. import dbworker, controller
import logging
from app.config_reader import load_config

config = load_config("config/bot.ini")
logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

class DataFinder(StatesGroup):
    credles = State()
    project = State()
    
async def finder_start(message: types.Message, state: FSMContext):
    logger.info("Authentifications...")
    if message.from_user.id not in [int(i) for i in config.tg_bot.admin_id]:
        await message.answer("Permission denied", reply_markup=types.ReplyKeyboardRemove())
        return

    logger.info("Getting credles")
    credles = controller.get_credentials()
    await state.update_data(credles=credles)


    await message.answer("Жду файл в формате csv :)")

    await DataFinder.project.set()


async def get_file(message: types.Message, state: FSMContext):
    try:
        await message.document.download()
        
        user_data = await state.get_data()
        
        file = os.listdir(r'./documents/')
        request = pd.read_csv('./documents/' + file[0])
        request.head()
        result = dbworker.finder_main(user_data["credles"], request.request_id.unique())
        # for request in request.request_id:
        #     logger.info(f"Request_id: {request}")
        #     rows = dbworker.finder_main(user_data["credles"], request)
        #     ic(rows)
        #     if rows:
        #         for i in rows:
        #             print(i)
        #             result.append(i)
        df = pd.DataFrame(result, columns=["request_id", "РРН", "Дата", "Статус",  "ФИО", "Сумма счета", "Накоплено", "Потрачено", 
            "Доплата с б/к", "Комиссия за aq", 
            "Сеть", "Заведение", "Юр. лицо", "Клуб"])

        df.to_excel (r'./create_docs/result.xlsx', index = False, header=True)
        doc = open(r'./create_docs/result.xlsx', 'rb')
        
        await message.answer_document(doc)
        
        files = os.listdir(r'./documents/')
        for file in files:
            os.remove(f'./documents/{file}')
    
    except Exception as e:
        logger.error(e)
        files = os.listdir(r'./documents/')
        for file in files:
            os.remove(f'./documents/{file}')
        
    
def register_handlers_finder(dp: Dispatcher):
    dp.register_message_handler(finder_start, commands="finder", state="*")
    dp.register_message_handler(get_file, content_types=[types.ContentType.DOCUMENT], state=DataFinder.project)