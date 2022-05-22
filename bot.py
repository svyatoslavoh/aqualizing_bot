import asyncio
import logging, os

from aiogram import Bot, Dispatcher
from aiogram.types import BotCommand
from aiogram.contrib.fsm_storage.memory import MemoryStorage

from app.config_reader import load_config
from app.handlers.aqualizing import register_handlers_aqualizing
from app.handlers.aqualizing_multi import register_handlers_multi
from app.handlers.bonus_up import register_handlers_bonus_up
from app.handlers.refresh import register_handlers_refresh
from app.handlers.cancel_process import register_handlers_cancel_process
from app.handlers.common import register_handlers_common
from app.handlers.finder import register_handlers_finder

logger = logging.getLogger(__name__)


async def set_commands(bot: Bot):
    commands = [
        BotCommand(command="/one", description="Единичные корректировки"),
        BotCommand(command="/many", description="Массовые корректировки"),
        BotCommand(command="/bonus_up", description="Изменение ставки бонусирования"),
        BotCommand(command="/refresh", description="Рефреш"),
        BotCommand(command="/cancel_process", description="Отмена/Подтв. операций"),
        BotCommand(command="/finder", description="Поиск банк.операций"),
        BotCommand(command="/cancel", description="Отменить")
    ]
    await bot.set_my_commands(commands)


async def main():
    # Настройка логирования в stdout
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    logger.info("Starting bot")

    # Парсинг файла конфигурации
    config = load_config("config/bot.ini")

    # Объявление и инициализация объектов бота и диспетчера
    # tg_bot = os.environ.get('tg_bot')
    
    bot = Bot(token=config.tg_bot.token)
    dp = Dispatcher(bot, storage=MemoryStorage())

    # registr handlers
    register_handlers_common(dp, config.tg_bot.admin_id)
    register_handlers_aqualizing(dp)
    register_handlers_multi(dp)
    register_handlers_bonus_up(dp)
    register_handlers_refresh(dp)
    register_handlers_cancel_process(dp)
    register_handlers_finder(dp)

    # set bot commands
    await set_commands(bot)

    # start pull]'[p]
    # await dp.skip_updates()  # пропуск накопившихся апдейтов (необязательно)
    await dp.start_polling()


if __name__ == '__main__':
    asyncio.run(main())
