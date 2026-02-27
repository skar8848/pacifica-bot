"""
Entry point — bot startup and shutdown.
"""

import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.memory import MemoryStorage

from bot.config import TELEGRAM_BOT_TOKEN
from database.db import get_db, close_db

from bot.handlers.start import router as start_router
from bot.handlers.trading import router as trading_router
from bot.handlers.portfolio import router as portfolio_router
from bot.handlers.copy_trade import router as copy_router
from bot.handlers.wallet import router as wallet_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def on_startup(bot: Bot):
    logger.info("Starting Trident bot...")
    await get_db()  # init DB tables
    logger.info("Database initialised.")

    # Start background tasks
    from bot.services.copy_engine import start_copy_engine
    from bot.services.gas_monitor import start_gas_monitor
    from bot.services.alert_monitor import start_alert_monitor

    asyncio.create_task(start_copy_engine(bot))
    asyncio.create_task(start_gas_monitor(bot))
    asyncio.create_task(start_alert_monitor(bot))
    logger.info("Background services started.")


async def on_shutdown(bot: Bot):
    logger.info("Shutting down...")
    from bot.services.copy_engine import stop_copy_engine
    from bot.services.gas_monitor import stop_gas_monitor
    from bot.services.alert_monitor import stop_alert_monitor

    stop_copy_engine()
    stop_gas_monitor()
    stop_alert_monitor()
    await close_db()


async def main():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN not set in .env")

    bot = Bot(
        token=TELEGRAM_BOT_TOKEN,
        default=DefaultBotProperties(parse_mode="HTML"),
    )
    dp = Dispatcher(storage=MemoryStorage())

    dp.include_router(start_router)
    dp.include_router(wallet_router)
    dp.include_router(trading_router)
    dp.include_router(portfolio_router)
    dp.include_router(copy_router)

    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    logger.info("Bot polling started.")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
