"""
Entry point — bot startup and shutdown.
"""

import asyncio
import logging
import os
from aiohttp import web

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
from bot.handlers.whale import router as whale_router
from bot.handlers.advanced import router as advanced_router
from bot.handlers.leaders import router as leaders_router

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
    from bot.services.whale_monitor import start_whale_monitor
    from bot.services.liquidation_monitor import start_liquidation_monitor
    from bot.services.funding_monitor import start_funding_monitor
    from bot.services.trailing_stop import start_trailing_stop_service
    from bot.services.dca_engine import start_dca_engine
    from bot.services.twap_engine import start_twap_engine
    from bot.services.onchain_tracker import start_onchain_tracker
    from bot.services.hl_whale_tracker import start_hl_whale_tracker

    asyncio.create_task(start_copy_engine(bot))
    asyncio.create_task(start_gas_monitor(bot))
    asyncio.create_task(start_alert_monitor(bot))
    asyncio.create_task(start_whale_monitor(bot))
    asyncio.create_task(start_liquidation_monitor(bot))
    asyncio.create_task(start_funding_monitor(bot))
    asyncio.create_task(start_trailing_stop_service(bot))
    asyncio.create_task(start_dca_engine(bot))
    asyncio.create_task(start_twap_engine(bot))
    asyncio.create_task(start_onchain_tracker(bot))
    asyncio.create_task(start_hl_whale_tracker(bot))
    logger.info("Background services started.")


async def on_shutdown(bot: Bot):
    logger.info("Shutting down...")
    from bot.services.copy_engine import stop_copy_engine
    from bot.services.gas_monitor import stop_gas_monitor
    from bot.services.alert_monitor import stop_alert_monitor
    from bot.services.whale_monitor import stop_whale_monitor
    from bot.services.market_data import close as close_market_data

    from bot.services.liquidation_monitor import stop_liquidation_monitor
    from bot.services.funding_monitor import stop_funding_monitor
    from bot.services.trailing_stop import stop_trailing_stop_service
    from bot.services.dca_engine import stop_dca_engine
    from bot.services.twap_engine import stop_twap_engine
    from bot.services.onchain_tracker import stop_onchain_tracker
    from bot.services.hl_whale_tracker import stop_hl_whale_tracker

    stop_copy_engine()
    stop_gas_monitor()
    stop_alert_monitor()
    stop_whale_monitor()
    stop_liquidation_monitor()
    stop_funding_monitor()
    stop_trailing_stop_service()
    stop_dca_engine()
    stop_twap_engine()
    stop_onchain_tracker()
    stop_hl_whale_tracker()
    await close_market_data()
    await close_db()


async def health_check(request):
    return web.Response(text="OK")


async def run_health_server():
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)

    # Register Telegram Mini App routes
    from bot.services.miniapp import register_miniapp_routes
    register_miniapp_routes(app)
    port = int(os.environ.get("PORT", 10000))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info("Health server running on port %d", port)


async def main():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN not set in .env")

    # Start health check server for Render
    await run_health_server()

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
    dp.include_router(whale_router)
    dp.include_router(advanced_router)
    dp.include_router(leaders_router)

    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    logger.info("Bot polling started.")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
