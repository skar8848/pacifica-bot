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
    from bot.services.hl_copy_engine import start_hl_copy_engine
    from bot.services.grid_engine import start_grid_engine
    from bot.services.funding_arb import start_funding_arb
    from bot.services.gap_monitor import start_gap_monitor
    from bot.services.pulse_detector import start_pulse_detector
    from bot.services.radar_scanner import start_radar_scanner
    from bot.services.regime_classifier import start_regime_classifier
    from bot.services.risk_guardian import start_risk_guardian
    from bot.services.mean_reversion import start_mean_reversion
    from bot.services.bracket_orders import start_bracket_engine
    from bot.services.reconciliation import start_reconciliation_service

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
    asyncio.create_task(start_hl_copy_engine(bot))
    asyncio.create_task(start_grid_engine(bot))
    asyncio.create_task(start_funding_arb(bot))
    asyncio.create_task(start_gap_monitor(bot))
    asyncio.create_task(start_pulse_detector(bot))
    asyncio.create_task(start_radar_scanner(bot))
    asyncio.create_task(start_regime_classifier(bot))
    asyncio.create_task(start_risk_guardian(bot))
    asyncio.create_task(start_mean_reversion(bot))
    asyncio.create_task(start_bracket_engine(bot))
    asyncio.create_task(start_reconciliation_service(bot))
    logger.info("Background services started (22 services).")


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
    from bot.services.hl_copy_engine import stop_hl_copy_engine
    from bot.services.grid_engine import stop_grid_engine
    from bot.services.funding_arb import stop_funding_arb
    from bot.services.gap_monitor import stop_gap_monitor
    from bot.services.pulse_detector import stop_pulse_detector
    from bot.services.radar_scanner import stop_radar_scanner
    from bot.services.regime_classifier import stop_regime_classifier
    from bot.services.risk_guardian import stop_risk_guardian
    from bot.services.mean_reversion import stop_mean_reversion
    from bot.services.bracket_orders import stop_bracket_engine
    from bot.services.reconciliation import stop_reconciliation_service

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
    stop_hl_copy_engine()
    stop_grid_engine()
    stop_funding_arb()
    stop_gap_monitor()
    stop_pulse_detector()
    stop_radar_scanner()
    stop_regime_classifier()
    stop_risk_guardian()
    stop_mean_reversion()
    stop_bracket_engine()
    stop_reconciliation_service()
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

    # Register Dashboard API routes
    from bot.services.dashboard_api import register_dashboard_routes
    register_dashboard_routes(app)
    port = int(os.environ.get("PORT", 10000))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info("Health server running on port %d", port)


async def _keep_alive():
    """Self-ping every 13 min to prevent Render free tier from sleeping.

    Render only counts EXTERNAL HTTP requests as activity.
    RENDER_EXTERNAL_URL is auto-set by Render (e.g. https://xxx.onrender.com).
    """
    import aiohttp
    external_url = os.environ.get("RENDER_EXTERNAL_URL", "")
    if not external_url:
        logger.warning("RENDER_EXTERNAL_URL not set — keep-alive disabled")
        return
    url = f"{external_url}/health"
    logger.info("Keep-alive will ping %s every 13 min", url)
    while True:
        await asyncio.sleep(780)
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)):
                    pass
            logger.debug("Keep-alive ping OK")
        except Exception as e:
            logger.debug("Keep-alive ping failed: %s", e)


async def main():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN not set in .env")

    # Start health check server for Render
    await run_health_server()
    asyncio.create_task(_keep_alive())

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
