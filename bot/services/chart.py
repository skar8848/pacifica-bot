"""
Candlestick chart generation using Pacifica kline data + mplfinance.
"""

import io
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


async def generate_chart(
    symbol: str,
    interval: str = "1h",
    num_candles: int = 48,
    width: int = 800,
    height: int = 400,
) -> bytes | None:
    """Fetch kline data from Pacifica and render a candlestick chart PNG."""
    try:
        import pandas as pd
        import mplfinance as mpf
        import matplotlib
        matplotlib.use("Agg")
    except ImportError:
        logger.warning("mplfinance or pandas not installed — chart generation disabled")
        return None

    # Fetch candle data
    from bot.services.pacifica_client import PacificaClient
    from solders.keypair import Keypair

    client = PacificaClient(account="public", keypair=Keypair())
    try:
        candles = await client.get_kline(symbol, interval)
    except Exception as e:
        logger.error("Failed to fetch klines for %s: %s", symbol, e)
        return None
    finally:
        await client.close()

    if not candles or len(candles) < 3:
        return None

    # Build DataFrame
    records = []
    for c in candles[-num_candles:]:
        records.append({
            "Date": datetime.fromtimestamp(int(c["t"]) / 1000, tz=timezone.utc),
            "Open": float(c["o"]),
            "High": float(c["h"]),
            "Low": float(c["l"]),
            "Close": float(c["c"]),
            "Volume": float(c["v"]),
        })

    df = pd.DataFrame(records)
    df.set_index("Date", inplace=True)

    # Trident dark theme matching the bot design system
    mc = mpf.make_marketcolors(
        up="#10B981",    # green
        down="#EF4444",  # red
        edge="inherit",
        wick="inherit",
        volume={"up": "#10B98166", "down": "#EF444466"},
    )
    style = mpf.make_mpf_style(
        marketcolors=mc,
        facecolor="#0D1117",
        edgecolor="#161B22",
        figcolor="#0D1117",
        gridcolor="#21262D",
        gridstyle="--",
        gridaxis="both",
        y_on_right=True,
        rc={
            "axes.labelcolor": "#8B949E",
            "xtick.color": "#8B949E",
            "ytick.color": "#8B949E",
        },
    )

    # Current price for title
    last_close = df["Close"].iloc[-1]
    prev_close = df["Close"].iloc[-2] if len(df) > 1 else last_close
    change = last_close - prev_close
    change_pct = (change / prev_close * 100) if prev_close else 0
    sign = "+" if change >= 0 else ""
    color = "#10B981" if change >= 0 else "#EF4444"

    title = f"{symbol}  ${last_close:,.2f}  ({sign}{change_pct:.2f}%)"

    # Render to PNG buffer
    buf = io.BytesIO()
    dpi = 100
    fig_w = width / dpi
    fig_h = height / dpi

    fig, axes = mpf.plot(
        df,
        type="candle",
        style=style,
        volume=True,
        title=title,
        figsize=(fig_w, fig_h),
        returnfig=True,
        tight_layout=True,
    )

    # Style title
    axes[0].set_title(title, color="white", fontsize=14, fontweight="bold", loc="left")

    fig.savefig(buf, format="png", dpi=dpi, bbox_inches="tight", facecolor="#0D1117")
    buf.seek(0)
    data = buf.read()
    import matplotlib.pyplot as plt
    plt.close(fig)
    return data
