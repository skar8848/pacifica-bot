"""
PnL share card generator — creates shareable PNG images for trade results.

Generates a dark-themed card with:
- Position info (symbol, side, leverage)
- Entry/mark price
- PnL ($ and %)
- Trident + Pacifica branding
- Referral link
"""

import io
import math
from PIL import Image, ImageDraw, ImageFont

# Card dimensions
W, H = 800, 480

# Colors
BG_DARK = (13, 17, 23)
BG_CARD = (22, 27, 34)
BORDER_GREEN = (16, 185, 129)
BORDER_RED = (239, 68, 68)
TEXT_WHITE = (255, 255, 255)
TEXT_DIM = (139, 148, 158)
TEXT_GREEN = (52, 211, 153)
TEXT_RED = (248, 113, 113)
ACCENT_BLUE = (56, 189, 248)
PACIFICA_PURPLE = (139, 92, 246)


def _load_font(size: int) -> ImageFont.FreeTypeFont:
    """Load a font, falling back to default if not available."""
    try:
        return ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", size)
    except (OSError, IOError):
        try:
            return ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", size)
        except (OSError, IOError):
            try:
                # macOS fallback
                return ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", size)
            except (OSError, IOError):
                return ImageFont.load_default()


def _draw_rounded_rect(draw: ImageDraw.ImageDraw, xy, radius, fill, outline=None, width=1):
    """Draw a rounded rectangle."""
    x0, y0, x1, y1 = xy
    draw.rounded_rectangle(xy, radius=radius, fill=fill, outline=outline, width=width)


def generate_pnl_card(
    symbol: str,
    side: str,          # "bid" or "ask"
    entry_price: float,
    mark_price: float,
    amount: float,
    leverage: int | float | str = "?",
    pnl_usd: float = 0.0,
    pnl_pct: float = 0.0,
    username: str | None = None,
    ref_code: str | None = None,
) -> bytes:
    """Generate a PnL card as PNG bytes."""

    is_profit = pnl_usd >= 0
    accent = BORDER_GREEN if is_profit else BORDER_RED
    pnl_color = TEXT_GREEN if is_profit else TEXT_RED
    side_label = "LONG" if side == "bid" else "SHORT"
    side_emoji = "LONG" if side == "bid" else "SHORT"

    img = Image.new("RGB", (W, H), BG_DARK)
    draw = ImageDraw.Draw(img)

    # Fonts
    font_title = _load_font(32)
    font_big = _load_font(48)
    font_medium = _load_font(22)
    font_small = _load_font(16)
    font_label = _load_font(14)

    # --- Background card with accent border ---
    card_margin = 20
    _draw_rounded_rect(
        draw,
        (card_margin, card_margin, W - card_margin, H - card_margin),
        radius=16,
        fill=BG_CARD,
        outline=accent,
        width=2,
    )

    # --- Top bar: branding + side badge ---
    # Trident logo text
    draw.text((48, 40), "TRIDENT", fill=TEXT_WHITE, font=font_title)
    draw.text((48 + draw.textlength("TRIDENT", font=font_title) + 8, 47), "on Pacifica", fill=TEXT_DIM, font=font_medium)

    # Side badge (right side)
    badge_text = f"{side_label} {symbol}"
    badge_w = draw.textlength(badge_text, font=font_medium) + 24
    badge_x = W - card_margin - badge_w - 20
    badge_y = 38
    badge_color = (16, 80, 56) if side == "bid" else (80, 20, 20)
    badge_border = accent
    _draw_rounded_rect(
        draw,
        (badge_x, badge_y, badge_x + badge_w, badge_y + 34),
        radius=8,
        fill=badge_color,
        outline=badge_border,
        width=1,
    )
    draw.text(
        (badge_x + 12, badge_y + 5),
        badge_text,
        fill=accent,
        font=font_medium,
    )

    # --- Separator line ---
    draw.line(
        [(card_margin + 20, 90), (W - card_margin - 20, 90)],
        fill=(48, 54, 61),
        width=1,
    )

    # --- Main PnL display ---
    pnl_sign = "+" if is_profit else ""
    pnl_text = f"{pnl_sign}${pnl_usd:,.2f}"
    pnl_pct_text = f"({pnl_sign}{pnl_pct:.2f}%)"

    # PnL dollar value (big, centered)
    pnl_w = draw.textlength(pnl_text, font=font_big)
    pnl_x = (W - pnl_w) / 2
    draw.text((pnl_x, 115), pnl_text, fill=pnl_color, font=font_big)

    # PnL percentage
    pct_w = draw.textlength(pnl_pct_text, font=font_medium)
    pct_x = (W - pct_w) / 2
    draw.text((pct_x, 172), pnl_pct_text, fill=pnl_color, font=font_medium)

    # --- PnL bar indicator ---
    bar_y = 210
    bar_h = 6
    bar_left = card_margin + 40
    bar_right = W - card_margin - 40
    bar_w = bar_right - bar_left

    # Background bar
    _draw_rounded_rect(
        draw, (bar_left, bar_y, bar_right, bar_y + bar_h),
        radius=3, fill=(48, 54, 61),
    )

    # Fill bar (capped at 100%)
    fill_pct = min(abs(pnl_pct) / 100, 1.0) if pnl_pct != 0 else 0.05
    fill_w = max(int(bar_w * fill_pct), 8)
    _draw_rounded_rect(
        draw, (bar_left, bar_y, bar_left + fill_w, bar_y + bar_h),
        radius=3, fill=accent,
    )

    # --- Trade details grid ---
    grid_y = 240
    col_w = (W - card_margin * 2 - 40) // 4
    cols = [
        ("Entry Price", f"${entry_price:,.2f}"),
        ("Mark Price", f"${mark_price:,.2f}"),
        ("Size", f"{amount:,.4f}" if amount < 1 else f"{amount:,.2f}"),
        ("Leverage", f"{leverage}x"),
    ]

    for i, (label, value) in enumerate(cols):
        x = card_margin + 40 + i * col_w
        draw.text((x, grid_y), label, fill=TEXT_DIM, font=font_label)
        draw.text((x, grid_y + 20), value, fill=TEXT_WHITE, font=font_medium)

    # --- Second separator ---
    draw.line(
        [(card_margin + 20, 310), (W - card_margin - 20, 310)],
        fill=(48, 54, 61),
        width=1,
    )

    # --- Price change arrow / visual ---
    arrow_y = 330
    arrow_mid = W // 2

    # Entry → Mark price line with arrow
    entry_x = card_margin + 60
    mark_x = W - card_margin - 60

    draw.text((entry_x, arrow_y), "Entry", fill=TEXT_DIM, font=font_label)
    draw.text(
        (entry_x, arrow_y + 18),
        f"${entry_price:,.2f}",
        fill=TEXT_WHITE,
        font=font_medium,
    )

    # Arrow line
    line_y = arrow_y + 28
    draw.line(
        [(entry_x + 120, line_y), (mark_x - 80, line_y)],
        fill=accent,
        width=2,
    )
    # Arrow head
    ax = mark_x - 80
    draw.polygon(
        [(ax, line_y - 6), (ax + 10, line_y), (ax, line_y + 6)],
        fill=accent,
    )

    draw.text(
        (mark_x - 60, arrow_y),
        "Now",
        fill=TEXT_DIM,
        font=font_label,
    )
    draw.text(
        (mark_x - 60, arrow_y + 18),
        f"${mark_price:,.2f}",
        fill=pnl_color,
        font=font_medium,
    )

    # --- Footer: username + referral + timestamp ---
    footer_y = H - card_margin - 40

    draw.line(
        [(card_margin + 20, footer_y - 8), (W - card_margin - 20, footer_y - 8)],
        fill=(48, 54, 61),
        width=1,
    )

    if username:
        draw.text(
            (card_margin + 40, footer_y + 2),
            f"@{username}",
            fill=ACCENT_BLUE,
            font=font_medium,
        )

    # Referral / bot link (right side)
    bot_text = "t.me/trident_pacifica_bot"
    if ref_code:
        bot_text = f"t.me/trident_pacifica_bot?start=ref_{ref_code}"
    bot_w = draw.textlength(bot_text, font=font_small)
    draw.text(
        (W - card_margin - bot_w - 30, footer_y + 5),
        bot_text,
        fill=PACIFICA_PURPLE,
        font=font_small,
    )

    # Powered by line
    powered_text = "Powered by Pacifica"
    powered_w = draw.textlength(powered_text, font=font_label)
    draw.text(
        ((W - powered_w) / 2, footer_y + 5),
        powered_text,
        fill=TEXT_DIM,
        font=font_label,
    )

    # Export to bytes
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    buf.seek(0)
    return buf.getvalue()
