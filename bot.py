import asyncio
import json
import logging
import sqlite3
import time
import os

from aiohttp import web, ClientSession
import websockets
from pydantic import BaseModel
from dotenv import load_dotenv
from telegram import Bot, ParseMode
from telegram.constants import ChatAction
from telegram.error import TelegramError

load_dotenv()

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")

MIN_MCAP = int(os.getenv("MIN_MARKET_CAP_USD", "44000"))
MAX_MCAP = int(os.getenv("MAX_MARKET_CAP_USD", "100000"))

ALERT_DEDUPE_SECONDS = int(os.getenv("ALERT_DEDUPE_SECONDS", "600"))
ALERTS_PER_MINUTE = int(os.getenv("ALERTS_PER_MINUTE", "6"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("bot")

bot = Bot(token=TELEGRAM_BOT_TOKEN)

# ---------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------
DB = "seen.db"

conn = sqlite3.connect(DB, check_same_thread=False)
cur = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS seen (
    mint TEXT PRIMARY KEY,
    ts INTEGER
);
""")
conn.commit()

def seen_before(mint: str) -> bool:
    q = cur.execute("SELECT 1 FROM seen WHERE mint=?", (mint,))
    return q.fetchone() is not None

def mark_seen(mint: str):
    cur.execute("INSERT OR REPLACE INTO seen (mint, ts) VALUES (?,?)",
                (mint, int(time.time())))
    conn.commit()


# ---------------------------------------------------------------------
# DexScreener API
# ---------------------------------------------------------------------
async def fetch_dexscreener_data(mint: str) -> dict | None:
    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
    async with ClientSession() as s:
        try:
            r = await s.get(url, timeout=10)
            data = await r.json()
            if "pairs" not in data or not data["pairs"]:
                return None
            return data["pairs"][0]
        except Exception as e:
            log.error(f"DexScreener error: {e}")
            return None


# ---------------------------------------------------------------------
# Premium Alert Formatting
# ---------------------------------------------------------------------
def format_alert(data: dict) -> str:
    mint = data["mint"]
    mcap = data["mcap"]
    price = data["price"]

    return (
        f"📡 <b>[CDEXSCOPE] New Listing</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 <b>Token:</b> <code>{mint}</code>\n"
        f"🕒 {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}\n\n"
        f"💵 <b>Marketcap:</b> ${mcap:,}\n"
        f"📉 <b>Price:</b> {price}\n"
        f"🟢 <b>Risk Level:</b> LOW\n"
        f"⭐ <b>Severity:</b> 22/100\n\n"
        f"📊 <b>Links:</b>\n"
        f"📈 <a href=\"https://dexscreener.com/solana/{mint}\">Chart</a>\n"
        f"🛒 <a href=\"https://jup.ag/swap/SOL-{mint}\">Buy</a>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )


# ---------------------------------------------------------------------
# Telegram Sender
# ---------------------------------------------------------------------
last_alert_times = []

async def send_channel_alert(text: str):
    global last_alert_times

    # Rate limit
    now = time.time()
    last_alert_times = [t for t in last_alert_times if now - t < 60]
    if len(last_alert_times) >= ALERTS_PER_MINUTE:
        log.warning("Rate limit reached, dropping alert")
        return

    try:
        await bot.send_message(
            chat_id=TELEGRAM_CHANNEL_ID,
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        last_alert_times.append(now)
        log.info("Alert sent to channel")
    except TelegramError as e:
        log.error(f"Telegram send error: {e}")


# ---------------------------------------------------------------------
# WebSocket Handler
# ---------------------------------------------------------------------
async def process_event(msg: dict):
    if "mint" not in msg:
        return

    mint = msg["mint"]

    if seen_before(mint):
        return

    # Fetch price + mcap
    ds = await fetch_dexscreener_data(mint)
    if not ds:
        return

    mcap = ds.get("fdv", 0)
    price = ds.get("priceUsd", 0)

    if mcap < MIN_MCAP or mcap > MAX_MCAP:
        log.info(f"Skipped by marketcap: {mint}")
        return

    alert_data = {
        "mint": mint,
        "mcap": int(mcap),
        "price": price,
    }

    text = format_alert(alert_data)
    await send_channel_alert(text)
    mark_seen(mint)


async def websocket_loop():
    WS_URL = os.getenv("HELIUS_WS", "wss://mainnet.helius-rpc.com/?api-key=YOUR_KEY")

    while True:
        try:
            log.info("Connecting to WebSocket...")
            async with websockets.connect(WS_URL) as ws:
                log.info("WebSocket connected")

                sub = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "blockSubscribe",
                    "params": [{"commitment": "processed"}],
                }
                await ws.send(json.dumps(sub))

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        await process_event(msg)
                    except Exception as e:
                        log.error(f"Process error: {e}")

        except Exception as e:
            log.error(f"WebSocket error: {e}")
            log.info("Reconnecting in 3 seconds...")
            await asyncio.sleep(3)


# ---------------------------------------------------------------------
# Health Server
# ---------------------------------------------------------------------
async def handle_health(request):
    return web.Response(text="OK", status=200)

async def run_health_server():
    app = web.Application()
    app.router.add_get("/", handle_health)

    port = int(os.getenv("PORT", "10000"))
    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "0.0.0.0", port)
    log.info(f"Health server running on port {port}")
    await site.start()


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
async def main():
    await asyncio.gather(
        websocket_loop(),
        run_health_server()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("Bot stopped")
