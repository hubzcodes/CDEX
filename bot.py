import asyncio
import logging
import os
import sqlite3
import time
from collections import deque

import httpx
import uvicorn
from fastapi import FastAPI
from telegram import Bot
from telegram.constants import ParseMode

# ================= CONFIG =================

TOKEN = os.environ["TELEGRAM_TOKEN"]
CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"])

MIN_MC = int(os.environ["MIN_MARKET_CAP_USD"])
MAX_MC = int(os.environ["MAX_MARKET_CAP_USD"])
MIN_VOL = int(os.environ["MIN_VOLUME_USD"])
MIN_LIQ = int(os.environ["MIN_LIQUIDITY_USD"])
MAX_LIQ = int(os.environ["MAX_LIQUIDITY_USD"])

DEDUP_SECONDS = int(os.environ.get("ALERT_DEDUPE_SECONDS", 600))
ALERTS_PER_MIN = int(os.environ.get("ALERTS_PER_MINUTE", 6))

PORT = int(os.environ.get("PORT", 10000))
DEX_URL = "https://api.dexscreener.com/latest/dex/search?q=solana"

# ================= LOGGING =================

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("FREE-SOL-BOT")

# ================= DATABASE =================

db = sqlite3.connect("alerts.db", check_same_thread=False)
db.execute("""
CREATE TABLE IF NOT EXISTS alerts (
    pair TEXT PRIMARY KEY,
    ts INTEGER
)
""")
db.commit()

def seen_recent(pair: str) -> bool:
    r = db.execute("SELECT ts FROM alerts WHERE pair=?", (pair,)).fetchone()
    return r and time.time() - r[0] < DEDUP_SECONDS

def mark_seen(pair: str):
    db.execute("INSERT OR REPLACE INTO alerts VALUES (?, ?)", (pair, int(time.time())))
    db.commit()

# ================= RATE LIMIT =================

alert_times = deque()

def rate_ok():
    now = time.time()
    while alert_times and now - alert_times[0] > 60:
        alert_times.popleft()
    return len(alert_times) < ALERTS_PER_MIN

def mark_rate():
    alert_times.append(time.time())

# ================= TELEGRAM =================

bot = Bot(TOKEN)

async def send_call(p):
    msg = f"""
🚀 **SOLANA LOW-CAP ALERT**

🪙 **{p['baseToken']['name']} ({p['baseToken']['symbol']})**
💰 **MC:** ${int(p['fdv']):,}
💧 **Liquidity:** ${int(p['liquidity']['usd']):,}
📊 **24h Volume:** ${int(p['volume']['h24']):,}

🔁 **DEX:** {p['dexId'].upper()}
🔗 [DexScreener]({p['url']})

⚠️ *Low-cap assets are risky*
"""
    await bot.send_message(
        chat_id=CHAT_ID,
        text=msg,
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True,
    )

# ================= SCANNER =================

async def scanner_loop():
    async with httpx.AsyncClient(timeout=20) as client:
        while True:
            try:
                r = await client.get(DEX_URL)
                pairs = r.json().get("pairs", [])

                for p in pairs:
                    if p.get("chainId") != "solana":
                        continue

                    mc = p.get("fdv") or 0
                    vol = p.get("volume", {}).get("h24", 0)
                    liq = p.get("liquidity", {}).get("usd", 0)

                    if not (MIN_MC <= mc <= MAX_MC):
                        continue
                    if vol < MIN_VOL:
                        continue
                    if not (MIN_LIQ <= liq <= MAX_LIQ):
                        continue
                    if seen_recent(p["pairAddress"]):
                        continue
                    if not rate_ok():
                        continue

                    await send_call(p)
                    mark_seen(p["pairAddress"])
                    mark_rate()

            except Exception as e:
                log.error(f"scanner error: {e}")

            await asyncio.sleep(10)

# ================= HTTP SERVER =================

app = FastAPI()

@app.get("/")
def health():
    return {"status": "ok", "bot": "running"}

# ================= MAIN =================

async def main():
    log.info("FREE Solana bot started")
    asyncio.create_task(scanner_loop())

    config = uvicorn.Config(app, host="0.0.0.0", port=PORT, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())
