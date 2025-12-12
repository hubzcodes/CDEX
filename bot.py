
# Compose and send alert"""
CDEXSCOPE - bot.py
Telegram alert bot for new Solana memecoins / high-volume low-marketcap calls.

Notes:
- Project name: CDEXSCOPE
- Written for readability and production-level structure.
- Uses asyncio, websockets and REST RPC calls to the Solana RPC provider.
- Configure via environment variables listed in README.

Disclaimer: heuristics and on-chain inferences are not perfect. Use alerts as signals only.
"""

import os
import asyncio
import json
import time
import logging
from typing import Dict, Any, Optional
import aiohttp
import websockets
from collections import defaultdict, deque
from datetime import datetime, timezone

# Telegram: we'll use basic async HTTP send (no heavy lib) to keep dependencies small.
PROJECT_NAME = "CDEXSCOPE"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
SOLANA_WS_URL = os.getenv("SOLANA_WS_URL", "wss://api.mainnet-beta.solana.com")
MIN_MARKET_CAP_USD = float(os.getenv("MIN_MARKET_CAP_USD", "50000"))
MAX_TOKEN_AGE_SECONDS = int(os.getenv("MAX_TOKEN_AGE_SECONDS", str(2*3600)))
ALERT_DEDUPE_SECONDS = int(os.getenv("ALERT_DEDUPE_SECONDS", "600"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# small caches
alert_dedupe: Dict[str, float] = {} # token_address -> last_alert_time
seen_mints = set()

# configure logging
logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
format="%(asctime)s %(levelname)s %(message)s")

# helper http session
_http_session: Optional[aiohttp.ClientSession] = None

def get_http_session() -> aiohttp.ClientSession:
global _http_session
if _http_session is None:
_http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
return _http_session

async def telegram_send(text: str):
if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
logging.warning("Telegram not configured; CDEXSCOPE would send: %s", text[:200])
return
url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode":"MarkdownV2"}
session = get_http_session()
try:
async with session.post(url, json=payload) as resp:
if resp.status != 200:
txt = await resp.text()
logging.warning("Telegram send failed %s: %s", resp.status, txt)
except Exception as e:
logging.exception("telegram_send error: %s", e)
logging.info("Terminated by user")
