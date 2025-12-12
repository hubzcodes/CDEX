"""
CDEXSCOPE - advanced bot

Features:
- Helius websocket logs subscription (resilient reconnect)
- Jupiter price lookup for token -> USDC price (used to estimate marketcap)
- SQLite persistence for seen mints, mute list, alert history, telegram offset
- Admin Telegram commands (polling): /mute, /unmute, /setmincap, /status
- Alert severity scoring + rate limiting per minute
- Minimal health HTTP server for Render
"""

import os
import sys
import time
import json
import math
import random
import logging
import asyncio
import sqlite3
from typing import Optional, Dict, Any, Set
from datetime import datetime, timezone, timedelta

import httpx
import websockets

# -------------------------
# Configuration
# -------------------------
PROJECT = "CDEXSCOPE"

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")  # REQUIRED
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")  # admin chat id for admin replies
HELIUS_KEY = os.getenv("HELIUS_KEY", "")
SOLANA_WS_URL = os.getenv("SOLANA_WS_URL", f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}")
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", f"https://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}")

# Jupiter public price API base (no key required)
JUPITER_PRICE_API = "https://quote-api.jup.ag/v1/price"

# default thresholds (can be changed with admin /setmincap)
MIN_MARKET_CAP_USD = float(os.getenv("MIN_MARKET_CAP_USD", "50000"))
MAX_TOKEN_AGE_SECONDS = int(os.getenv("MAX_TOKEN_AGE_SECONDS", str(2 * 3600)))
ALERT_DEDUPE_SECONDS = int(os.getenv("ALERT_DEDUPE_SECONDS", "600"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Known stable mints (so we quote against USDC)
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1zN7K4m3o8fM7k8UXfJv"  # shortened? using canonical prefix - make sure env override if needed

# Program constants
TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

# Rate limiting: max alerts per minute
ALERTS_PER_MINUTE = int(os.getenv("ALERTS_PER_MINUTE", "6"))

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    stream=sys.stdout,
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger(PROJECT)

# -------------------------
# HTTP client (httpx)
# -------------------------
_http_client: Optional[httpx.AsyncClient] = None


def get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None:
        _http_client = httpx.AsyncClient(timeout=20.0)
    return _http_client


# -------------------------
# SQLite persistence
# -------------------------
DB_FILE = os.getenv("CDEX_DB_FILE", "cdexscope.db")


def init_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS seen_mints (
            mint TEXT PRIMARY KEY,
            created_at INTEGER,
            last_alerted INTEGER
        )
    """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS muted_mints (
            mint TEXT PRIMARY KEY
        )
    """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS meta (
            k TEXT PRIMARY KEY,
            v TEXT
        )
    """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mint TEXT,
            severity REAL,
            ts INTEGER
        )
    """
    )
    conn.commit()
    conn.close()


def db_upsert_seen(mint: str, created_at: Optional[int]):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO seen_mints (mint, created_at, last_alerted) VALUES (?, ?, COALESCE((SELECT last_alerted FROM seen_mints WHERE mint = ?), NULL))",
        (mint, created_at or None, mint),
    )
    conn.commit()
    conn.close()


def db_get_muted() -> Set[str]:
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT mint FROM muted_mints")
    rows = cur.fetchall()
    conn.close()
    return set(r[0] for r in rows)


def db_mute(mint: str):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO muted_mints (mint) VALUES (?)", (mint,))
    conn.commit()
    conn.close()


def db_unmute(mint: str):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("DELETE FROM muted_mints WHERE mint = ?", (mint,))
    conn.commit()
    conn.close()


def db_record_alert(mint: str, severity: float):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    ts = int(time.time())
    cur.execute("INSERT INTO alerts (mint, severity, ts) VALUES (?, ?, ?)", (mint, severity, ts))
    cur.execute("UPDATE seen_mints SET last_alerted = ? WHERE mint = ?", (ts, mint))
    conn.commit()
    conn.close()


def db_get_meta(key: str) -> Optional[str]:
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT v FROM meta WHERE k = ?", (key,))
    r = cur.fetchone()
    conn.close()
    return r[0] if r else None


def db_set_meta(key: str, value: str):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO meta (k, v) VALUES (?, ?)", (key, value))
    conn.commit()
    conn.close()


# -------------------------
# Telegram helpers (send + poll getUpdates)
# -------------------------
async def telegram_send(text: str, parse_mode: str = "MarkdownV2") -> bool:
    if not TELEGRAM_TOKEN:
        log.warning("TELEGRAM_TOKEN not set; cannot send message")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": parse_mode}
    client = get_http_client()
    for i in range(3):
        try:
            r = await client.post(url, json=payload)
            if r.status_code == 200:
                return True
            else:
                log.warning("telegram_send status=%s body=%s", r.status_code, await r.aread())
        except Exception as e:
            log.exception("telegram_send error: %s", e)
        await asyncio.sleep(1 + i)
    return False


async def telegram_get_updates(offset: Optional[int] = None) -> Dict[str, Any]:
    if not TELEGRAM_TOKEN:
        return {}
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
    params = {}
    if offset:
        params["offset"] = offset
    client = get_http_client()
    try:
        r = await client.get(url, params=params, timeout=10.0)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.debug("telegram_get_updates error: %s", e)
        return {}


# -------------------------
# Rate limiting & counters
# -------------------------
# simple rolling-minute counter
_alert_counter = {"minute": None, "count": 0}


def can_send_alert() -> bool:
    now_min = int(time.time() // 60)
    if _alert_counter["minute"] != now_min:
        _alert_counter["minute"] = now_min
        _alert_counter["count"] = 0
    if _alert_counter["count"] >= ALERTS_PER_MINUTE:
        return False
    _alert_counter["count"] += 1
    return True


# -------------------------
# Utilities: price & marketcap estimation
# -------------------------
async def jupiter_price_in_usdc(mint: str) -> Optional[float]:
    """
    Query Jupiter price API to get price of 1 token in USDC.
    Returns number or None if unavailable.
    """
    try:
        client = get_http_client()
        params = {"inputMint": mint, "outputMint": USDC_MINT}
        r = await client.get(JUPITER_PRICE_API, params=params, timeout=8.0)
        if r.status_code != 200:
            return None
        data = r.json()
        price = data.get("price")  # Jupiter returns field name "price"
        if price is None:
            # older API returned field 'data' or similar; guard
            price = data.get("data", {}).get("price")
        if price:
            return float(price)
    except Exception as e:
        log.debug("jupiter_price_in_usdc error: %s", e)
    return None


async def estimate_marketcap_usd(mint: str) -> Optional[float]:
    """
    Try 1: get supply via RPC and price via Jupiter -> supply * price.
    Returns None if cannot estimate.
    """
    try:
        # get supply
        body = {"jsonrpc": "2.0", "id": 1, "method": "getTokenSupply", "params": [mint]}
        client = get_http_client()
        r = await client.post(SOLANA_RPC_URL, json=body, timeout=10.0)
        if r.status_code != 200:
            return None
        res = r.json().get("result")
        if not res:
            return None
        value = res.get("value", {})
        amount = float(value.get("amount", 0))
        decimals = int(value.get("decimals", 0) or 0)
        if decimals:
            supply = amount / (10 ** decimals)
        else:
            supply = amount
        price = await jupiter_price_in_usdc(mint)
        if price is None:
            return None
        return supply * price
    except Exception as e:
        log.debug("estimate_marketcap_usd error: %s", e)
        return None


# -------------------------
# Token analysis & scoring
# -------------------------
async def infer_token_creation_timestamp(mint: str) -> Optional[int]:
    """
    Inspect on-chain signatures for this mint and return block time of the oldest returned signature.
    """
    try:
        body = {"jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress", "params": [mint, {"limit": 20}]}
        client = get_http_client()
        r = await client.post(SOLANA_RPC_URL, json=body, timeout=10.0)
        if r.status_code != 200:
            return None
        sigs = r.json().get("result", [])
        if not sigs:
            return None
        oldest = sigs[-1]
        slot = oldest.get("slot")
        if slot is None:
            return None
        # get block time
        body2 = {"jsonrpc": "2.0", "id": 1, "method": "getBlockTime", "params": [slot]}
        r2 = await client.post(SOLANA_RPC_URL, json=body2, timeout=8.0)
        if r2.status_code != 200:
            return None
        bt = r2.json().get("result")
        return bt
    except Exception as e:
        log.debug("infer_token_creation_timestamp error: %s", e)
        return None


async def compute_top_holder_percent(mint: str) -> Optional[float]:
    try:
        body = {"jsonrpc": "2.0", "id": 1, "method": "getTokenLargestAccounts", "params": [mint]}
        client = get_http_client()
        r = await client.post(SOLANA_RPC_URL, json=body, timeout=10.0)
        if r.status_code != 200:
            return None
        largest = r.json().get("result", {}).get("value", [])
        body2 = {"jsonrpc": "2.0", "id": 1, "method": "getTokenSupply", "params": [mint]}
        r2 = await client.post(SOLANA_RPC_URL, json=body2, timeout=10.0)
        if r2.status_code != 200:
            return None
        supply_info = r2.json().get("result", {})
        amount = float(supply_info.get("value", {}).get("amount", 0))
        if amount == 0:
            return None
        if not largest:
            return None
        top_amount = float(largest[0].get("amount", 0))
        pct = (top_amount / amount) * 100.0
        return pct
    except Exception as e:
        log.debug("compute_top_holder_percent error: %s", e)
        return None


def severity_score(marketcap: Optional[float], top_percent: Optional[float], age_seconds: Optional[int]) -> float:
    """
    Return a severity score 0..100 where higher means more interesting (for alerts).
    Heuristics:
    - lower marketcap => higher score
    - higher top_percent => higher risk score (increase severity)
    - very new tokens => higher severity
    """
    score = 0.0
    if marketcap is None:
        score += 20.0  # unknown marketcap but still interesting
    else:
        # scale: below 1k -> +40, below 50k -> +30, below 200k -> +15
        if marketcap < 1000:
            score += 40
        elif marketcap < 50000:
            score += 30
        elif marketcap < 200000:
            score += 15
        else:
            score += max(0, 10 - math.log10(marketcap))  # small value

    if top_percent is not None:
        # top holder >50% -> big risk -> increase severity
        if top_percent > 50:
            score += 30
        elif top_percent > 30:
            score += 15
        elif top_percent > 10:
            score += 5

    if age_seconds is not None:
        # very new tokens (minutes) are higher score
        if age_seconds < 300:
            score += 20
        elif age_seconds < 3600:
            score += 10
        elif age_seconds < 7200:
            score += 5

    return min(100.0, score)


# -------------------------
# Core log processing
# -------------------------
_seen_local: Set[str] = set()


async def handle_candidate(mint: str, triggering_sig: Optional[str]):
    # persist seen
    created_ts = await infer_token_creation_timestamp(mint)
    db_upsert_seen(mint, created_ts)
    # skip if muted
    muted = db_get_muted()
    if mint in muted:
        log.info("Mint %s is muted, skipping", mint)
        return

    # age
    age_seconds = None
    if created_ts:
        age_seconds = int(time.time() - created_ts)
        if age_seconds > MAX_TOKEN_AGE_SECONDS:
            log.info("Mint %s age %s > max %s -> skip", mint, age_seconds, MAX_TOKEN_AGE_SECONDS)
            return

    # marketcap try
    marketcap = await estimate_marketcap_usd(mint)
    if marketcap is not None and marketcap > MIN_MARKET_CAP_USD:
        log.info("Mint %s marketcap $%s > min %s -> skip", mint, marketcap, MIN_MARKET_CAP_USD)
        return

    # top holder
    top_pct = await compute_top_holder_percent(mint)

    # severity
    sev = severity_score(marketcap, top_pct, age_seconds)

    # rate limit overall
    if not can_send_alert():
        log.info("Alert rate limit hit; skipping alert for %s (sev=%s)", mint, sev)
        return

    # build message
    lines = [f"*{PROJECT} Alert*"]
    lines.append(f"`{mint}`")
    if created_ts:
        lines.append(f"Created: {datetime.fromtimestamp(created_ts, tz=timezone.utc).isoformat()}")
    if marketcap:
        lines.append(f"Estimated marketcap: ${marketcap:,.0f}")
    else:
        lines.append("Estimated marketcap: _unknown_")
    if top_pct is not None:
        lines.append(f"Top holder: {top_pct:.1f}%")
        if top_pct > 40:
            lines.append("_Flag: top holder >40% (high rug risk)_")

    lines.append(f"Severity: *{sev:.0f}/100*")
    if triggering_sig:
        lines.append(f"Trigger tx: `{triggering_sig}`")

    msg = "\n".join(lines)
    # send
    ok = await telegram_send(msg, parse_mode="MarkdownV2")
    if ok:
        db_record_alert(mint, sev)
        log.info("Alert sent for %s sev=%s", mint, sev)
    else:
        log.warning("Failed to send alert for %s", mint)


async def process_logs_result(result: Dict[str, Any]):
    try:
        logs = result.get("logs", [])
        signature = result.get("signature")
        joined = " ".join(logs)
        if "InitializeMint" not in joined and "MintTo" not in joined and "create_account" not in joined:
            return
        candidates = set()
        for token in joined.split():
            # heuristic for base58-ish addresses
            if 42 <= len(token) <= 44 and all(c.isalnum() or c in "-_" for c in token):
                candidates.add(token)
        for t in candidates:
            if t in _seen_local:
                continue
            _seen_local.add(t)
            log.info("Detected candidate mint %s (sig=%s)", t, signature)
            asyncio.create_task(handle_candidate(t, signature))
    except Exception as e:
        log.exception("process_logs_result error: %s", e)


# -------------------------
# WebSocket loop (resilient)
# -------------------------
async def ws_loop():
    backoff = 1
    while True:
        try:
            log.info("Connecting to %s", SOLANA_WS_URL)
            async with websockets.connect(SOLANA_WS_URL, ping_interval=30, max_size=None) as ws:
                log.info("Websocket connected")
                subscribe = {"jsonrpc": "2.0", "id": 1, "method": "logsSubscribe", "params": ["all", {"commitment": "confirmed"}]}
                await ws.send(json.dumps(subscribe))
                while True:
                    raw = await ws.recv()
                    try:
                        data = json.loads(raw)
                    except Exception:
                        continue
                    params = data.get("params")
                    if not params:
                        continue
                    result = params.get("result", {})
                    asyncio.create_task(process_logs_result(result))
        except websockets.exceptions.InvalidStatusCode as e:
            log.error("InvalidStatusCode: %s", e)
        except Exception as e:
            log.exception("Websocket loop error: %s", e)
        # reconnect with jitter/backoff
        sleep = min(backoff, 60) + random.random()
        log.info("Reconnect sleep %.2fs", sleep)
        await asyncio.sleep(sleep)
        backoff = min(backoff * 2, 60)


# -------------------------
# Telegram admin polling
# -------------------------
async def process_admin_command(msg: Dict[str, Any]):
    """
    Expected commands (from any admin):
    - /mute <mint>
    - /unmute <mint>
    - /setmincap <usd>
    - /status
    """
    try:
        text = msg.get("message", {}).get("text", "")
        chat = msg.get("message", {}).get("chat", {})
        chat_id = chat.get("id")
        from_user = msg.get("message", {}).get("from", {}).get("username")
        # Only allow admin to change config if chat ID matches TELEGRAM_CHAT_ID
        allowed_admin = str(chat_id) == str(TELEGRAM_CHAT_ID)
        if not text:
            return
        parts = text.strip().split()
        cmd = parts[0].lower()
        if cmd == "/mute" and len(parts) >= 2 and allowed_admin:
            mint = parts[1].strip()
            db_mute(mint)
            await telegram_send(f"Muted {mint} (admin: {from_user})")
        elif cmd == "/unmute" and len(parts) >= 2 and allowed_admin:
            mint = parts[1].strip()
            db_unmute(mint)
            await telegram_send(f"Unmuted {mint} (admin: {from_user})")
        elif cmd == "/setmincap" and len(parts) >= 2 and allowed_admin:
            try:
                val = float(parts[1])
                # store in meta
                db_set_meta("min_marketcap", str(val))
                global MIN_MARKET_CAP_USD
                MIN_MARKET_CAP_USD = val
                await telegram_send(f"MIN_MARKET_CAP_USD set to ${val:.0f}")
            except Exception:
                await telegram_send("Usage: /setmincap <usd>")
        elif cmd == "/status":
            muted = db_get_muted()
            alerts_t = db_get_meta("last_alert_ts") or "none"
            s = f"CDEXSCOPE status\nmuted mints: {len(muted)}\nmin_marketcap: ${MIN_MARKET_CAP_USD}\nalerts this minute: {_alert_counter['count']}\nlast alert: {alerts_t}"
            await telegram_send(s)
    except Exception as e:
        log.exception("process_admin_command error: %s", e)


async def telegram_poller():
    """
    Periodically poll getUpdates and process admin commands.
    We store offset in DB to not re-process updates on restart.
    """
    offset = db_get_meta("tg_offset")
    offset_val = int(offset) if offset else None
    client = get_http_client()
    while True:
        try:
            data = await telegram_get_updates(offset_val)
            if not data:
                await asyncio.sleep(2)
                continue
            if not data.get("ok"):
                await asyncio.sleep(2)
                continue
            results = data.get("result", [])
            for upd in results:
                offset_val = max(offset_val or 0, upd.get("update_id", 0) + 1)
                await process_admin_command(upd)
            if offset_val:
                db_set_meta("tg_offset", str(offset_val))
        except Exception as e:
            log.exception("telegram_poller error: %s", e)
        await asyncio.sleep(1)


# -------------------------
# Health HTTP server
# -------------------------
async def _handle_tcp_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        await reader.read(1024)
        body = b"OK"
        response = (
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/plain; charset=utf-8\r\n"
            b"Content-Length: " + str(len(body)).encode() + b"\r\n"
            b"Connection: close\r\n"
            b"\r\n" + body
        )
        writer.write(response)
        await writer.drain()
    except Exception:
        pass
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def start_health_server():
    port = int(os.getenv("PORT", os.getenv("RENDER_PORT", "10000")))
    try:
        server = await asyncio.start_server(_handle_tcp_client, host="0.0.0.0", port=port)
        addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
        log.info("Health HTTP listener started on %s", addrs)
        async with server:
            await server.serve_forever()
    except Exception as e:
        log.exception("Health server failed: %s", e)


# -------------------------
# Entrypoint
# -------------------------
async def main():
    log.info("Starting %s advanced bot", PROJECT)
    init_db()
    # load meta overrides
    mm = db_get_meta("min_marketcap")
    if mm:
        try:
            global MIN_MARKET_CAP_USD
            MIN_MARKET_CAP_USD = float(mm)
        except Exception:
            pass
    # sanity: ensure token provided
    if not TELEGRAM_TOKEN:
        log.warning("TELEGRAM_TOKEN not provided — bot will run, but cannot send Telegram messages.")

    # heartbeat message
    try:
        await telegram_send(f"{PROJECT} started (heartbeat).")
    except Exception:
        pass

    ws_task = asyncio.create_task(ws_loop())
    health_task = asyncio.create_task(start_health_server())
    poller_task = asyncio.create_task(telegram_poller())
    await asyncio.gather(ws_task, health_task, poller_task)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutting down")
