# bot.py
"""
CDEXSCOPE — production-grade single-file bot.

Drop this file into your repo and deploy to Render (or run locally).
Set TELEGRAM_TOKEN in environment before running.

Environment variables (recommended to set in Render):
- TELEGRAM_TOKEN (required)
- TELEGRAM_CHAT_ID (default set below to your channel: -1003312132383)
- TELEGRAM_ADMIN_CHAT (default your DM admin: 8066430050)
- HELIUS_KEY (recommended)
- SOLANA_WS_URL (optional; defaults to Helius)
- SOLANA_RPC_URL (optional; defaults to Helius)
- MIN_MARKET_CAP_USD (optional)
- MAX_TOKEN_AGE_SECONDS (optional)
- ALERTS_PER_MINUTE (optional)
- ALERT_DEDUPE_SECONDS (optional)
- LOG_LEVEL (optional, DEBUG/INFO)
- CDEX_DB_FILE (optional)
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
import html
from typing import Optional, Dict, Any, Set
from datetime import datetime, timezone

import httpx
import websockets

# -------------------------
# Configuration defaults
# -------------------------
PROJECT = "CDEXSCOPE"

# Secrets — set TELEGRAM_TOKEN in Render
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")  # REQUIRED
# Default to your provided channel id — change in env if you want notifications somewhere else
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "-1003312132383")
# Admin who can run /mute /unmute /setmincap — default to the DM id you gave earlier
TELEGRAM_ADMIN_CHAT = os.getenv("TELEGRAM_ADMIN_CHAT", "8066430050")

HELIUS_KEY = os.getenv("HELIUS_KEY", "")
SOLANA_WS_URL = os.getenv("SOLANA_WS_URL", f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}")
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", f"https://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}")

JUPITER_PRICE_API = os.getenv("JUPITER_PRICE_API", "https://quote-api.jup.ag/v1/price")
USDC_MINT = os.getenv("USDC_MINT", "EPjFWdd5AufqSSqeM2qN1zN7K4m3o8fM7k8UXfJv")

# Tunables
MIN_MARKET_CAP_USD = float(os.getenv("MIN_MARKET_CAP_USD", "50000"))
MAX_TOKEN_AGE_SECONDS = int(os.getenv("MAX_TOKEN_AGE_SECONDS", str(2 * 3600)))
ALERTS_PER_MINUTE = int(os.getenv("ALERTS_PER_MINUTE", "6"))
ALERT_DEDUPE_SECONDS = int(os.getenv("ALERT_DEDUPE_SECONDS", "600"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

DB_FILE = os.getenv("CDEX_DB_FILE", "cdexscope.db")

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
# httpx Async client
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
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS seen_mints (
            mint TEXT PRIMARY KEY,
            created_at INTEGER,
            last_alert_ts INTEGER
        );
        """
    )
    cur.execute("CREATE TABLE IF NOT EXISTS muted_mints (mint TEXT PRIMARY KEY);")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mint TEXT,
            severity REAL,
            ts INTEGER
        );
        """
    )
    cur.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);")
    conn.commit()
    conn.close()


def db_upsert_seen(mint: str, created_at: Optional[int]):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO seen_mints (mint, created_at, last_alert_ts) VALUES (?, ?, COALESCE((SELECT last_alert_ts FROM seen_mints WHERE mint = ?), NULL))",
        (mint, created_at, mint),
    )
    conn.commit()
    conn.close()


def db_is_muted(mint: str) -> bool:
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM muted_mints WHERE mint = ?", (mint,))
    r = cur.fetchone()
    conn.close()
    return bool(r)


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
    cur.execute("UPDATE seen_mints SET last_alert_ts = ? WHERE mint = ?", (ts, mint))
    conn.commit()
    conn.close()


def db_get_meta(k: str) -> Optional[str]:
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT v FROM meta WHERE k = ?", (k,))
    r = cur.fetchone()
    conn.close()
    return r[0] if r else None


def db_set_meta(k: str, v: str):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO meta (k, v) VALUES (?, ?)", (k, v))
    conn.commit()
    conn.close()


def db_get_muted_list() -> Set[str]:
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT mint FROM muted_mints")
    rows = cur.fetchall()
    conn.close()
    return set(r[0] for r in rows)


# -------------------------
# Telegram helpers (safe)
# -------------------------
async def telegram_send(text: str) -> bool:
    """
    HTML-escape and send to TELEGRAM_CHAT_ID (the channel by default)
    Retries and logs failures.
    """
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured (TELEGRAM_TOKEN/TELEGRAM_CHAT_ID).")
        return False

    safe = html.escape(text)
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": safe, "parse_mode": "HTML"}
    client = get_http_client()
    for attempt in range(3):
        try:
            r = await client.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", json=payload)
            if r.status_code == 200:
                return True
            else:
                body = await r.aread()
                log.warning("telegram_send failed status=%s body=%s", r.status_code, body)
        except Exception as e:
            log.exception("telegram_send exception: %s", e)
        await asyncio.sleep(1 + attempt * 2)
    return False


async def telegram_get_updates(offset: Optional[int] = None) -> Dict[str, Any]:
    if not TELEGRAM_TOKEN:
        return {}
    client = get_http_client()
    params = {}
    if offset:
        params["offset"] = offset
    try:
        r = await client.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates", params=params, timeout=8.0)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.debug("telegram_get_updates error: %s", e)
        return {}


# -------------------------
# Rate limiting + counters
# -------------------------
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
# Solana RPC helpers
# -------------------------
_rpc_id = 1
_rpc_lock = asyncio.Lock()


async def solana_rpc(method: str, params=None, timeout=15) -> Any:
    global _rpc_id
    if params is None:
        params = []
    async with _rpc_lock:
        body = {"jsonrpc": "2.0", "id": _rpc_id, "method": method, "params": params}
        _rpc_id += 1
    client = get_http_client()
    for _ in range(3):
        try:
            r = await client.post(SOLANA_RPC_URL, json=body, timeout=timeout)
            r.raise_for_status()
            return r.json().get("result", r.json())
        except Exception as e:
            log.debug("solana_rpc %s failed: %s", method, e)
            await asyncio.sleep(1)
    raise RuntimeError(f"solana_rpc {method} failed after retries")


async def get_token_supply(mint: str) -> Optional[Dict[str, Any]]:
    try:
        return await solana_rpc("getTokenSupply", [mint])
    except Exception:
        return None


async def get_token_largest_accounts(mint: str) -> Optional[Any]:
    try:
        res = await solana_rpc("getTokenLargestAccounts", [mint])
        return res.get("value") if isinstance(res, dict) else res
    except Exception:
        return None


async def infer_creation_ts(mint: str) -> Optional[int]:
    try:
        sigs = await solana_rpc("getSignaturesForAddress", [mint, {"limit": 40}])
        if not sigs:
            return None
        oldest = sigs[-1]
        slot = oldest.get("slot")
        if not slot:
            return None
        bt = await solana_rpc("getBlockTime", [slot])
        return bt
    except Exception:
        return None


# -------------------------
# Price & marketcap estimation
# -------------------------
async def jupiter_price_in_usdc(mint: str) -> Optional[float]:
    try:
        client = get_http_client()
        params = {"inputMint": mint, "outputMint": USDC_MINT}
        r = await client.get(JUPITER_PRICE_API, params=params, timeout=8.0)
        if r.status_code != 200:
            return None
        data = r.json()
        price = data.get("price") or data.get("data", {}).get("price")
        if price:
            return float(price)
    except Exception:
        return None
    return None


async def estimate_marketcap_usd(mint: str) -> Optional[float]:
    # supply
    supply_res = await get_token_supply(mint)
    if not supply_res:
        return None
    val = supply_res.get("value", {})
    amount = float(val.get("amount", 0))
    decimals = int(val.get("decimals") or 0)
    total_supply = amount / (10 ** decimals) if decimals else amount

    # price via Jupiter
    price = await jupiter_price_in_usdc(mint)
    if price:
        return total_supply * price

    # fallback placeholder (on-chain LP parsing not implemented here)
    return None


# -------------------------
# Anti-rug heuristics & scoring
# -------------------------
async def compute_top_holder_pct(mint: str) -> Optional[float]:
    largest = await get_token_largest_accounts(mint)
    if not largest:
        return None
    supply = await get_token_supply(mint)
    if not supply:
        return None
    total_amount = float(supply.get("value", {}).get("amount", 0))
    top_amount = float(largest[0].get("amount", 0))
    if total_amount == 0:
        return None
    return (top_amount / total_amount) * 100.0


def compute_severity(marketcap: Optional[float], top_pct: Optional[float], age_seconds: Optional[int]) -> float:
    score = 0.0
    if marketcap is None:
        score += 25.0
    else:
        if marketcap < 1000:
            score += 40.0
        elif marketcap < 50000:
            score += 30.0
        elif marketcap < 200000:
            score += 15.0
        else:
            score += max(0.0, 5.0 - math.log10(max(marketcap, 1.0)))

    if top_pct is not None:
        if top_pct > 50:
            score += 30.0
        elif top_pct > 30:
            score += 15.0
        elif top_pct > 10:
            score += 5.0

    if age_seconds is not None:
        if age_seconds < 300:
            score += 20.0
        elif age_seconds < 3600:
            score += 10.0
        elif age_seconds < 7200:
            score += 5.0

    return min(100.0, score)


# -------------------------
# Alerts pipeline
# -------------------------
_seen_local: Set[str] = set()
_alerted_cache: Dict[str, float] = {}


def should_alert_dedupe(mint: str) -> bool:
    now = time.time()
    last = _alerted_cache.get(mint)
    if last and now - last < ALERT_DEDUPE_SECONDS:
        return False
    _alerted_cache[mint] = now
    return True


async def analyze_and_alert(mint: str, triggering_sig: Optional[str]):
    if not should_alert_dedupe(mint):
        log.debug("Deduped %s", mint)
        return

    created_ts = await infer_creation_ts(mint)
    db_upsert_seen(mint, created_ts)

    if db_is_muted(mint):
        log.info("Mint %s is muted; skipping", mint)
        return

    age_seconds = None
    if created_ts:
        age_seconds = int(time.time() - created_ts)
        if age_seconds > MAX_TOKEN_AGE_SECONDS:
            log.info("Mint %s older than threshold (%s), skipping", mint, age_seconds)
            return

    marketcap = await estimate_marketcap_usd(mint)
    if marketcap is not None and marketcap > MIN_MARKET_CAP_USD:
        log.info("Mint %s marketcap $%s above threshold (min=%s), skipping", mint, marketcap, MIN_MARKET_CAP_USD)
        return

    top_pct = await compute_top_holder_pct(mint)
    severity = compute_severity(marketcap, top_pct, age_seconds)

    if not can_send_alert():
        log.info("Global rate limit reached, skipping %s", mint)
        return

    lines = [f"<b>{PROJECT} Alert</b>", f"<code>{mint}</code>"]
    if created_ts:
        lines.append(f"Created: {datetime.fromtimestamp(created_ts, tz=timezone.utc).isoformat()}")
    lines.append(f"Marketcap: ${marketcap:,.0f}" if marketcap else "Marketcap: <i>unknown</i>")
    if top_pct is not None:
        lines.append(f"Top holder: {top_pct:.1f}%")
        if top_pct > 40.0:
            lines.append("<b>Flag: top holder >40% (high rug risk)</b>")
    lines.append(f"Severity: <b>{severity:.0f}/100</b>")
    if triggering_sig:
        lines.append(f"Trigger tx: <code>{triggering_sig}</code>")

    body = "\n".join(lines)
    ok = await telegram_send(body)
    if ok:
        db_record_alert(mint, severity)
        log.info("Alert sent for %s severity=%s", mint, severity)
    else:
        log.warning("Failed to send alert for %s", mint)


# -------------------------
# Websocket processing
# -------------------------
async def process_logs_result(result: Dict[str, Any]):
    try:
        logs = result.get("logs", [])
        signature = result.get("signature")
        joined = " ".join(logs)
        if "InitializeMint" not in joined and "MintTo" not in joined and "create_account" not in joined:
            return
        candidates = set()
        for token in joined.split():
            if 42 <= len(token) <= 44 and all(c.isalnum() or c in "-_" for c in token):
                candidates.add(token)
        for t in candidates:
            if t in _seen_local:
                continue
            _seen_local.add(t)
            log.info("Candidate mint detected %s sig=%s", t, signature)
            asyncio.create_task(analyze_and_alert(t, signature))
    except Exception as e:
        log.exception("process_logs_result error: %s", e)


async def websocket_loop():
    backoff = 1
    while True:
        try:
            log.info("Connecting to SOL WS %s", SOLANA_WS_URL)
            async with websockets.connect(SOLANA_WS_URL, ping_interval=30, max_size=None) as ws:
                log.info("WS connected")
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
        sleep = min(backoff, 60) + random.random()
        log.info("Reconnect in %.2fs", sleep)
        await asyncio.sleep(sleep)
        backoff = min(backoff * 2, 60)


# -------------------------
# Admin poller (Telegram getUpdates)
# -------------------------
async def process_admin_update(update: Dict[str, Any]):
    try:
        if "message" not in update:
            return
        msg = update["message"]
        text = (msg.get("text") or "").strip()
        chat_id = str(msg.get("chat", {}).get("id"))
        is_admin = (TELEGRAM_ADMIN_CHAT != "" and chat_id == str(TELEGRAM_ADMIN_CHAT))
        if not text:
            return
        parts = text.split()
        cmd = parts[0].lower()
        if cmd == "/mute" and len(parts) >= 2 and is_admin:
            mint = parts[1].strip()
            db_mute(mint)
            await telegram_send(f"Muted {mint}")
        elif cmd == "/unmute" and len(parts) >= 2 and is_admin:
            mint = parts[1].strip()
            db_unmute(mint)
            await telegram_send(f"Unmuted {mint}")
        elif cmd == "/setmincap" and len(parts) >= 2 and is_admin:
            try:
                v = float(parts[1])
                db_set_meta("min_marketcap", str(v))
                global MIN_MARKET_CAP_USD
                MIN_MARKET_CAP_USD = v
                await telegram_send(f"MIN_MARKET_CAP_USD set to ${v:,.0f}")
            except Exception:
                await telegram_send("Usage: /setmincap <usd>")
        elif cmd == "/status":
            muted = db_get_muted_list()
            s = f"CDEXSCOPE status\nmuted: {len(muted)}\nmin_marketcap: ${MIN_MARKET_CAP_USD}\nalerts_this_minute: {_alert_counter['count']}"
            await telegram_send(s)
    except Exception as e:
        log.exception("process_admin_update error: %s", e)


async def telegram_poller():
    offset = db_get_meta("tg_offset")
    offset_val = int(offset) if offset else None
    while True:
        try:
            data = await telegram_get_updates(offset_val)
            if not data or not data.get("ok"):
                await asyncio.sleep(1)
                continue
            for upd in data.get("result", []):
                offset_val = max(offset_val or 0, upd.get("update_id", 0) + 1)
                await process_admin_update(upd)
            if offset_val:
                db_set_meta("tg_offset", str(offset_val))
        except Exception as e:
            log.exception("telegram_poller error: %s", e)
        await asyncio.sleep(1.0)


# -------------------------
# Health server (Render)
# -------------------------
async def _handle_tcp_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        await reader.read(1024)
        body = b"OK"
        resp = (
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/plain; charset=utf-8\r\n"
            b"Content-Length: " + str(len(body)).encode() + b"\r\n"
            b"Connection: close\r\n"
            b"\r\n" + body
        )
        writer.write(resp)
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
        log.info("Health server listening on %s", addrs)
        async with server:
            await server.serve_forever()
    except Exception as e:
        log.exception("Health server failed: %s", e)


# -------------------------
# Entrypoint
# -------------------------
async def main():
    log.info("Starting %s production bot", PROJECT)
    init_db()
    mm = db_get_meta("min_marketcap")
    if mm:
        try:
            global MIN_MARKET_CAP_USD
            MIN_MARKET_CAP_USD = float(mm)
        except Exception:
            pass
    if not TELEGRAM_TOKEN:
        log.warning("TELEGRAM_TOKEN not set. Set in env.")
    try:
        await telegram_send(f"{PROJECT} started at {datetime.now(timezone.utc).isoformat()}")
    except Exception:
        pass
    tasks = [
        asyncio.create_task(websocket_loop()),
        asyncio.create_task(start_health_server()),
        asyncio.create_task(telegram_poller()),
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutdown requested")
