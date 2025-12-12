#!/usr/bin/env python3
"""
CDEXSCOPE — single-file production bot (PTB v20 async)
Features:
- Helius WS (logsSubscribe) with auto-reconnect/backoff
- DexScreener + Jupiter price + supply -> marketcap
- On-chain LP decoding scaffold (Raydium/Orca heuristics)
- SQLite persistence (seen_mints, muted_mints, alerts, meta)
- Admin commands via Telegram (mute/unmute/setmincap/rescan/stats/status/restart)
- Premium message layout with inline buttons
- Rate limiting, dedupe, severity scoring v2
- Marketcap window filtering (default 44k - 100k)
- Webhook forwarding (optional)
- Embedded HTML preview endpoint (asyncio.start_server)
- Redis/Postgres stubs (no external deps required)
- Fully async, designed for Render
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
import base64
import struct
from typing import Optional, Dict, Any, Set, List, Tuple
from datetime import datetime, timezone

import httpx
import websockets

# python-telegram-bot v20
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# ---- Configuration (env) ---------------------------------------------------
PROJECT = "CDEXSCOPE"

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")  # required
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "-1003312132383")
TELEGRAM_ADMIN_CHAT = os.getenv("TELEGRAM_ADMIN_CHAT", "8066430050")

HELIUS_KEY = os.getenv("HELIUS_KEY", "")
SOLANA_WS_URL = os.getenv("SOLANA_WS_URL", f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}")
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", f"https://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}")

DEXSCREENER_TOKEN_API = "https://api.dexscreener.com/latest/dex/tokens/{}"
JUPITER_PRICE_API = "https://quote-api.jup.ag/v1/price"
USDC_MINT = os.getenv("USDC_MINT", "EPjFWdd5AufqSSqeM2qN1zN7K4m3o8fM7k8UXfJv")

MIN_MARKET_CAP_USD = float(os.getenv("MIN_MARKET_CAP_USD", "44000"))
MAX_MARKET_CAP_USD = float(os.getenv("MAX_MARKET_CAP_USD", "100000"))
ALERTS_PER_MINUTE = int(os.getenv("ALERTS_PER_MINUTE", "6"))
ALERT_DEDUPE_SECONDS = int(os.getenv("ALERT_DEDUPE_SECONDS", "600"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
DB_FILE = os.getenv("CDEX_DB_FILE", "cdexscope.db")
WEBHOOK_FORWARD_URL = os.getenv("WEBHOOK_FORWARD_URL", "")

REDIS_URL = os.getenv("REDIS_URL", "")       # stubbed/scaffolded if present
DATABASE_URL = os.getenv("DATABASE_URL", "") # stubbed/scaffolded if present

MAX_WS_BACKOFF = 60
RECHECK_INTERVAL_SECONDS = 60
RESEND_SEVERITY_INCREASE = 15.0

# ---- Logging ----------------------------------------------------------------
logging.basicConfig(
    stream=sys.stdout,
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger(PROJECT)

# ---- HTTPX client (shared) --------------------------------------------------
_http_client: Optional[httpx.AsyncClient] = None


def get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None:
        _http_client = httpx.AsyncClient(timeout=20.0)
    return _http_client


# ---- SQLite persistence ----------------------------------------------------
def init_db() -> None:
    conn = sqlite3.connect(DB_FILE, timeout=30)
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
    cur.execute("CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts (ts);")
    conn.commit()
    conn.close()


def db_upsert_seen(mint: str, created_at: Optional[int]) -> None:
    conn = sqlite3.connect(DB_FILE, timeout=30)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO seen_mints (mint, created_at, last_alert_ts) VALUES (?, ?, COALESCE((SELECT last_alert_ts FROM seen_mints WHERE mint = ?), NULL))",
        (mint, created_at, mint),
    )
    conn.commit()
    conn.close()


def db_is_muted(mint: str) -> bool:
    conn = sqlite3.connect(DB_FILE, timeout=30)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM muted_mints WHERE mint = ?", (mint,))
    r = cur.fetchone()
    conn.close()
    return bool(r)


def db_mute(mint: str) -> None:
    conn = sqlite3.connect(DB_FILE, timeout=30)
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO muted_mints (mint) VALUES (?)", (mint,))
    conn.commit()
    conn.close()


def db_unmute(mint: str) -> None:
    conn = sqlite3.connect(DB_FILE, timeout=30)
    cur = conn.cursor()
    cur.execute("DELETE FROM muted_mints WHERE mint = ?", (mint,))
    conn.commit()
    conn.close()


def db_record_alert(mint: str, severity: float) -> None:
    conn = sqlite3.connect(DB_FILE, timeout=30)
    cur = conn.cursor()
    ts = int(time.time())
    cur.execute("INSERT INTO alerts (mint, severity, ts) VALUES (?, ?, ?)", (mint, severity, ts))
    cur.execute("UPDATE seen_mints SET last_alert_ts = ? WHERE mint = ?", (ts, mint))
    conn.commit()
    conn.close()


def db_get_meta(k: str) -> Optional[str]:
    conn = sqlite3.connect(DB_FILE, timeout=30)
    cur = conn.cursor()
    cur.execute("SELECT v FROM meta WHERE k = ?", (k,))
    r = cur.fetchone()
    conn.close()
    return r[0] if r else None


def db_set_meta(k: str, v: str) -> None:
    conn = sqlite3.connect(DB_FILE, timeout=30)
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO meta (k, v) VALUES (?, ?)", (k, v))
    conn.commit()
    conn.close()


def db_get_recent_alerts(limit: int = 50) -> List[Dict[str, Any]]:
    conn = sqlite3.connect(DB_FILE, timeout=30)
    cur = conn.cursor()
    cur.execute("SELECT mint,severity,ts FROM alerts ORDER BY ts DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    conn.close()
    return [{"mint": r[0], "severity": r[1], "ts": r[2]} for r in rows]


# ---- Telegram helpers (PTB v20) -------------------------------------------
TG_APP = None  # will hold Application instance


async def tg_send_text(text: str, chat_id: Optional[str] = None, buttons: Optional[List[Tuple[str, str]]] = None) -> bool:
    """Send HTML message to chat_id (channel default)."""
    global TG_APP
    if TG_APP is None:
        log.warning("tg_send_text: TG_APP not initialized")
        return False
    target = int(chat_id) if chat_id is not None and str(chat_id).lstrip("-").isdigit() else int(TELEGRAM_CHAT_ID)
    reply_markup = None
    if buttons:
        keyboard = [[InlineKeyboardButton(label, url=url)] for label, url in buttons]
        reply_markup = InlineKeyboardMarkup(keyboard)
    try:
        await TG_APP.bot.send_message(chat_id=target, text=text, parse_mode="HTML", disable_web_page_preview=True, reply_markup=reply_markup)
        log.debug("tg_send_text: sent to %s", target)
        return True
    except Exception as e:
        log.exception("tg_send_text exception: %s", e)
        # attempt to DM admin for diagnostics
        try:
            if TELEGRAM_ADMIN_CHAT and int(TELEGRAM_ADMIN_CHAT) != target:
                await TG_APP.bot.send_message(chat_id=int(TELEGRAM_ADMIN_CHAT), text=f"[CDEXSCOPE DIAG] Failed to send to {target}: {e}")
        except Exception:
            log.exception("tg_send_text diag failed")
        return False


# ---- HTTP helpers (httpx) --------------------------------------------------
async def http_get_json(url: str, params: Optional[dict] = None, timeout: int = 10) -> Optional[dict]:
    client = get_http_client()
    try:
        r = await client.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.debug("http_get_json failed %s %s", url, e)
        return None


# ---- Solana RPC helpers ----------------------------------------------------
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
        sigs = await solana_rpc("getSignaturesForAddress", [mint, {"limit": 50}])
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


# ---- Price & marketcap helpers --------------------------------------------
async def dexscreener_for_mint(mint: str) -> Optional[dict]:
    return await http_get_json(DEXSCREENER_TOKEN_API.format(mint))


async def jupiter_price_in_usdc(mint: str) -> Optional[float]:
    data = await http_get_json(JUPITER_PRICE_API, params={"inputMint": mint, "outputMint": USDC_MINT})
    if not data:
        return None
    price = data.get("price") or (data.get("data", {}) or {}).get("price")
    try:
        return float(price) if price is not None else None
    except Exception:
        return None


# ---- LP decoders (Raydium/Orca) - scaffold --------------------------------
# NOTE: exact borsh structures are complex. This scaffold tries DexScreener first,
# and falls back to scanning provided candidate pool accounts (if you provide them).
async def _rpc_get_account_info(pubkey: str) -> Optional[dict]:
    try:
        res = await solana_rpc("getAccountInfo", [pubkey, {"encoding": "base64", "commitment": "confirmed"}])
        return res
    except Exception:
        return None


async def _try_decode_pool_from_account_data(b64: str) -> Optional[Dict[str, float]]:
    try:
        raw = base64.b64decode(b64)
        L = len(raw)
        # heuristic: search for contiguous u64 pairs that look like reserves
        for i in range(0, min(512, L - 16), 4):
            try:
                a = struct.unpack_from("<Q", raw, i)[0]
                b = struct.unpack_from("<Q", raw, i + 8)[0]
                if 0 < a < 10 ** 30 and 0 < b < 10 ** 30:
                    return {"reserve_a": float(a), "reserve_b": float(b)}
            except Exception:
                continue
    except Exception:
        return None
    return None


async def get_best_pool_price_and_liquidity_onchain(mint: str) -> Optional[Dict[str, Any]]:
    # try dexscreener first (fast)
    ds = await dexscreener_for_mint(mint)
    if ds and isinstance(ds, dict) and ds.get("pairs"):
        for p in ds["pairs"]:
            price = p.get("priceUsd") or p.get("price")
            liq = p.get("liquidityUsd") or p.get("liquidity")
            try:
                if price:
                    return {"price": float(price), "liquidity": float(liq) if liq else None}
            except Exception:
                continue
    # fallback: if you want us to decode exact Raydium/Orca pools, provide candidate pool addresses here
    candidates: List[str] = []
    for acct in candidates:
        info = await _rpc_get_account_info(acct)
        if info and info.get("value") and info["value"].get("data"):
            b64 = info["value"]["data"][0]
            decode = await _try_decode_pool_from_account_data(b64)
            if decode:
                r_a, r_b = decode["reserve_a"], decode["reserve_b"]
                if r_a > 0 and r_b > 0:
                    price = r_b / r_a
                    liquidity_usd = r_b
                    return {"price": float(price), "liquidity": float(liquidity_usd)}
    return None


# ---- Anti-rug heuristics & scoring ----------------------------------------
async def compute_top_holder_pct(mint: str) -> Optional[float]:
    largest = await get_token_largest_accounts(mint)
    supply = await get_token_supply(mint)
    if not largest or not supply:
        return None
    try:
        total_amount = float(supply.get("value", {}).get("amount", 0))
        top_amount = float(largest[0].get("amount", 0))
        if total_amount == 0:
            return None
        return (top_amount / total_amount) * 100.0
    except Exception:
        return None


async def approximate_liquidity_usd(mint: str) -> Optional[float]:
    ds = await dexscreener_for_mint(mint)
    if ds and "pairs" in ds and len(ds["pairs"]) > 0:
        for p in ds["pairs"]:
            liq = p.get("liquidityUsd") or p.get("liquidity")
            if liq:
                try:
                    return float(liq)
                except Exception:
                    continue
    oc = await get_best_pool_price_and_liquidity_onchain(mint)
    if oc and oc.get("liquidity"):
        return float(oc["liquidity"])
    return None


def severity_score_v2(mcap: Optional[float], liquidity: Optional[float], top_pct: Optional[float], age_seconds: Optional[int]) -> float:
    score = 0.0
    if mcap is None:
        score += 30.0
    else:
        if mcap < 1000:
            score += 40.0
        elif mcap < 10000:
            score += 30.0
        elif mcap < 50000:
            score += 20.0
        elif mcap < 200000:
            score += 10.0
        else:
            score += max(0.0, 5.0 - math.log10(max(mcap, 1.0)))
    if liquidity is not None:
        if liquidity < 100:
            score += 20.0
        elif liquidity < 1000:
            score += 10.0
    if top_pct is not None:
        if top_pct > 50:
            score += 30.0
        elif top_pct > 30:
            score += 15.0
        elif top_pct > 10:
            score += 5.0
    if age_seconds is not None:
        if age_seconds < 300:
            score += 15.0
        elif age_seconds < 3600:
            score += 7.0
    return min(100.0, score)


# ---- Message formatting ----------------------------------------------------
def format_premium_message(mint: str, created_iso: Optional[str], mcap: Optional[float], price: Optional[float],
                           top_pct: Optional[float], risk_label: str, severity: float, trigger_tx: Optional[str]) -> Tuple[str, List[Tuple[str, str]]]:
    created_line = created_iso or "unknown"
    mcap_line = f"${mcap:,.0f}" if mcap else "<i>unknown</i>"
    price_line = f"${price:.10f}" if price else "<i>unknown</i>"
    top_holder_line = f"{top_pct:.1f}%" if top_pct is not None else "unknown"

    lines = [
        "📡 <b>[CDEXSCOPE] New Listing</b>",
        "━━━━━━━━━━━━━━━━━━━━━━",
        f"🪙 <b>Token:</b> <code>{mint}</code>",
        f"🕒 {created_line}",
        "",
        f"💵 <b>Marketcap:</b> {mcap_line}",
        f"📉 <b>Price:</b> {price_line}",
        f"👥 <b>Top Holder:</b> {top_holder_line}",
        f"🟢 <b>Risk Level:</b> {risk_label}",
        f"⭐ <b>Severity:</b> {severity:.0f}/100",
        "",
        "📊 <b>Links:</b>",
        f"📈 <a href=\"https://dexscreener.com/solana/{mint}\">Chart</a>",
        f"🛒 <a href=\"https://jup.ag/swap/SOL-{mint}\">Buy</a>",
        "━━━━━━━━━━━━━━━━━━━━━━",
    ]
    if trigger_tx:
        lines.append(f"Trigger tx: <code>{trigger_tx}</code>")
    text = "\n".join(lines)
    buttons = [("🛒 Buy on Jupiter", f"https://jup.ag/swap/SOL-{mint}"), ("📈 Chart", f"https://dexscreener.com/solana/{mint}")]
    return text, buttons


# ---- Dedupe and rate limiter ------------------------------------------------
_seen_local: Set[str] = set()
_alerted_cache: Dict[str, float] = {}
_alert_counter = {"minute": None, "count": 0}


def should_alert_dedupe(mint: str) -> bool:
    now = time.time()
    last = _alerted_cache.get(mint)
    if last and now - last < ALERT_DEDUPE_SECONDS:
        return False
    _alerted_cache[mint] = now
    return True


def can_send_alert() -> bool:
    now_min = int(time.time() // 60)
    if _alert_counter["minute"] != now_min:
        _alert_counter["minute"] = now_min
        _alert_counter["count"] = 0
    if _alert_counter["count"] >= ALERTS_PER_MINUTE:
        return False
    _alert_counter["count"] += 1
    return True


# ---- Webhook forwarder -----------------------------------------------------
async def forward_webhook(payload: dict) -> None:
    if not WEBHOOK_FORWARD_URL:
        return
    try:
        client = get_http_client()
        r = await client.post(WEBHOOK_FORWARD_URL, json=payload, timeout=8.0)
        log.info("webhook_forward -> status=%s", r.status_code)
    except Exception as e:
        log.exception("forward_webhook failed: %s", e)


# ---- Analyzer pipeline -----------------------------------------------------
async def analyze_and_alert(mint: str, triggering_sig: Optional[str]) -> None:
    try:
        if not should_alert_dedupe(mint):
            log.debug("deduped %s", mint)
            return

        created_ts = await infer_creation_ts(mint)
        db_upsert_seen(mint, created_ts if created_ts else None)

        if db_is_muted(mint):
            log.info("mint %s is muted, skip", mint)
            return

        age_seconds = None
        created_iso = None
        if created_ts:
            age_seconds = int(time.time() - created_ts)
            created_iso = datetime.fromtimestamp(created_ts, tz=timezone.utc).isoformat()
            max_age = int(os.getenv("MAX_TOKEN_AGE_SECONDS", str(2 * 3600)))
            if age_seconds > max_age:
                log.info("mint %s older than max_age (%s), skip", mint, age_seconds)
                return

        mcap = None
        price = None
        liquidity = None

        # 1) DexScreener
        ds = await dexscreener_for_mint(mint)
        if ds and isinstance(ds, dict) and ds.get("pairs"):
            for p in ds["pairs"]:
                try:
                    pricep = p.get("priceUsd") or p.get("price")
                    fdv = p.get("fdv")
                    liq = p.get("liquidityUsd") or p.get("liquidity")
                    if pricep:
                        price = float(pricep)
                    if fdv:
                        try:
                            mcap = float(fdv)
                        except Exception:
                            mcap = None
                    if liq:
                        try:
                            liquidity = float(liq)
                        except Exception:
                            liquidity = None
                    if price is not None:
                        break
                except Exception:
                    continue

        # 2) Jupiter price + supply -> mcap
        if mcap is None:
            jp = await jupiter_price_in_usdc(mint)
            if jp is not None:
                supply_res = await get_token_supply(mint)
                if supply_res:
                    val = supply_res.get("value", {})
                    amount = float(val.get("amount", 0))
                    decimals = int(val.get("decimals") or 0)
                    total_supply = amount / (10 ** decimals) if decimals else amount
                    if total_supply > 0:
                        mcap = total_supply * jp
                        price = jp

        # 3) On-chain pool fallback
        if (mcap is None or liquidity is None) and (price is None or liquidity is None):
            oc = await get_best_pool_price_and_liquidity_onchain(mint)
            if oc:
                if oc.get("price") and price is None:
                    price = float(oc["price"])
                if oc.get("liquidity") and liquidity is None:
                    liquidity = float(oc["liquidity"])
                if mcap is None and price is not None:
                    supply_res = await get_token_supply(mint)
                    if supply_res:
                        val = supply_res.get("value", {})
                        amount = float(val.get("amount", 0))
                        decimals = int(val.get("decimals") or 0)
                        total_supply = amount / (10 ** decimals) if decimals else amount
                        if total_supply > 0:
                            mcap = total_supply * price

        top_pct = await compute_top_holder_pct(mint)
        sev = severity_score_v2(mcap, liquidity, top_pct, age_seconds)

        # marketcap window
        if mcap is not None:
            if not (MIN_MARKET_CAP_USD <= mcap <= MAX_MARKET_CAP_USD):
                log.info("mint %s mcap %s outside window %s-%s -> skip", mint, mcap, MIN_MARKET_CAP_USD, MAX_MARKET_CAP_USD)
                return

        if not can_send_alert():
            log.info("global rate limit reached; skip %s", mint)
            return

        risk_label = "LOW"
        if sev >= 70 or (top_pct and top_pct > 50):
            risk_label = "HIGH"
        elif sev >= 40:
            risk_label = "MEDIUM"

        text, buttons = format_premium_message(mint, created_iso, mcap, price, top_pct, risk_label, sev, triggering_sig)
        sent = await tg_send_text(text, chat_id=TELEGRAM_CHAT_ID, buttons=buttons)

        if sent:
            db_record_alert(mint, sev)
            log.info("alert sent %s sev=%s", mint, sev)
            payload = {"mint": mint, "mcap": mcap, "price": price, "liquidity": liquidity, "severity": sev, "top_pct": top_pct, "created": created_iso}
            asyncio.create_task(forward_webhook(payload))
        else:
            log.warning("failed to send alert for %s", mint)

    except Exception as e:
        log.exception("analyze_and_alert error: %s", e)


# ---- Websocket processing --------------------------------------------------
async def _process_logs_result(result: Dict[str, Any]) -> None:
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
            log.info("candidate mint detected %s sig=%s", t, signature)
            asyncio.create_task(analyze_and_alert(t, signature))
    except Exception as e:
        log.exception("_process_logs_result error: %s", e)


async def websocket_loop() -> None:
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
                    asyncio.create_task(_process_logs_result(result))
        except websockets.exceptions.InvalidStatusCode as e:
            log.error("InvalidStatusCode: %s", e)
        except Exception as e:
            log.exception("websocket_loop error: %s", e)
        sleep = min(backoff, MAX_WS_BACKOFF) + random.random()
        log.info("Reconnect in %.2fs", sleep)
        await asyncio.sleep(sleep)
        backoff = min(backoff * 2, MAX_WS_BACKOFF)


# ---- Periodic rechecker (resend if severity increases) --------------------
async def periodic_rechecker() -> None:
    while True:
        try:
            conn = sqlite3.connect(DB_FILE, timeout=30)
            cur = conn.cursor()
            cutoff = int(time.time()) - 3600
            cur.execute("SELECT mint,severity,ts FROM alerts WHERE ts >= ? ORDER BY ts DESC", (cutoff,))
            rows = cur.fetchall()
            conn.close()
            for mint, old_sev, ts in rows:
                created_ts = await infer_creation_ts(mint)
                age = int(time.time() - created_ts) if created_ts else None
                ds = await dexscreener_for_mint(mint)
                mcap = None
                liquidity = None
                if ds and "pairs" in ds and len(ds["pairs"]) > 0:
                    for p in ds["pairs"]:
                        pricep = p.get("priceUsd") or p.get("price")
                        fdv = p.get("fdv")
                        liq = p.get("liquidityUsd") or p.get("liquidity")
                        if pricep:
                            try:
                                if fdv:
                                    mcap = float(fdv)
                                if liq:
                                    liquidity = float(liq)
                            except Exception:
                                pass
                            break
                top_pct = await compute_top_holder_pct(mint)
                new_sev = severity_score_v2(mcap, liquidity, top_pct, age)
                if new_sev - old_sev >= RESEND_SEVERITY_INCREASE:
                    log.info("resend triggered for %s old=%s new=%s", mint, old_sev, new_sev)
                    asyncio.create_task(analyze_and_alert(mint, None))
            await asyncio.sleep(RECHECK_INTERVAL_SECONDS)
        except Exception as e:
            log.exception("periodic_rechecker error: %s", e)
            await asyncio.sleep(RECHECK_INTERVAL_SECONDS)


# ---- Telegram admin handlers -----------------------------------------------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_chat.send_message("CDEXSCOPE bot active.")


async def mute_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.effective_chat.send_message("Usage: /mute <mint>")
        return
    if str(update.effective_chat.id) != str(TELEGRAM_ADMIN_CHAT):
        await update.effective_chat.send_message("Unauthorized.")
        return
    mint = context.args[0].strip()
    db_mute(mint)
    await update.effective_chat.send_message(f"Muted {mint}")


async def unmute_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.effective_chat.send_message("Usage: /unmute <mint>")
        return
    if str(update.effective_chat.id) != str(TELEGRAM_ADMIN_CHAT):
        await update.effective_chat.send_message("Unauthorized.")
        return
    mint = context.args[0].strip()
    db_unmute(mint)
    await update.effective_chat.send_message(f"Unmuted {mint}")


async def setmincap_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.effective_chat.send_message("Usage: /setmincap <usd>")
        return
    if str(update.effective_chat.id) != str(TELEGRAM_ADMIN_CHAT):
        await update.effective_chat.send_message("Unauthorized.")
        return
    try:
        v = float(context.args[0])
        db_set_meta("min_marketcap", str(v))
        global MIN_MARKET_CAP_USD
        MIN_MARKET_CAP_USD = v
        await update.effective_chat.send_message(f"MIN_MARKET_CAP_USD set to ${v:,.0f}")
    except Exception:
        await update.effective_chat.send_message("Invalid value")


async def rescan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.effective_chat.send_message("Usage: /rescan <mint>")
        return
    if str(update.effective_chat.id) != str(TELEGRAM_ADMIN_CHAT):
        await update.effective_chat.send_message("Unauthorized.")
        return
    mint = context.args[0].strip()
    asyncio.create_task(analyze_and_alert(mint, None))
    await update.effective_chat.send_message(f"Rescan queued for {mint}")


async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(TELEGRAM_ADMIN_CHAT):
        await update.effective_chat.send_message("Unauthorized.")
        return
    conn = sqlite3.connect(DB_FILE, timeout=30)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM seen_mints")
    seen = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM alerts WHERE ts > ?", (int(time.time()) - 3600,))
    recent_alerts = cur.fetchone()[0]
    conn.close()
    await update.effective_chat.send_message(f"Stats: seen tokens={seen}, alerts_last_hour={recent_alerts}")


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_chat.send_message(f"CDEXSCOPE status\nmin_marketcap={MIN_MARKET_CAP_USD}\nalerts_this_minute={_alert_counter['count']}")


async def restart_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(TELEGRAM_ADMIN_CHAT):
        await update.effective_chat.send_message("Unauthorized.")
        return
    await update.effective_chat.send_message("Restarting (admin requested)...")
    os._exit(0)


# ---- Minimal preview/health HTTP endpoint (asyncio) ------------------------
async def _handle_tcp_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        await reader.read(1024)
        alerts = db_get_recent_alerts(50)
        rows = "".join(
            f"<tr><td>{html.escape(r['mint'])}</td><td>{r['severity']:.0f}</td><td>{datetime.fromtimestamp(r['ts'], timezone.utc).isoformat()}</td></tr>"
            for r in alerts
        )
        body = f"""
        <html><head><meta charset="utf-8"><title>CDEXSCOPE Preview</title></head>
        <body>
        <h1>CDEXSCOPE - Recent Alerts</h1>
        <table border="1" cellpadding="6"><thead><tr><th>Mint</th><th>Severity</th><th>Time</th></tr></thead><tbody>{rows}</tbody></table>
        <p>Instance healthy.</p>
        </body></html>
        """
        resp = (
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/html; charset=utf-8\r\n"
            b"Content-Length: " + str(len(body.encode("utf-8"))).encode() + b"\r\n"
            b"Connection: close\r\n"
            b"\r\n" + body.encode("utf-8")
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


async def start_preview_server():
    port = int(os.getenv("PORT", os.getenv("RENDER_PORT", "10000")))
    server = await asyncio.start_server(_handle_tcp_client, host="0.0.0.0", port=port)
    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    log.info("Preview/health server listening on %s", addrs)
    async with server:
        await server.serve_forever()


# ---- Entrypoint ------------------------------------------------------------
async def main():
    global TG_APP
    log.info("Starting %s (single-file) bot", PROJECT)
    init_db()

    mm = db_get_meta("min_marketcap")
    if mm:
        try:
            global MIN_MARKET_CAP_USD
            MIN_MARKET_CAP_USD = float(mm)
        except Exception:
            pass

    # build PTB Application
    if TELEGRAM_TOKEN:
        app = ApplicationBuilder().token(TELEGRAM_TOKEN).concurrent_updates(True).build()
        TG_APP = app
        app.add_handler(CommandHandler("start", start_cmd))
        app.add_handler(CommandHandler("mute", mute_cmd))
        app.add_handler(CommandHandler("unmute", unmute_cmd))
        app.add_handler(CommandHandler("setmincap", setmincap_cmd))
        app.add_handler(CommandHandler("rescan", rescan_cmd))
        app.add_handler(CommandHandler("stats", stats_cmd))
        app.add_handler(CommandHandler("status", status_cmd))
        app.add_handler(CommandHandler("restart", restart_cmd))

        await app.initialize()
        await app.start()
        log.info("Telegram application started")
    else:
        log.warning("TELEGRAM_TOKEN not set; Telegram functionality disabled")
        TG_APP = None

    try:
        if TG_APP:
            await tg_send_text(f"{PROJECT} started at {datetime.now(timezone.utc).isoformat()}")
    except Exception:
        log.exception("startup heartbeat failed")

    # background tasks
    tasks = [
        asyncio.create_task(websocket_loop()),
        asyncio.create_task(periodic_rechecker()),
        asyncio.create_task(start_preview_server()),
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutdown requested")
