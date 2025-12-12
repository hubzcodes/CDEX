"""
CDEXSCOPE - bot.py (updated to avoid aiohttp compilation issues)
Uses: httpx (async HTTP), websockets (async WS)

Note: this version removes aiohttp to avoid build failures on Render.
"""

import os
import asyncio
import json
import time
import logging
from typing import Dict, Any, Optional
import httpx
import websockets
from datetime import datetime, timezone

PROJECT_NAME = "CDEXSCOPE"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
SOLANA_WS_URL = os.getenv("SOLANA_WS_URL", "wss://api.mainnet-beta.solana.com")
MIN_MARKET_CAP_USD = float(os.getenv("MIN_MARKET_CAP_USD", "50000"))
MAX_TOKEN_AGE_SECONDS = int(os.getenv("MAX_TOKEN_AGE_SECONDS", str(2*3600)))
ALERT_DEDUPE_SECONDS = int(os.getenv("ALERT_DEDUPE_SECONDS", "600"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
                    format="%(asctime)s %(levelname)s %(message)s")

_http_client: Optional[httpx.AsyncClient] = None

def get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None:
        _http_client = httpx.AsyncClient(timeout=20.0)
    return _http_client

async def telegram_send(text: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("Telegram not configured; CDEXSCOPE would send: %s", text[:200])
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
    client = get_http_client()
    try:
        r = await client.post(url, json=payload)
        if r.status_code != 200:
            logging.warning("Telegram send failed %s: %s", r.status_code, r.text)
    except Exception as e:
        logging.exception("telegram_send error: %s", e)

# Minimal RPC helper using httpx
_rpc_id = 1
_rpc_lock = asyncio.Lock()

async def solana_rpc(method: str, params=None, timeout=20) -> Any:
    global _rpc_id
    if params is None:
        params = []
    async with _rpc_lock:
        body = {"jsonrpc":"2.0", "id": _rpc_id, "method": method, "params": params}
        _rpc_id += 1
    client = get_http_client()
    for _ in range(3):
        try:
            r = await client.post(SOLANA_RPC_URL, json=body, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            return data.get("result", data)
        except Exception as e:
            logging.warning("RPC call %s failed: %s; retrying", method, e)
            await asyncio.sleep(1)
    raise RuntimeError(f"RPC call {method} failed after retries")

# Helpers
async def get_token_mint_creation_slot(mint_address: str) -> Optional[int]:
    try:
        res = await solana_rpc("getSignaturesForAddress", [mint_address, {"limit": 20}])
        if not res:
            return None
        last = res[-1]
        return last.get("slot")
    except Exception as e:
        logging.debug("get_token_mint_creation_slot error: %s", e)
        return None

async def get_block_time(slot: int) -> Optional[int]:
    try:
        res = await solana_rpc("getBlockTime", [slot])
        return res
    except Exception as e:
        logging.debug("get_block_time error: %s", e)
        return None

async def estimate_marketcap_usd(mint_address: str) -> Optional[float]:
    # Placeholder for better pool parsing. Returning None keeps logic conservative.
    try:
        supply_res = await solana_rpc("getTokenSupply", [mint_address])
        if not supply_res:
            return None
        return None
    except Exception as e:
        logging.debug("estimate_marketcap_usd error: %s", e)
        return None

async def check_dev_risk(mint_address: str) -> Dict[str, Any]:
    result = {"top_holder_percent": None, "top_holder": None, "flags": []}
    try:
        largest = await solana_rpc("getTokenLargestAccounts", [mint_address])
        if not largest:
            return result
        accounts = largest.get("value", [])
        if accounts:
            top = accounts[0]
            amount = float(top.get("amount", 0))
            supply_res = await solana_rpc("getTokenSupply", [mint_address])
            supply_val = float(supply_res.get("value", {}).get("amount", 1))
            top_percent = amount / supply_val * 100 if supply_val else None
            result["top_holder_percent"] = top_percent
            result["top_holder"] = top.get("address")
            if top_percent and top_percent > 40:
                result["flags"].append(f"Top holder {top_percent:.1f}%")
        return result
    except Exception as e:
        logging.debug("check_dev_risk error: %s", e)
        return result

alert_dedupe: Dict[str, float] = {}
seen_mints = set()
ALERT_DEDUPE_SECONDS = int(os.getenv("ALERT_DEDUPE_SECONDS", "600"))

def should_alert(mint_address: str) -> bool:
    now = time.time()
    last = alert_dedupe.get(mint_address)
    if last and now - last < ALERT_DEDUPE_SECONDS:
        return False
    alert_dedupe[mint_address] = now
    return True

async def alert_token(mint_address: str, info: Dict[str,Any]):
    if not should_alert(mint_address):
        logging.info("Skipping alert dedupe for %s", mint_address)
        return
    created_at = info.get("created_at_iso", "unknown")
    marketcap = info.get("marketcap_usd")
    dev_info = info.get("dev_info", {})
    lines = [f"*{PROJECT_NAME} Alert*", f"`{mint_address}`", f"Created: {created_at}"]
    if marketcap:
        lines.append(f"Estimated marketcap: ${marketcap:,.0f}")
    else:
        lines.append("Marketcap: _unknown_")
    if dev_info.get("top_holder_percent"):
        lines.append(f"Top holder: {dev_info['top_holder']} ({dev_info['top_holder_percent']:.1f}%)")
    if dev_info.get("flags"):
        lines.append("Flags: " + ", ".join(dev_info["flags"]))
    text = "\n".join(lines)
    await telegram_send(text)

async def handle_ws():
    logging.info("Connecting to SOL websocket %s", SOLANA_WS_URL)
    backoff = 1
    while True:
        try:
            async with websockets.connect(SOLANA_WS_URL, ping_interval=30, max_size=None) as ws:
                logging.info("Websocket connected")
                subscribe_msg = {"jsonrpc":"2.0","id":1,"method":"logsSubscribe","params":["all", {"commitment":"confirmed"}]}
                await ws.send(json.dumps(subscribe_msg))
                while True:
                    raw = await ws.recv()
                    msg = json.loads(raw)
                    params = msg.get("params")
                    if not params:
                        continue
                    result = params.get("result", {})
                    logs = result.get("logs", [])
                    joined = " ".join(logs)
                    if "InitializeMint" in joined or "mintTo" in joined or "MintTo" in joined or "create_account" in joined:
                        tokens_found = set()
                        for part in joined.split():
                            if len(part) in (42,43,44) and all(c.isalnum() or c in ['-','_'] for c in part):
                                tokens_found.add(part)
                        for t in tokens_found:
                            if t in seen_mints:
                                continue
                            seen_mints.add(t)
                            logging.info("Candidate token mint seen: %s", t)
                            asyncio.create_task(process_candidate_token(t))
        except Exception as e:
            logging.exception("Websocket connection error: %s", e)
            logging.info("Reconnecting after backoff %s", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

async def process_candidate_token(mint_address: str):
    logging.info("Processing candidate %s", mint_address)
    slot = await get_token_mint_creation_slot(mint_address)
    created_iso = "unknown"
    if slot:
        bt = await get_block_time(slot)
        if bt:
            created_iso = datetime.fromtimestamp(bt, tz=timezone.utc).isoformat()
            age = time.time() - bt
            logging.info("Token %s age seconds: %s", mint_address, age)
            if age > MAX_TOKEN_AGE_SECONDS:
                logging.info("Token %s older than threshold; ignoring", mint_address)
                return
    marketcap = await estimate_marketcap_usd(mint_address)
    if marketcap is not None and marketcap > MIN_MARKET_CAP_USD:
        logging.info("Token %s marketcap $%s above threshold, skipping", mint_address, marketcap)
        return
    dev_info = await check_dev_risk(mint_address)
    info = {
        "mint_address": mint_address,
        "created_at_iso": created_iso,
        "marketcap_usd": marketcap,
        "dev_info": dev_info,
    }
    await alert_token(mint_address, info)

async def main():
    logging.info(f"Starting {PROJECT_NAME} memecoin monitor bot")
    await handle_ws()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Terminated by user")
