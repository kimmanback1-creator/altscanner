# ══════════════════════════════════════════
#  exchanges/bybit.py
#  Bybit Linear Futures trades WS + OI REST
# ══════════════════════════════════════════

import asyncio
import aiohttp
import json
import logging
import websockets
from config import BYBIT, TOP_N_SYMBOLS, MIN_QUOTE_VOL, OI_POLL_SEC, SYMBOL_REFRESH_MIN
import core.state as state

logger = logging.getLogger(__name__)
EXCHANGE = "bybit"


async def fetch_top_symbols() -> list[str]:
    async with aiohttp.ClientSession() as session:
        async with session.get(BYBIT["rest_top"]) as resp:
            data = await resp.json()
    tickers = data.get("result", {}).get("list", [])
    filtered = [
        t for t in tickers
        if t["symbol"].endswith("USDT")
        and float(t.get("turnover24h", 0)) >= MIN_QUOTE_VOL
    ]
    filtered.sort(key=lambda x: float(x.get("turnover24h", 0)), reverse=True)
    top = filtered[:TOP_N_SYMBOLS]

    # 24h 변화율 같이 저장 (Bybit는 소수 → ×100)
    for t in top:
        try:
            chg_24h = float(t.get("price24hPcnt", 0)) * 100
            state.update_24h_chg(EXCHANGE, t["symbol"], chg_24h)
        except (ValueError, TypeError):
            pass

    symbols = [t["symbol"] for t in top]
    logger.info(f"[Bybit] 심볼 {len(symbols)}개 선정")
    return symbols

async def fetch_24h_only():
    """24h 변화율만 갱신 (심볼 리스트 변경 없음)"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(BYBIT["rest_top"]) as resp:
                data = await resp.json()
        for t in data.get("result", {}).get("list", []):
            try:
                chg_24h = float(t.get("price24hPcnt", 0)) * 100
                state.update_24h_chg(EXCHANGE, t["symbol"], chg_24h)
            except (ValueError, TypeError):
                continue
    except Exception as e:
        logger.error(f"[Bybit] 24h 갱신 실패: {e}")

async def oi_poller(symbols_ref: list):
    async with aiohttp.ClientSession() as session:
        while True:
            for symbol in list(symbols_ref):
                try:
                    url = f"{BYBIT['rest_oi']}?category=linear&symbol={symbol}&intervalTime=5min&limit=1"
                    async with session.get(url) as resp:
                        data = await resp.json()
                    oi = float(data["result"]["list"][0]["openInterest"])
                    state.update_oi(EXCHANGE, symbol, oi)
                except Exception as e:
                    logger.warning(f"[Bybit] OI 실패 {symbol}: {e}")
                await asyncio.sleep(0.05)
            await asyncio.sleep(OI_POLL_SEC)


async def trades_ws(symbols_ref: list):
    while True:
        try:
            args = [f"publicTrade.{s}" for s in symbols_ref]
            logger.info(f"[Bybit] WS 연결 중... ({len(symbols_ref)}개)")

            async with websockets.connect(BYBIT["ws"], ping_interval=20) as ws:
                await ws.send(json.dumps({
                    "op": "subscribe",
                    "args": args
                }))
                logger.info("[Bybit] WS 구독 완료")

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        if msg.get("op") in ("pong", "subscribe"):
                            continue
                        topic = msg.get("topic", "")
                        if not topic.startswith("publicTrade"):
                            continue
                        symbol = topic.split(".")[-1]
                        for t in msg.get("data", []):
                            price  = float(t["p"])
                            qty    = float(t["v"])
                            is_buy = t["S"] == "Buy"
                            state.update_trade(EXCHANGE, symbol, price, qty, is_buy)
                    except Exception as e:
                        logger.warning(f"[Bybit] 파싱 오류: {e}")

        except Exception as e:
            logger.error(f"[Bybit] WS 끊김: {e} — 5초 후 재연결")
            await asyncio.sleep(5)


async def symbol_refresher(symbols_ref: list):
    while True:
        await asyncio.sleep(SYMBOL_REFRESH_MIN * 60)
        try:
            new_symbols = await fetch_top_symbols()
            symbols_ref.clear()
            symbols_ref.extend(new_symbols)
            logger.info(f"[Bybit] 심볼 갱신 완료: {len(new_symbols)}개")
        except Exception as e:
            logger.error(f"[Bybit] 심볼 갱신 실패: {e}")


async def run():
    symbols = await fetch_top_symbols()
    symbols_ref = list(symbols)
    await asyncio.gather(
        trades_ws(symbols_ref),
        oi_poller(symbols_ref),
        symbol_refresher(symbols_ref),
    )
