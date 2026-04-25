# ══════════════════════════════════════════
#  exchanges/binance.py
#  Binance Futures aggTrades WS + OI REST
# ══════════════════════════════════════════

import asyncio
import aiohttp
import json
import logging
import websockets
from config import BINANCE, TOP_N_SYMBOLS, MIN_QUOTE_VOL, OI_POLL_SEC, SYMBOL_REFRESH_MIN
import core.state as state

logger = logging.getLogger(__name__)
EXCHANGE = "binance"


async def fetch_top_symbols() -> list[str]:
    async with aiohttp.ClientSession() as session:
        async with session.get(BINANCE["rest_top"]) as resp:
            data = await resp.json()
    filtered = [
        t for t in data
        if t["symbol"].endswith("USDT")
        and float(t.get("quoteVolume", 0)) >= MIN_QUOTE_VOL
    ]
    filtered.sort(key=lambda x: float(x["quoteVolume"]), reverse=True)
    symbols = [t["symbol"] for t in filtered[:TOP_N_SYMBOLS]]
    logger.info(f"[Binance] 심볼 {len(symbols)}개 선정")
    return symbols


async def oi_poller(symbols_ref: list):
    """OI 30초 폴링 - symbols_ref는 공유 리스트"""
    async with aiohttp.ClientSession() as session:
        while True:
            for symbol in list(symbols_ref):
                try:
                    url = f"{BINANCE['rest_oi']}?symbol={symbol}"
                    async with session.get(url) as resp:
                        data = await resp.json()
                    oi = float(data["openInterest"])
                    state.update_oi(EXCHANGE, symbol, oi)
                except Exception as e:
                    logger.warning(f"[Binance] OI 실패 {symbol}: {e}")
                await asyncio.sleep(0.05)
            await asyncio.sleep(OI_POLL_SEC)


async def trades_ws(symbols_ref: list):
    """aggTrades WebSocket - 24시간마다 자동 재연결"""
    while True:
        try:
            streams = "/".join([f"{s.lower()}@aggTrade" for s in symbols_ref])
            url = f"{BINANCE['ws']}{streams}"
            logger.info(f"[Binance] WS 연결 중... ({len(symbols_ref)}개)")

            async with websockets.connect(
                url,
                ping_interval=20,
                close_timeout=10
            ) as ws:
                logger.info("[Binance] WS 연결 완료")
                async for raw in ws:
                    try:
                        msg  = json.loads(raw)
                        data = msg.get("data", msg)
                        symbol = data["s"]
                        price  = float(data["p"])
                        qty    = float(data["q"])
                        is_buy = not data["m"]
                        logger.info(f"[Binance] 틱: {symbol} {price}")
                        state.update_trade(EXCHANGE, symbol, price, qty, is_buy)
                    except Exception as e:
                        logger.warning(f"[Binance] 파싱 오류: {e}")

        except Exception as e:
            logger.error(f"[Binance] WS 끊김: {e} — 5초 후 재연결")
            await asyncio.sleep(5)


async def symbol_refresher(symbols_ref: list):
    """1시간마다 심볼 리스트 갱신"""
    while True:
        await asyncio.sleep(SYMBOL_REFRESH_MIN * 60)
        try:
            new_symbols = await fetch_top_symbols()
            symbols_ref.clear()
            symbols_ref.extend(new_symbols)
            logger.info(f"[Binance] 심볼 갱신 완료: {len(new_symbols)}개")
        except Exception as e:
            logger.error(f"[Binance] 심볼 갱신 실패: {e}")


async def run():
    symbols = await fetch_top_symbols()
    symbols_ref = list(symbols)
    await asyncio.gather(
        trades_ws(symbols_ref),
        oi_poller(symbols_ref),
        symbol_refresher(symbols_ref),
    )
