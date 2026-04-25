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
    """OI 30초 폴링"""
    async with aiohttp.ClientSession() as session:
        while True:
            for symbol in list(symbols_ref):
                try:
                    url = f"{BINANCE['rest_oi']}?symbol={symbol}"
                    async with session.get(url) as resp:
                        data = await resp.json()
                        logger.warning(f"[Binance] OI 응답내용: {data}")
                    oi = float(data["openInterest"])
                    state.update_oi(EXCHANGE, symbol, oi)
                except Exception as e:
                    logger.warning(f"[Binance] OI 실패 {symbol}: {e}")
                await asyncio.sleep(0.1)
            await asyncio.sleep(OI_POLL_SEC)


async def trades_ws_chunk(symbols: list, chunk_id: int):
    """심볼 청크 단위 WS 연결"""
    streams = "/".join([f"{s.lower()}@aggTrade" for s in symbols])
    url = f"{BINANCE['ws']}{streams}"
    logger.info(f"[Binance] WS청크{chunk_id} 연결 중... ({len(symbols)}개)")

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                close_timeout=10,
                max_size=10 * 1024 * 1024,
            ) as ws:
                logger.info(f"[Binance] WS청크{chunk_id} 연결 완료")
                msg_count = 0
                async for raw in ws:
                    msg_count += 1
                    if msg_count <= 3:
                        logger.info(f"[Binance] 청크{chunk_id} RAW: {str(raw)[:150]}")
                    try:
                        msg  = json.loads(raw)
                        data = msg.get("data", msg)
                        symbol = data["s"]
                        price  = float(data["p"])
                        qty    = float(data["q"])
                        is_buy = not data["m"]
                        state.update_trade(EXCHANGE, symbol, price, qty, is_buy)
                    except Exception as e:
                        logger.error(f"[Binance] 청크{chunk_id} 파싱 오류: {e}")

        except Exception as e:
            logger.error(f"[Binance] WS청크{chunk_id} 끊김: {e} — 5초 후 재연결")
            await asyncio.sleep(5)


async def trades_ws(symbols_ref: list):
    """심볼을 50개씩 나눠서 병렬 WS 연결"""
    chunk_size = 50
    chunks = [
        symbols_ref[i:i+chunk_size]
        for i in range(0, len(symbols_ref), chunk_size)
    ]
    logger.info(f"[Binance] 총 {len(chunks)}개 청크로 WS 연결")
    await asyncio.gather(*[
        trades_ws_chunk(chunk, i)
        for i, chunk in enumerate(chunks)
    ])


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
