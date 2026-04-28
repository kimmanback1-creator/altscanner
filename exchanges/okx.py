# ══════════════════════════════════════════
#  exchanges/okx.py
#  OKX Futures trades WS + OI REST
# ══════════════════════════════════════════

import asyncio
import aiohttp
import json
import logging
import websockets
from config import OKX, TOP_N_SYMBOLS, MIN_QUOTE_VOL, OI_POLL_SEC, SYMBOL_REFRESH_MIN
import core.state as state

logger = logging.getLogger(__name__)
EXCHANGE = "okx"


async def fetch_top_symbols() -> list[str]:
    async with aiohttp.ClientSession() as session:
        async with session.get(OKX["rest_top"]) as resp:
            data = await resp.json()
    tickers = data.get("data", [])
    filtered = [
        t for t in tickers
        if t["instId"].endswith("USDT-SWAP")
        and float(t.get("volCcy24h", 0)) * float(t.get("last", 1)) >= MIN_QUOTE_VOL
    ]
    filtered.sort(key=lambda x: float(x.get("volCcy24h", 0)), reverse=True)
    top = filtered[:TOP_N_SYMBOLS]

    # 24h 변화율 같이 저장 (OKX는 last/open24h로 계산)
    for t in top:
        try:
            last    = float(t.get("last", 0))
            open24h = float(t.get("open24h", 0))
            if open24h > 0:
                chg_24h = (last - open24h) / open24h * 100
                state.update_24h_chg(EXCHANGE, t["instId"], chg_24h)
        except (ValueError, TypeError):
            pass

    symbols = [t["instId"] for t in top]
    logger.info(f"[OKX] 심볼 {len(symbols)}개 선정")
    return symbols

async def fetch_24h_only():
    """24h 변화율만 갱신 (심볼 리스트 변경 없음)"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(OKX["rest_top"]) as resp:
                data = await resp.json()
        for t in data.get("data", []):
            try:
                last    = float(t.get("last", 0))
                open24h = float(t.get("open24h", 0))
                if open24h > 0:
                    chg_24h = (last - open24h) / open24h * 100
                    state.update_24h_chg(EXCHANGE, t["instId"], chg_24h)
            except (ValueError, TypeError):
                continue
    except Exception as e:
        logger.error(f"[OKX] 24h 갱신 실패: {e}")

async def oi_poller(symbols_ref: list):
    async with aiohttp.ClientSession() as session:
        while True:
            for symbol in list(symbols_ref):
                try:
                    url = f"{OKX['rest_oi']}?instId={symbol}"
                    async with session.get(url) as resp:
                        data = await resp.json()
                    oi = float(data["data"][0]["oi"])
                    state.update_oi(EXCHANGE, symbol, oi)
                except Exception as e:
                    logger.warning(f"[OKX] OI 실패 {symbol}: {e}")
                await asyncio.sleep(0.05)
            await asyncio.sleep(OI_POLL_SEC)


async def trades_ws(symbols_ref: list):
    while True:
        try:
            args = [{"channel": "trades", "instId": s} for s in symbols_ref]
            logger.info(f"[OKX] WS 연결 중... ({len(symbols_ref)}개)")

            async with websockets.connect(OKX["ws"], ping_interval=20) as ws:
                # 3개씩 나눠서 구독 (초당 3개 제한)
                for i in range(0, len(args), 3):
                    await ws.send(json.dumps({
                        "op": "subscribe",
                        "args": args[i:i+3]
                    }))
                    await asyncio.sleep(1.1)

                logger.info("[OKX] WS 구독 완료")

                async for raw in ws:
                    try:
                        if raw == "pong":
                            continue
                        msg = json.loads(raw)
                        if msg.get("event") == "subscribe":
                            continue
                        for t in msg.get("data", []):
                            symbol = t["instId"]
                            price  = float(t["px"])
                            qty    = float(t["sz"])
                            is_buy = t["side"] == "buy"
                            state.update_trade(EXCHANGE, symbol, price, qty, is_buy)
                    except Exception as e:
                        logger.warning(f"[OKX] 파싱 오류: {e}")

        except Exception as e:
            logger.error(f"[OKX] WS 끊김: {e} — 5초 후 재연결")
            await asyncio.sleep(5)


async def symbol_refresher(symbols_ref: list):
    while True:
        await asyncio.sleep(SYMBOL_REFRESH_MIN * 60)
        try:
            new_symbols = await fetch_top_symbols()
            symbols_ref.clear()
            symbols_ref.extend(new_symbols)
            logger.info(f"[OKX] 심볼 갱신 완료: {len(new_symbols)}개")
        except Exception as e:
            logger.error(f"[OKX] 심볼 갱신 실패: {e}")


async def run():
    symbols = await fetch_top_symbols()
    symbols_ref = list(symbols)
    await asyncio.gather(
        trades_ws(symbols_ref),
        oi_poller(symbols_ref),
        symbol_refresher(symbols_ref),
    )
