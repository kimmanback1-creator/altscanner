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

    # 워치리스트 강제 포함 (TOP N 밖이어도)
    from db.supabase import fetch_watchlist
    watchlist = fetch_watchlist(EXCHANGE)
    extra = [s for s in watchlist if s not in symbols]
    if extra:
        symbols.extend(extra)
        logger.info(f"[Bybit] 워치리스트 추가 {len(extra)}개: {extra}")

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


async def trades_ws(symbols_ref: list, ws_holder: dict):
    while True:
        try:
            args = [f"publicTrade.{s}" for s in symbols_ref]
            logger.info(f"[Bybit] WS 연결 중... ({len(symbols_ref)}개)")

            async with websockets.connect(BYBIT["ws"], ping_interval=20) as ws:
                ws_holder["trades"] = ws  # refresher가 종료 가능하도록 보관
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
            ws_holder["trades"] = None
            await asyncio.sleep(5)

async def liquidations_ws(symbols_ref: list):
    """청산 WS - Bybit은 심볼별 구독"""
    while True:
        try:
            args = [f"liquidation.{s}" for s in symbols_ref]
            logger.info(f"[Bybit] 청산 WS 연결 중... ({len(args)}개)")

            async with websockets.connect(BYBIT["ws"], ping_interval=20) as ws:
                # 한 번에 너무 많이 구독하면 거부될 수 있으므로 청크로 나눠서 구독
                chunk_size = 50
                for i in range(0, len(args), chunk_size):
                    await ws.send(json.dumps({
                        "op": "subscribe",
                        "args": args[i:i+chunk_size]
                    }))
                    await asyncio.sleep(0.3)

                logger.info("[Bybit] 청산 WS 구독 완료")

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        if msg.get("op") in ("pong", "subscribe"):
                            continue
                        topic = msg.get("topic", "")
                        if not topic.startswith("liquidation"):
                            continue
                        # Bybit liquidation 메시지: data가 dict 또는 list
                        data = msg.get("data", {})
                        entries = data if isinstance(data, list) else [data]
                        for d in entries:
                            symbol = d.get("symbol", topic.split(".")[-1])
                            # Bybit: side='Buy'=숏청산, side='Sell'=롱청산
                            bybit_side = d.get("side", "")
                            if bybit_side == "Sell":
                                side = "LONG"  # 롱 포지션이 청산됨
                            elif bybit_side == "Buy":
                                side = "SHORT"  # 숏 포지션이 청산됨
                            else:
                                continue
                            
                            qty   = float(d.get("size", 0))
                            price = float(d.get("price", 0))
                            if qty <= 0 or price <= 0:
                                continue
                            await state.insert_liquidation(EXCHANGE, symbol, side, qty, price)
                    except Exception as e:
                        logger.warning(f"[Bybit] 청산 파싱 오류: {e}")

        except Exception as e:
            logger.error(f"[Bybit] 청산 WS 끊김: {e} — 5초 후 재연결")
            await asyncio.sleep(5)

async def symbol_refresher(symbols_ref: list, ws_holder: dict):
    while True:
        await asyncio.sleep(SYMBOL_REFRESH_MIN * 60)
        try:
            new_symbols = await fetch_top_symbols()
            old_set = set(symbols_ref)
            new_set = set(new_symbols)
            symbols_ref.clear()
            symbols_ref.extend(new_symbols)
            if old_set != new_set:
                added = new_set - old_set
                removed = old_set - new_set
                logger.info(f"[Bybit] 심볼 변화 — 추가:{len(added)} 제거:{len(removed)} → WS 재연결 트리거")
                if ws_holder.get("trades"):
                    await ws_holder["trades"].close()
            else:
                logger.info(f"[Bybit] 심볼 갱신 완료: {len(new_symbols)}개 (변화 없음)")
        except Exception as e:
            logger.error(f"[Bybit] 심볼 갱신 실패: {e}")


async def run():
    symbols = await fetch_top_symbols()
    symbols_ref = list(symbols)
    ws_holder = {"trades": None}  # WS 객체 공유 (refresher가 강제 종료용)
    await asyncio.gather(
        trades_ws(symbols_ref, ws_holder),
        oi_poller(symbols_ref),
        liquidations_ws(symbols_ref),
        symbol_refresher(symbols_ref, ws_holder),
    )
