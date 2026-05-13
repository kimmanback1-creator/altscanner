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

    # Binance가 에러 시 dict로 응답하는 경우 방어
    if not isinstance(data, list):
        logger.error(f"[Binance] fetch_top_symbols: 예상치 못한 응답 형태 {type(data).__name__}: {str(data)[:200]}")
        # 빈 리스트 반환하면 main에서 죽으므로, 5초 대기 후 한 번 재시도
        await asyncio.sleep(5)
        async with aiohttp.ClientSession() as session:
            async with session.get(BINANCE["rest_top"]) as resp:
                data = await resp.json()
        if not isinstance(data, list):
            logger.error(f"[Binance] 재시도도 실패 — 빈 심볼 리스트로 진행")
            return []

    filtered = [
        t for t in data
        if isinstance(t, dict)
        and t.get("symbol", "").endswith("USDT")
        and float(t.get("quoteVolume", 0)) >= MIN_QUOTE_VOL
    ]
    filtered.sort(key=lambda x: float(x["quoteVolume"]), reverse=True)
    top = filtered[:TOP_N_SYMBOLS]

    # 24h 변화율 같이 저장
    for t in top:
        try:
            chg_24h = float(t.get("priceChangePercent", 0))
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
        logger.info(f"[Binance] 워치리스트 추가 {len(extra)}개: {extra}")

    logger.info(f"[Binance] 심볼 {len(symbols)}개 선정")
    return symbols

async def fetch_24h_only():
    """24h 변화율만 갱신 (심볼 리스트 변경 없음)"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(BINANCE["rest_top"]) as resp:
                data = await resp.json()
        if not isinstance(data, list):
            logger.warning(f"[Binance] 24h 응답 비정상: {str(data)[:200]}")
            return
        for t in data:
            if not isinstance(t, dict):
                continue
            try:
                chg_24h = float(t.get("priceChangePercent", 0))
                state.update_24h_chg(EXCHANGE, t["symbol"], chg_24h)
            except (ValueError, TypeError):
                continue
    except Exception as e:
        logger.error(f"[Binance] 24h 갱신 실패: {e}")

async def oi_poller(symbols_ref: list):
    """OI 30초 폴링"""
    async with aiohttp.ClientSession() as session:
        while True:
            for symbol in list(symbols_ref):
                try:
                    url = f"{BINANCE['rest_oi']}?symbol={symbol}"
                    async with session.get(url) as resp:
                        data = await resp.json()
                    if "openInterest" not in data:
                        continue
                    oi = float(data["openInterest"])
                    state.update_oi(EXCHANGE, symbol, oi)
                except Exception as e:
                    logger.warning(f"[Binance] OI 실패 {symbol}: {e}")
                await asyncio.sleep(0.1)
            await asyncio.sleep(OI_POLL_SEC)


async def trades_ws_chunk(symbols: list, chunk_id: int):
    """심볼 청크 단위 WS 연결 + stale timeout + 진단 로그"""
    import time as _time
    streams = "/".join([f"{s.lower()}@aggTrade" for s in symbols])
    url = f"{BINANCE['ws']}{streams}"
    logger.info(f"[Binance] WS청크{chunk_id} 연결 중... ({len(symbols)}개) URL앞80자={url[:80]}")
    logger.info(f"[Binance] 청크{chunk_id} 심볼샘플: {symbols[:3]}")

    STALE_TIMEOUT = 60  # 60초 무응답이면 강제 재연결

    while True:
        connect_start = _time.time()
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                close_timeout=10,
                max_size=10 * 1024 * 1024,
            ) as ws:
                connect_elapsed = _time.time() - connect_start
                logger.info(f"[Binance] WS청크{chunk_id} 연결 완료 ({connect_elapsed:.2f}초)")
                msg_count = 0
                last_msg_time = _time.time()
                heartbeat_count = 0  # 핸드셰이크/keepalive 메시지
                trade_count = 0      # 실제 trade 메시지
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=STALE_TIMEOUT)
                    except asyncio.TimeoutError:
                        elapsed = _time.time() - last_msg_time
                        logger.warning(f"[Binance] WS청크{chunk_id} {STALE_TIMEOUT}초 무응답 (총 msg={msg_count}, trade={trade_count}, 마지막 메시지 {elapsed:.0f}초 전) — 강제 재연결")
                        await ws.close()
                        break
                    
                    last_msg_time = _time.time()
                    msg_count += 1
                    if msg_count <= 5:
                        logger.info(f"[Binance] 청크{chunk_id} RAW #{msg_count} ({len(raw)}B): {str(raw)[:200]}")
                    
                    try:
                        msg  = json.loads(raw)
                        data = msg.get("data", msg)
                        if "s" not in data:
                            heartbeat_count += 1
                            if heartbeat_count <= 3:
                                logger.info(f"[Binance] 청크{chunk_id} non-trade msg: {str(msg)[:150]}")
                            continue
                        symbol = data["s"]
                        price  = float(data["p"])
                        qty    = float(data["q"])
                        is_buy = not data["m"]
                        state.update_trade(EXCHANGE, symbol, price, qty, is_buy)
                        trade_count += 1
                        if trade_count == 1:
                            logger.info(f"[Binance] ✅ 청크{chunk_id} 첫 trade 수신: {symbol} {price} {qty}")
                    except Exception as e:
                        logger.error(f"[Binance] 청크{chunk_id} 파싱 오류: {e} raw={str(raw)[:200]}")

        except Exception as e:
            logger.error(f"[Binance] WS청크{chunk_id} 끊김: {type(e).__name__}: {e} — 5초 후 재연결")
            await asyncio.sleep(5)


async def trades_ws(symbols_ref: list):
    """심볼을 10개씩 나눠서 병렬 WS 연결 (Binance throttle 회피용)"""
    chunk_size = 10
    chunks = [
        symbols_ref[i:i+chunk_size]
        for i in range(0, len(symbols_ref), chunk_size)
    ]
    logger.info(f"[Binance] 총 {len(chunks)}개 청크로 WS 연결 (chunk_size={chunk_size})")
    await asyncio.gather(*[
        trades_ws_chunk(chunk, i)
        for i, chunk in enumerate(chunks)
    ])

async def liquidations_ws_chunk(symbols: list, chunk_id: int):
    """청산 WS - 심볼 청크 단위"""
    streams = "/".join([f"{s.lower()}@forceOrder" for s in symbols])
    url = f"{BINANCE['ws']}{streams}"
    logger.info(f"[Binance] LiqWS청크{chunk_id} 연결 중... ({len(symbols)}개)")

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                close_timeout=10,
                max_size=10 * 1024 * 1024,
            ) as ws:
                logger.info(f"[Binance] LiqWS청크{chunk_id} 연결 완료")
                async for raw in ws:
                    try:
                        msg  = json.loads(raw)
                        data = msg.get("data", msg)
                        order = data.get("o", {})
                        if not order:
                            continue
                        symbol = order["s"]
                        # Binance: S='SELL'=롱 청산, S='BUY'=숏 청산
                        side = "LONG" if order["S"] == "SELL" else "SHORT"
                        qty   = float(order["q"])
                        price = float(order.get("ap", order.get("p", 0)))  # 평균체결가 우선
                        if price <= 0:
                            continue
                        await state.insert_liquidation(EXCHANGE, symbol, side, qty, price)
                    except Exception as e:
                        logger.warning(f"[Binance] LiqWS청크{chunk_id} 파싱 오류: {e}")

        except Exception as e:
            logger.error(f"[Binance] LiqWS청크{chunk_id} 끊김: {e} — 5초 후 재연결")
            await asyncio.sleep(5)


async def liquidations_ws(symbols_ref: list):
    """심볼을 50개씩 나눠서 병렬 청산 WS 연결"""
    chunk_size = 50
    chunks = [
        symbols_ref[i:i+chunk_size]
        for i in range(0, len(symbols_ref), chunk_size)
    ]
    logger.info(f"[Binance] 총 {len(chunks)}개 청크로 청산 WS 연결")
    await asyncio.gather(*[
        liquidations_ws_chunk(chunk, i)
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
        liquidations_ws(symbols_ref),
        symbol_refresher(symbols_ref),
    )
