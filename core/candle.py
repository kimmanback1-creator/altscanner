# ══════════════════════════════════════════
#  core/candle.py  –  15분봉 타이머
#  마감 시 스냅샷 → 백분위 → 신호 → 저장 → 알림
# ══════════════════════════════════════════

import asyncio
import time
import logging
from datetime import datetime, timezone, timedelta

from config import CANDLE_MIN, CLEANUP_HOUR
import core.state as state
from core.scorer import calc_score, calc_score_4h, calc_score_1h, check_signal, format_telegram
from db.supabase import insert_candle, sent_within_hours, log_signal, run_cleanup, refresh_ticker_counts, cleanup_liquidations, insert_major_hourly, cleanup_major_hourly_db
from notify.telegram import send_message
from exchanges import binance as ex_binance, okx as ex_okx, bybit as ex_bybit

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))
EXCHANGES = ["binance", "okx", "bybit"]

# ── 메이저 (BTC/ETH/SOL) 거래소별 심볼 ──
MAJOR_SYMBOLS = {
    "binance": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
    "okx":     ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"],
    "bybit":   ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
}

# 기본 임계값 (페이지 슬라이더로 변경 가능 → 추후 Supabase config 테이블로 연동)
LONG_PARAMS  = {"cvd": 60.0, "oi": 50.0, "vol": 70.0}
SHORT_PARAMS = {"cvd": 60.0, "oi": 50.0, "vol": 70.0}


def _next_candle_close(interval_min: int) -> float:
    """다음 N분봉 마감까지 남은 초"""
    now = time.time()
    interval_sec = interval_min * 60
    return interval_sec - (now % interval_sec)


def _candle_ts() -> int:
    """현재 15분봉 시작 타임스탬프"""
    now = int(time.time())
    interval = CANDLE_MIN * 60
    return now - (now % interval)

def _is_4h_close() -> bool:
    """현재 시점이 4H 봉 마감인지 (UTC 기준 0/4/8/12/16/20시 정각)"""
    now_utc = datetime.now(timezone.utc)
    return now_utc.hour % 4 == 0 and now_utc.minute < CANDLE_MIN
    # 정확히 :00:00 부터 다음 15분봉 마감(:14:59)까지를 4H 마감 시점으로 처리
    # 즉 _next_candle_close가 막 끝나서 분석 시작할 때 이게 True면 4H 마감


def _candle_ts_4h() -> int:
    """현재 4시간봉 시작 타임스탬프"""
    now = int(time.time())
    interval = 4 * 60 * 60  # 4시간 = 14400초
    return now - (now % interval)

def _is_1h_close() -> bool:
    """현재 시점이 1H 봉 마감인지 (UTC 매 정시)"""
    now_utc = datetime.now(timezone.utc)
    return now_utc.minute < CANDLE_MIN
    # :00 ~ :14 사이에 분석이 시작되면 1H 마감

def _candle_ts_1h() -> int:
    """현재 1시간봉 시작 타임스탬프"""
    now = int(time.time())
    return now - (now % 3600)

async def candle_loop():
    """15분마다 실행 메인 루프 + 4H 마감 시 추가 분석"""
    logger.info("캔들 루프 시작")
    cleanup_counter = 0

    while True:
        wait = _next_candle_close(CANDLE_MIN)
        logger.info(f"다음 {CANDLE_MIN}분봉 마감까지 {wait:.0f}초")
        await asyncio.sleep(wait)

        ts  = _candle_ts()
        now_kst = datetime.now(KST).strftime("%H:%M")
        logger.info(f"[캔들] {CANDLE_MIN}분봉 마감 — KST {now_kst}")

        # ── 24h 변화율 갱신 (3거래소 병렬, 분석 전에) ──
        try:
            await asyncio.gather(
                ex_binance.fetch_24h_only(),
                ex_okx.fetch_24h_only(),
                ex_bybit.fetch_24h_only(),
            )
            logger.info("[캔들] 24h 변화율 갱신 완료")
        except Exception as e:
            logger.error(f"[캔들] 24h 갱신 실패: {e}")
            
        # ── 15분 분석 (기존) ──
        results = []
        for exchange in EXCHANGES:
            symbols = state.get_all_symbols(exchange)
            logger.info(f"[캔들] {exchange} 심볼 수: {len(symbols)}")
            for symbol in symbols:
                snap = state.snapshot_and_reset(exchange, symbol)
                result = calc_score(snap)
                if result is None:
                    continue
                result["timeframe"] = "15m"
                results.append(result)

                # Supabase 저장
                await insert_candle(result, ts)

        logger.info(f"[캔들] 15분 분석 완료 — {len(results)}개 심볼")

        # ── 4시간 분석 (4H 마감 시점만) ──
        is_4h = _is_4h_close()
        results_4h = []
        if is_4h:
            ts_4h = _candle_ts_4h()
            logger.info(f"[캔들] ★ 4시간봉 마감 — KST {now_kst}")
            for exchange in EXCHANGES:
                symbols = state.get_all_symbols(exchange)
                for symbol in symbols:
                    snap_4h = state.snapshot_and_reset_4h(exchange, symbol)
                    result_4h = calc_score_4h(snap_4h)
                    if result_4h is None:
                        continue
                    results_4h.append(result_4h)

                    # Supabase 저장 (timeframe='4h')
                    await insert_candle(result_4h, ts_4h)
            logger.info(f"[캔들] 4시간 분석 완료 — {len(results_4h)}개 심볼")

        # ── 1시간 분석 (메이저 BTC/ETH/SOL만, 1H 마감 시) ──
        is_1h = _is_1h_close()
        if is_1h:
            ts_1h = _candle_ts_1h()
            logger.info(f"[캔들] ★ 1시간봉 마감 — KST {now_kst}")
            count_1h = 0
            for exchange in EXCHANGES:
                for symbol in MAJOR_SYMBOLS.get(exchange, []):
                    snap_1h = state.snapshot_and_reset_1h(exchange, symbol)
                    if snap_1h["vol_candle"] == 0:
                        continue  # 거래 없으면 스킵
                    # 진단 시도 — 데이터 부족(flat 등) 시 None, 그래도 raw는 저장
                    result_1h = calc_score_1h(snap_1h)
                    data_to_save = result_1h if result_1h else snap_1h
                    await insert_major_hourly(data_to_save, ts_1h)
                    count_1h += 1
            logger.info(f"[캔들] 1시간 메이저 저장 완료 — {count_1h}개")

            # 7일 지난 데이터 정리
            await cleanup_major_hourly_db()

        # 신호 판정 + 텔레그램
        for result in results:
            direction = check_signal(result, LONG_PARAMS, SHORT_PARAMS)
            if direction is None:
                continue

            symbol   = result["symbol"]
            exchange = result["exchange"]

           # 4시간 쿨다운 체크
            if await sent_within_hours(exchange, symbol, direction, hours=4):
                logger.info(f"[알림] 쿨다운 — {symbol} {direction} (4h 이내 전송됨)")
                await log_signal(result, direction, sent=False)  # 관찰용 기록
                continue

            # 텔레그램 전송
            msg = format_telegram(result, direction)
            await send_message(msg)
            await log_signal(result, direction, sent=True)
            await asyncio.sleep(0.3)  # rate limit 방지

        # 1시간마다 롤링 딜리트
        cleanup_counter += 1
        if cleanup_counter >= (60 // CANDLE_MIN):
            await run_cleanup()
            await cleanup_liquidations()
            cleanup_counter = 0
        # ticker_counts 갱신 (사이클 끝, INSERT/DELETE 모두 반영)
        await refresh_ticker_counts()
