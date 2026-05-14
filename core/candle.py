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
from core.scorer import calc_score, calc_score_4h, check_signal, format_telegram
from db.supabase import insert_candle, sent_within_hours, log_signal, run_cleanup, refresh_ticker_counts, cleanup_liquidations, fetch_watchlist
from notify.telegram import send_message
from exchanges import okx as ex_okx, bybit as ex_bybit  # binance 제외

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))
EXCHANGES = ["okx", "bybit"]  # Binance는 Render Singapore IP throttle로 제외

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

        # ── 24h 변화율 갱신 (OKX/Bybit 병렬, 분석 전에) ──
        try:
            await asyncio.gather(
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
            # watchlist 심볼은 force=True (분석 통과 못해도 DB 저장)
            watchlist_set = set(fetch_watchlist(exchange))
            for symbol in symbols:
                snap = state.snapshot_and_reset(exchange, symbol)
                is_watch = symbol in watchlist_set
                result = calc_score(snap, force=is_watch)
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
                watchlist_set = set(fetch_watchlist(exchange))
                for symbol in symbols:
                    snap_4h = state.snapshot_and_reset_4h(exchange, symbol)
                    is_watch = symbol in watchlist_set
                    result_4h = calc_score_4h(snap_4h, force=is_watch)
                    if result_4h is None:
                        continue
                    results_4h.append(result_4h)

                    # Supabase 저장 (timeframe='4h')
                    await insert_candle(result_4h, ts_4h)
            logger.info(f"[캔들] 4시간 분석 완료 — {len(results_4h)}개 심볼")

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
