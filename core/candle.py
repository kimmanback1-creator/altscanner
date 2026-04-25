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
from core.scorer import calc_score, check_signal, format_telegram
from db.supabase import insert_candle, already_sent_today, log_signal, run_cleanup
from notify.telegram import send_message

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))
EXCHANGES = ["binance", "okx", "bybit"]

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


async def candle_loop():
    """15분마다 실행 메인 루프"""
    logger.info("캔들 루프 시작")
    cleanup_counter = 0

    while True:
        wait = _next_candle_close(CANDLE_MIN)
        logger.info(f"다음 {CANDLE_MIN}분봉 마감까지 {wait:.0f}초")
        await asyncio.sleep(wait)

        ts  = _candle_ts()
        now_kst = datetime.now(KST).strftime("%H:%M")
        logger.info(f"[캔들] {CANDLE_MIN}분봉 마감 — KST {now_kst}")

        results = []

        for exchange in EXCHANGES:
            symbols = state.get_all_symbols(exchange)
            for symbol in symbols:
                snap = state.snapshot_and_reset(exchange, symbol)
                result = calc_score(snap)
                if result is None:
                    continue
                results.append(result)

                # Supabase 저장 (비동기)
                await insert_candle(result, ts)

        logger.info(f"[캔들] 분석 완료 — {len(results)}개 심볼")

        # 신호 판정 + 텔레그램
        for result in results:
            direction = check_signal(result, LONG_PARAMS, SHORT_PARAMS)
            if direction is None:
                continue

            symbol   = result["symbol"]
            exchange = result["exchange"]

            # 오늘 이미 보냈는지 체크
            if await already_sent_today(exchange, symbol, direction):
                logger.info(f"[알림] 오늘 이미 전송 — {symbol} {direction}")
                continue

            # 텔레그램 전송
            msg = format_telegram(result, direction)
            await send_message(msg)
            await log_signal(result, direction)
            await asyncio.sleep(0.3)  # rate limit 방지

        # 1시간마다 롤링 딜리트
        cleanup_counter += 1
        if cleanup_counter >= (60 // CANDLE_MIN):
            await run_cleanup()
            cleanup_counter = 0
