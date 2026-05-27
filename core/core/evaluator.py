# ══════════════════════════════════════════
#  core/evaluator.py  –  setup_log_auto 자동 평가 워커
#
#  매 4H 봉 마감 시점(UTC 0/4/8/12/16/20시 + 5분)에
#  7일 지난 pending 셋업의 결과를 7개 라벨로 자동 판정.
#  OKX history-candles API 사용 (Binance 차단 대체).
# ══════════════════════════════════════════

import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta, timezone

from db.supabase import get_client

logger = logging.getLogger(__name__)

OKX_HISTORY_URL = "https://www.okx.com/api/v5/market/history-candles"
OKX_SYMBOL = "BTC-USDT-SWAP"

# 라벨 판정 임계값
THRESHOLD_4H = 0.03   # ±3%
THRESHOLD_1D = 0.05   # ±5%

# 평가 기간
EVAL_DAYS = 7

# 임밸런스 흡수 판정 — entry로부터 % 이내 터치로 인정
IMB_TOUCH_TOLERANCE = 0.003   # ±0.3%


# ── OKX OHLC fetch ──────────────────────────
async def fetch_okx_ohlc(session, start_ts: datetime, end_ts: datetime, bar: str = "1H"):
    """
    OKX history-candles로 [start_ts, end_ts) 범위 봉 가져옴.
    반환: 시간순 오름차순 [{ts_ms, open, high, low, close}, ...]
    """
    start_ms = int(start_ts.timestamp() * 1000)
    end_ms = int(end_ts.timestamp() * 1000)

    bars = []
    cursor_before = end_ms

    while cursor_before > start_ms:
        params = {
            "instId": OKX_SYMBOL,
            "bar": bar,
            "before": str(cursor_before),
            "after": str(start_ms - 1),
            "limit": "100",
        }
        try:
            async with session.get(OKX_HISTORY_URL, params=params, timeout=10) as resp:
                data = await resp.json()
        except Exception as e:
            logger.warning(f"[okx_ohlc] 요청 실패: {e}")
            break

        if data.get("code") != "0":
            logger.warning(f"[okx_ohlc] API 오류: {data}")
            break

        candles = data.get("data", [])
        if not candles:
            break

        for c in candles:
            ts_ms = int(c[0])
            if ts_ms < start_ms:
                continue
            bars.append({
                "ts_ms": ts_ms,
                "open":  float(c[1]),
                "high":  float(c[2]),
                "low":   float(c[3]),
                "close": float(c[4]),
            })

        oldest_ms = int(candles[-1][0])
        if oldest_ms <= start_ms:
            break
        cursor_before = oldest_ms
        await asyncio.sleep(0.1)

    bars.sort(key=lambda x: x["ts_ms"])
    return bars


# ── 라벨 판정 ──────────────────────────────
def judge(bars: list, entry_price: float, threshold: float, signals: dict) -> str:
    """7개 라벨 판정"""
    if not bars:
        return "횡보"

    target_up   = entry_price * (1 + threshold)
    target_down = entry_price * (1 - threshold)

    hit_up = hit_down = False
    hit_up_idx = hit_down_idx = None

    for idx, bar in enumerate(bars):
        if not hit_up   and bar["high"] >= target_up:
            hit_up, hit_up_idx = True, idx
        if not hit_down and bar["low"]  <= target_down:
            hit_down, hit_down_idx = True, idx
        if hit_up and hit_down:
            break

    # ── 임밸런스 흡수 케이스 (1~5보다 우선) ──
    imb = (signals or {}).get("imbalance") or {}
    nearest_support    = imb.get("nearest_support")
    nearest_resistance = imb.get("nearest_resistance")

    # 매수 임밸런스 흡수 후 상승
    if nearest_support is not None and hit_up and not hit_down:
        try:
            ns = float(nearest_support)
            ns_tol = ns * (1 + IMB_TOUCH_TOLERANCE)
            check_range = bars[:hit_up_idx + 1]
            if any(b["low"] <= ns_tol for b in check_range):
                return "매수 임밸런스 흡수 후 상승"
        except (ValueError, TypeError):
            pass

    # 매도 임밸런스 흡수 후 하락
    if nearest_resistance is not None and hit_down and not hit_up:
        try:
            nr = float(nearest_resistance)
            nr_tol = nr * (1 - IMB_TOUCH_TOLERANCE)
            check_range = bars[:hit_down_idx + 1]
            if any(b["high"] >= nr_tol for b in check_range):
                return "매도 임밸런스 흡수 후 하락"
        except (ValueError, TypeError):
            pass

    # ── 일반 5개 케이스 ──
    if hit_up and not hit_down:
        return "상승"
    if hit_down and not hit_up:
        return "하락"
    if hit_up and hit_down:
        return "상승 후 하락" if hit_up_idx < hit_down_idx else "하락 후 상승"
    return "횡보"


# ── 한 봉 평가 ──────────────────────────────
async def evaluate_one(session, row: dict) -> bool:
    """한 행 평가 후 UPDATE"""
    try:
        bar_ts = datetime.fromisoformat(row["bar_ts"].replace("Z", "+00:00"))
        if bar_ts.tzinfo is None:
            bar_ts = bar_ts.replace(tzinfo=timezone.utc)

        timeframe   = row["timeframe"]
        entry_price = float(row["entry_price"])
        signals     = row.get("signals") or {}

        if timeframe == "4H":
            bar_duration, threshold = timedelta(hours=4), THRESHOLD_4H
        elif timeframe == "1D":
            bar_duration, threshold = timedelta(days=1),  THRESHOLD_1D
        else:
            logger.warning(f"[evaluator] 알 수 없는 timeframe: {timeframe}")
            return False

        eval_start = bar_ts + bar_duration  # 봉 마감 시점부터
        eval_end   = eval_start + timedelta(days=EVAL_DAYS)

        now = datetime.now(timezone.utc)
        if eval_end > now:
            return False  # 아직 평가 시점 안 됨

        bars = await fetch_okx_ohlc(session, eval_start, eval_end, bar="1H")
        if not bars:
            logger.warning(f"[evaluator] OHLC 없음: {timeframe} {bar_ts.isoformat()}")
            return False

        eval_high  = max(b["high"]  for b in bars)
        eval_low   = min(b["low"]   for b in bars)
        eval_close = bars[-1]["close"]

        result = judge(bars, entry_price, threshold, signals)

        sb = get_client()
        sb.table("setup_log_auto").update({
            "result":     result,
            "result_at":  now.isoformat(),
            "eval_high":  eval_high,
            "eval_low":   eval_low,
            "eval_close": eval_close,
        }).eq("id", row["id"]).execute()

        logger.info(
            f"[evaluator] {timeframe} {bar_ts.isoformat()} → {result} "
            f"(entry={entry_price:.2f} H={eval_high:.2f} L={eval_low:.2f} C={eval_close:.2f})"
        )
        return True

    except Exception as e:
        logger.error(f"[evaluator] 실패 (id={row.get('id')}): {e}", exc_info=True)
        return False


# ── pending 행 일괄 평가 ───────────────────
async def evaluate_pending():
    """result IS NULL AND 봉마감+7일 < NOW 인 행 평가"""
    sb = get_client()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=EVAL_DAYS + 1)).isoformat()

    res = (
        sb.table("setup_log_auto")
        .select("id, symbol, timeframe, bar_ts, entry_price, signals")
        .is_("result", "null")
        .lte("bar_ts", cutoff)
        .order("bar_ts")
        .execute()
    )
    rows = res.data or []

    if not rows:
        logger.info("[evaluator] 평가할 봉 없음")
        return

    logger.info(f"[evaluator] 평가 시작 — {len(rows)}개 봉")

    success = 0
    async with aiohttp.ClientSession() as session:
        for row in rows:
            if await evaluate_one(session, row):
                success += 1
            await asyncio.sleep(0.2)

    logger.info(f"[evaluator] 평가 완료 — {success}/{len(rows)} 성공")


# ── 백그라운드 루프 ─────────────────────────
async def evaluator_loop():
    """UTC 4H 정각 + 5분마다 평가 실행"""
    logger.info("[evaluator] 워커 시작")

    # 부팅 직후 1차 실행 (30초 대기)
    await asyncio.sleep(30)
    try:
        await evaluate_pending()
    except Exception as e:
        logger.error(f"[evaluator] 초기 실행 실패: {e}", exc_info=True)

    while True:
        now = datetime.now(timezone.utc)
        next_hour_block = (now.hour // 4 + 1) * 4
        if next_hour_block >= 24:
            next_run = now.replace(hour=0, minute=5, second=0, microsecond=0) + timedelta(days=1)
        else:
            next_run = now.replace(hour=next_hour_block, minute=5, second=0, microsecond=0)

        sleep_sec = (next_run - now).total_seconds()
        logger.info(f"[evaluator] 다음 실행 {next_run.isoformat()} ({sleep_sec/60:.0f}분 후)")
        await asyncio.sleep(sleep_sec)

        try:
            await evaluate_pending()
        except Exception as e:
            logger.error(f"[evaluator] 실행 실패: {e}", exc_info=True)
