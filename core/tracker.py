# ══════════════════════════════════════════
#  core/tracker.py  –  알림 성과 추적 (Phase 0 검증)
#
#  signal_log INSERT 시 signal_performance row 자동 생성.
#  15분마다 추적 중인 행들의 현재가 갱신 + 체크포인트 채우기.
#  트레일링 시뮬: 신고점에서 callback% 도달 시점 청산 가정 (파라미터 하단 정의).
#  7일 경과 시 status='completed'.
# ══════════════════════════════════════════

import asyncio
import logging
from datetime import datetime, timedelta, timezone
import requests
from db.supabase import get_client

logger = logging.getLogger(__name__)

# ── 트레일링 시뮬 파라미터 ──────────────────
TRAILING_ACTIVATE_PCT = 5.0    # +5% 도달 시 활성화
TRAILING_CALLBACK_PCT = 5.0    # 신고점 -5% 시 청산
INITIAL_SL_PCT        = 5.0    # 초기 SL: -5%

# ── 자동 셋업(BTC) 추적 파라미터 (변동성 작아 타이트) ──
REC_TRAIL_ACTIVATE_PCT = 3.0
REC_TRAIL_CALLBACK_PCT = 3.0
REC_INITIAL_SL_PCT     = 3.0
REC_BTC_INST = "BTC-USDT-SWAP"   # OKX 무기한

# 체크포인트 (분 단위)
CHECKPOINTS = [
    (15,           "price_15m",  "pnl_15m"),
    (60,           "price_1h",   "pnl_1h"),
    (60 * 4,       "price_4h",   "pnl_4h"),
    (60 * 24,      "price_24h",  "pnl_24h"),
    (60 * 24 * 3,  "price_72h",  "pnl_72h"),
    (60 * 24 * 7,  "price_7d",   "pnl_7d"),
]

TRACK_DAYS = 7   # 7일 추적


# ── signal_log → signal_performance 생성 ─────
def create_performance_row(signal_id: int, result: dict, direction: str, entry_at: datetime):
    """알림 발생 시 추적 row 생성. (log_signal 직후 호출)"""
    sb = get_client()
    try:
        sb.table("signal_performance").insert({
            "signal_id":   signal_id,
            "symbol":      result["symbol"],
            "exchange":    result["exchange"],
            "direction":   direction,
            "entry_price": float(result["price"]),
            "entry_at":    entry_at.isoformat(),
            "status":      "tracking",
        }).execute()
        logger.info(f"[tracker] 추적 시작 — {result['symbol']} {direction}")
    except Exception as e:
        # UNIQUE 위반 등은 무시 (중복 INSERT 방지)
        if "duplicate" not in str(e).lower():
            logger.error(f"[tracker] row 생성 실패: {e}")


# ── 현재가 조회 (candle_data 최신 15m) ──────
def fetch_recent_candles(exchange: str, symbol: str, limit: int = 30) -> list[tuple] | None:
    """최근 1분봉 OHLC를 시간 오름차순으로 반환. [(ts_ms, high, low, close), ...]
    OKX/Bybit 둘 다 응답이 최신→과거 역순이라 reversed로 뒤집어 시간순 정렬.
    실패 시 None (호출부에서 종가 폴링으로 fallback)."""
    try:
        if exchange == "okx":
            url = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar=1m&limit={limit}"
            resp = requests.get(url, timeout=8)
            raw = (resp.json() or {}).get("data") or []
        elif exchange == "bybit":
            url = f"https://api.bybit.com/v5/market/kline?category=linear&symbol={symbol}&interval=1&limit={limit}"
            resp = requests.get(url, timeout=8)
            raw = ((resp.json() or {}).get("result") or {}).get("list") or []
        else:
            return None  # binance 등은 미지원 → fallback

        if not raw:
            return None

        # 둘 다 [ts, o, h, l, c, ...], 최신→과거 → 시간순으로 뒤집기
        candles = []
        for c in reversed(raw):
            candles.append((
                int(c[0]),       # ts_ms
                float(c[2]),     # high
                float(c[3]),     # low
                float(c[4]),     # close
            ))
        return candles
    except Exception as e:
        logger.warning(f"[tracker] candles 조회 실패 {exchange}:{symbol}: {e}")
        return None


def fetch_current_price(exchange: str, symbol: str) -> float | None:
    sb = get_client()
    try:
        res = (
            sb.table("candle_data")
            .select("price")
            .eq("exchange", exchange)
            .eq("symbol", symbol)
            .eq("timeframe", "15m")
            .order("ts", desc=True)
            .limit(1)
            .execute()
        )
        rows = res.data or []
        return float(rows[0]["price"]) if rows and rows[0].get("price") else None
    except Exception as e:
        logger.error(f"[tracker] 가격 조회 실패 {symbol}: {e}")
        return None


# ── PnL 계산 (direction 적용) ──────────────
def calc_pnl(entry: float, current: float, direction: str) -> float:
    if not entry or not current:
        return 0.0
    pct = (current - entry) / entry * 100
    return pct if direction == "LONG" else -pct


def _price_at_pnl(entry: float, pnl_pct: float, direction: str) -> float:
    """주어진 PnL%에 해당하는 가격 (exit_price 기록용)."""
    if direction == "LONG":
        return entry * (1 + pnl_pct / 100)
    return entry * (1 - pnl_pct / 100)


# ── 단일 row 갱신 ──────────────────────────
def update_one(row: dict, candles: list = None) -> bool:
    """추적 중인 행 1개 갱신. 7일 경과 시 completed 처리.
    candles: 사이클에서 미리 받아둔 1분봉 (심볼별 중복 fetch 방지). None이면 직접 조회."""
    symbol    = row["symbol"]
    exchange  = row["exchange"]
    direction = row["direction"]
    entry     = float(row["entry_price"])
    entry_at  = datetime.fromisoformat(row["entry_at"].replace("Z", "+00:00"))
    now       = datetime.now(timezone.utc)
    elapsed_min = (now - entry_at).total_seconds() / 60

    # 봉 단위 정밀 추적 — 미리 받은 캐시 우선, 없으면 직접 조회
    if candles is None:
        candles = fetch_recent_candles(exchange, symbol)
    current = fetch_current_price(exchange, symbol)
    if current is None and candles:
        current = candles[-1][3]  # 마지막 봉 종가 fallback
    if current is None:
        return False

    current_pnl = calc_pnl(entry, current, direction)

    # 변경 사항 누적
    updates = {}

    # ── 이미 청산된 행: 추적 중단 (포지션 = 단일 청산 이벤트) ──
    already_exited = row.get("exit_reason") is not None

    # ── 체크포인트 (청산 후에도 참고용으로 계속 채움) ──
    for mins, price_col, pnl_col in CHECKPOINTS:
        if elapsed_min >= mins and row.get(price_col) is None:
            updates[price_col] = current
            updates[pnl_col] = round(current_pnl, 3)

    # ── 청산 후엔 max/SL/trailing 동결, 체크포인트만 기록 ──
    if not already_exited:
        old_max = row.get("max_pnl") or 0
        old_min = row.get("min_pnl") or 0
        max_now = old_max
        min_now = old_min

        # 봉 시퀀스 구성: candles 있으면 1분봉 (high/low), 없으면 현재가 1점 fallback
        # 각 원소 = (favorable_pnl, adverse_pnl) — 방향별로 유리/불리 극값
        # LONG: high=유리(수익), low=불리(손실) / SHORT: 반대
        seq = []
        if candles:
            for ts_ms, high, low, close in candles:
                hp = calc_pnl(entry, high, direction)
                lp = calc_pnl(entry, low, direction)
                fav = max(hp, lp)   # 유리 극값
                adv = min(hp, lp)   # 불리 극값
                seq.append((fav, adv))
        else:
            seq.append((current_pnl, current_pnl))

        exit_done = False
        for fav, adv in seq:
            if exit_done:
                break
            # 1) 고점 먼저 — 신고점 갱신
            new_high = fav > max_now
            if fav > max_now:
                max_now = fav
            if adv < min_now:
                min_now = adv

            # 신고점을 세운 봉은 저점이 고점보다 시간상 앞설 수 있어
            # (봉 OHLC만으론 순서 불명) 트레일/SL 트리거에서 제외 — 거짓 조기청산 방지.
            # 청산은 다음 봉(고점 갱신 안 한 봉)의 저점에서 판정.
            if new_high:
                continue

            trailing_active = max_now >= TRAILING_ACTIVATE_PCT

            # 2) 저점으로 트레일/SL 터치 판정 (같은 봉: 고점 먼저, 저점 나중)
            if not trailing_active:
                # 트레일링 비활성: 고정 SL
                if adv <= -INITIAL_SL_PCT:
                    exit_pnl = -INITIAL_SL_PCT
                    if adv < exit_pnl:  # 갭다운 — 트리거선보다 더 아래에서 체결
                        exit_pnl = adv
                    updates["sl_hit"]      = True
                    updates["sl_hit_at"]   = now.isoformat()
                    updates["exit_reason"] = "sl"
                    updates["exit_pnl"]    = round(exit_pnl, 3)
                    updates["exit_price"]  = round(_price_at_pnl(entry, exit_pnl, direction), 8)
                    updates["exit_at"]     = now.isoformat()
                    exit_done = True
            else:
                # 트레일링 활성: 신고점 -callback% 트리거
                trail_line = max_now - TRAILING_CALLBACK_PCT
                if adv <= trail_line:
                    exit_pnl = trail_line
                    if adv < exit_pnl:  # 갭다운 — 트리거선보다 더 아래에서 체결
                        exit_pnl = adv
                    updates["exit_reason"]         = "trailing"
                    updates["exit_pnl"]            = round(exit_pnl, 3)
                    updates["exit_price"]          = round(_price_at_pnl(entry, exit_pnl, direction), 8)
                    updates["exit_at"]             = now.isoformat()
                    updates["trailing_exit_price"] = updates["exit_price"]
                    updates["trailing_exit_at"]    = now.isoformat()
                    updates["trailing_pnl"]        = round(exit_pnl, 3)
                    exit_done = True

        # max/min 갱신분 반영
        if max_now > old_max:
            updates["max_pnl"] = round(max_now, 3)
            updates["max_pnl_at"] = now.isoformat()
        if min_now < old_min:
            updates["min_pnl"] = round(min_now, 3)

        # TP 히트 플래그 (max_now 기준)
        if not row.get("tp_10_hit")  and max_now >= 10:  updates["tp_10_hit"]  = True
        if not row.get("tp_20_hit")  and max_now >= 20:  updates["tp_20_hit"]  = True
        if not row.get("tp_50_hit")  and max_now >= 50:  updates["tp_50_hit"]  = True
        if not row.get("tp_100_hit") and max_now >= 100: updates["tp_100_hit"] = True
    # ── 7일 경과 시 완료 처리 ─────────────
    if elapsed_min >= 60 * 24 * TRACK_DAYS:
        updates["status"] = "completed"
        updates["completed_at"] = now.isoformat()
        # 청산 이벤트 없이 7일 도달 → timeout 마감 (7d 가격으로)
        if row.get("exit_reason") is None and updates.get("exit_reason") is None:
            updates["exit_reason"] = "timeout"
            updates["exit_pnl"]    = round(current_pnl, 3)
            updates["exit_price"]  = current
            updates["exit_at"]     = now.isoformat()
            # 레거시 컬럼 호환
            updates["trailing_pnl"]        = round(current_pnl, 3)
            updates["trailing_exit_price"] = current
            updates["trailing_exit_at"]    = now.isoformat()
    if not updates:
        return False

    # 한 번의 UPDATE로 모든 변경사항 반영
    try:
        sb = get_client()
        sb.table("signal_performance").update(updates).eq("id", row["id"]).execute()
        return True
    except Exception as e:
        logger.error(f"[tracker] {symbol} 갱신 실패: {e}")
        return False


# ── 메인 갱신 사이클 ───────────────────────
def update_all_tracking():
    """status=tracking 인 행 전체 갱신."""
    sb = get_client()
    try:
        res = (
            sb.table("signal_performance")
            .select("*")
            .eq("status", "tracking")
            .execute()
        )
        rows = res.data or []
    except Exception as e:
        logger.error(f"[tracker] 추적 목록 조회 실패: {e}")
        return

    if not rows:
        logger.info("[tracker] 추적 중인 알림 없음")
        return

    # ── 고유 (exchange, symbol)만 candles 1회씩 prefetch (중복 티커 호출 절감) ──
    candle_cache = {}
    uniq_keys = {(r["exchange"], r["symbol"]) for r in rows}
    for ex, sym in uniq_keys:
        candle_cache[(ex, sym)] = fetch_recent_candles(ex, sym)

    logger.info(f"[tracker] candles prefetch — 추적 {len(rows)}건 / 고유 심볼 {len(uniq_keys)}개")

    updated = 0
    for r in rows:
        cached = candle_cache.get((r["exchange"], r["symbol"]))
        if update_one(r, cached):
            updated += 1

    logger.info(f"[tracker] 사이클 완료 — {len(rows)}건 검토, {updated}건 갱신")


# ══════════════════════════════════════════
#  자동 셋업(BTC) 추천 추적 — rec_performance
#  STRONG 추천 발생 시 진입 가정.
#  두 전략 동시 시뮬:
#    trail_* : 3% 트레일링/SL (레벨 무시)
#    fixed_* : 추천이 준 TP/SL 도달 여부
#  PnL = 순수 가격 변화율 (레버리지 미적용)
# ══════════════════════════════════════════
import aiohttp

OKX_TICKER_URL = "https://www.okx.com/api/v5/market/ticker"


async def fetch_btc_price() -> float | None:
    """OKX 무기한 BTC 현재가 (REST 단발 조회)."""
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{OKX_TICKER_URL}?instId={REC_BTC_INST}"
            async with session.get(url, timeout=10) as resp:
                data = await resp.json()
        last = data.get("data", [{}])[0].get("last")
        return float(last) if last else None
    except Exception as e:
        logger.error(f"[rec-tracker] BTC 가격 조회 실패: {e}")
        return None


def create_rec_performance_row(tf: str, bar_ts: str, rec: dict, entry_at: datetime):
    """STRONG 추천 발생 시 추적 row 생성 (recommendation.check_and_push에서 호출)."""
    sb = get_client()
    levels = rec.get("levels") or {}
    try:
        sb.table("rec_performance").insert({
            "timeframe":      tf,
            "bar_ts":         bar_ts,
            "verdict":        rec["verdict"],
            "direction":      rec["direction"],
            "confidence_pct": rec.get("confidence_pct"),
            "entry_price":    float(levels.get("entry") or 0),
            "rec_sl":         levels.get("stop_loss"),
            "rec_tp":         levels.get("take_profit"),
            "rec_tp2":        levels.get("take_profit2"),
            "entry_at":       entry_at.isoformat(),
            "status":         "tracking",
        }).execute()
        logger.info(f"[rec-tracker] 추적 시작 — {tf} {rec['verdict']} @ {levels.get('entry')}")
    except Exception as e:
        if "duplicate" not in str(e).lower():
            logger.error(f"[rec-tracker] row 생성 실패: {e}")


def _rec_price_at_pnl(entry: float, pnl_pct: float, direction: str) -> float:
    """주어진 PnL%에 해당하는 가격 (exit_price 기록용)."""
    if direction == "LONG":
        return entry * (1 + pnl_pct / 100)
    return entry * (1 - pnl_pct / 100)


def update_rec_one(row: dict, current: float) -> bool:
    """rec_performance 행 1개 갱신. trail/fixed 두 청산 독립 추적."""
    direction = row["direction"]
    entry     = float(row["entry_price"])
    entry_at  = datetime.fromisoformat(row["entry_at"].replace("Z", "+00:00"))
    now       = datetime.now(timezone.utc)
    elapsed_min = (now - entry_at).total_seconds() / 60

    if not entry or current is None:
        return False

    current_pnl = calc_pnl(entry, current, direction)
    updates = {}

    # ── 체크포인트 (청산 여부와 무관하게 계속 채움) ──
    for mins, price_col, pnl_col in CHECKPOINTS:
        if elapsed_min >= mins and row.get(price_col) is None:
            updates[price_col] = round(current, 2)
            updates[pnl_col]   = round(current_pnl, 3)

    # ── max/min (둘 다 청산되기 전까지 갱신; trail 활성화 판정에 필요) ──
    old_max = row.get("max_pnl") or 0
    old_min = row.get("min_pnl") or 0
    if current_pnl > old_max:
        updates["max_pnl"] = round(current_pnl, 3)
        updates["max_pnl_at"] = now.isoformat()
    if current_pnl < old_min:
        updates["min_pnl"] = round(current_pnl, 3)
    max_now = max(old_max, current_pnl)

    # ══ 전략 A: 3% 트레일링/SL ══
    if row.get("trail_exit_reason") is None:
        trailing_active = max_now >= REC_TRAIL_ACTIVATE_PCT
        if not trailing_active:
            if current_pnl <= -REC_INITIAL_SL_PCT:
                exit_pnl = min(current_pnl, -REC_INITIAL_SL_PCT)
                updates["trail_exit_reason"] = "sl"
                updates["trail_exit_pnl"]    = round(exit_pnl, 3)
                updates["trail_exit_price"]  = round(_rec_price_at_pnl(entry, exit_pnl, direction), 2)
                updates["trail_exit_at"]     = now.isoformat()
        else:
            if (max_now - current_pnl) >= REC_TRAIL_CALLBACK_PCT:
                exit_pnl = min(current_pnl, max_now - REC_TRAIL_CALLBACK_PCT)
                updates["trail_exit_reason"] = "trailing"
                updates["trail_exit_pnl"]    = round(exit_pnl, 3)
                updates["trail_exit_price"]  = round(_rec_price_at_pnl(entry, exit_pnl, direction), 2)
                updates["trail_exit_at"]     = now.isoformat()

    # ══ 전략 B: 추천 고정 TP/SL ══
    if row.get("fixed_exit_reason") is None:
        rec_sl  = row.get("rec_sl")
        rec_tp  = row.get("rec_tp")
        rec_tp2 = row.get("rec_tp2")
        hit = None
        exit_price = None
        if direction == "LONG":
            # SL 우선 체크 (보수적: 같은 봉에 둘 다 닿으면 손실 가정)
            if rec_sl and current <= float(rec_sl):
                hit, exit_price = "sl", float(rec_sl)
            elif rec_tp2 and current >= float(rec_tp2):
                hit, exit_price = "tp2", float(rec_tp2)
            elif rec_tp and current >= float(rec_tp):
                hit, exit_price = "tp", float(rec_tp)
        else:  # SHORT
            if rec_sl and current >= float(rec_sl):
                hit, exit_price = "sl", float(rec_sl)
            elif rec_tp2 and current <= float(rec_tp2):
                hit, exit_price = "tp2", float(rec_tp2)
            elif rec_tp and current <= float(rec_tp):
                hit, exit_price = "tp", float(rec_tp)
        if hit:
            updates["fixed_exit_reason"] = hit
            updates["fixed_exit_pnl"]    = round(calc_pnl(entry, exit_price, direction), 3)
            updates["fixed_exit_price"]  = round(exit_price, 2)
            updates["fixed_exit_at"]     = now.isoformat()

    # ── 7일 경과: 미청산 전략은 timeout 마감 + status 완료 ──
    if elapsed_min >= 60 * 24 * TRACK_DAYS:
        updates["status"] = "completed"
        updates["completed_at"] = now.isoformat()
        if row.get("trail_exit_reason") is None and "trail_exit_reason" not in updates:
            updates["trail_exit_reason"] = "timeout"
            updates["trail_exit_pnl"]    = round(current_pnl, 3)
            updates["trail_exit_price"]  = round(current, 2)
            updates["trail_exit_at"]     = now.isoformat()
        if row.get("fixed_exit_reason") is None and "fixed_exit_reason" not in updates:
            updates["fixed_exit_reason"] = "timeout"
            updates["fixed_exit_pnl"]    = round(current_pnl, 3)
            updates["fixed_exit_price"]  = round(current, 2)
            updates["fixed_exit_at"]     = now.isoformat()

    if not updates:
        return False
    try:
        sb = get_client()
        sb.table("rec_performance").update(updates).eq("id", row["id"]).execute()
        return True
    except Exception as e:
        logger.error(f"[rec-tracker] 갱신 실패 id={row.get('id')}: {e}")
        return False


async def update_all_rec_tracking():
    """rec_performance status=tracking 전체 갱신 (BTC 단일가 1회 조회)."""
    sb = get_client()
    try:
        res = sb.table("rec_performance").select("*").eq("status", "tracking").execute()
        rows = res.data or []
    except Exception as e:
        logger.error(f"[rec-tracker] 추적 목록 조회 실패: {e}")
        return

    if not rows:
        return

    current = await fetch_btc_price()
    if current is None:
        logger.warning("[rec-tracker] BTC 가격 없음 — 이번 사이클 스킵")
        return

    updated = 0
    for r in rows:
        if update_rec_one(r, current):
            updated += 1
    logger.info(f"[rec-tracker] 사이클 — {len(rows)}건 검토, {updated}건 갱신 (BTC ${current:,.0f})")


# ── 백그라운드 루프 ─────────────────────────
async def tracker_loop():
    """15분마다 추적 갱신."""
    logger.info("[tracker] 추적 워커 시작")

    await asyncio.sleep(90)  # 부팅 후 1.5분 대기 (다른 워커 안정화 후)

    while True:
        try:
            update_all_tracking()
        except Exception as e:
            logger.error(f"[tracker] 사이클 실패: {e}", exc_info=True)

        try:
            await update_all_rec_tracking()
        except Exception as e:
            logger.error(f"[rec-tracker] 사이클 실패: {e}", exc_info=True)

        # 다음 15분봉 마감 + 30초 후 (가격 안정화 대기)
        now = datetime.now(timezone.utc)
        next_quarter = (now.minute // 15 + 1) * 15
        if next_quarter >= 60:
            next_run = now.replace(minute=0, second=30, microsecond=0) + timedelta(hours=1)
        else:
            next_run = now.replace(minute=next_quarter, second=30, microsecond=0)
        sleep_sec = (next_run - now).total_seconds()
        logger.info(f"[tracker] 다음 사이클 {next_run.isoformat()} ({sleep_sec/60:.1f}분 후)")
        await asyncio.sleep(sleep_sec)
