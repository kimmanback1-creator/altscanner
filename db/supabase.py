# ══════════════════════════════════════════
#  db/supabase.py  –  Supabase 연동
# ══════════════════════════════════════════

import logging
from datetime import datetime, date, timezone, timedelta
from supabase import create_client, Client
from config import SUPABASE_URL, SUPABASE_KEY

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))

_client: Client = None


def get_client() -> Client:
    global _client
    if _client is None:
        _client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _client


def now_kst() -> datetime:
    return datetime.now(KST)


def today_kst() -> date:
    return now_kst().date()


# ── candle_data ───────────────────────────
async def insert_candle(result: dict, ts: int):
    """봉 결과값 저장 (15m 또는 4h)"""
    try:
        get_client().table("candle_data").insert({
            "exchange":  result["exchange"],
            "symbol":    result["symbol"],
            "ts":        ts,
            "ts_kst":    now_kst().isoformat(),
            "cvd_pct":   result["cvd_pct"],
            "oi_pct":    result["oi_pct"],
            "vol_pct":   result["vol_pct"],
            "cvd_delta": result["cvd_delta"],
            "oi_chg":    result["oi_chg"],
            "vol_ratio": result["vol_ratio"],
            "vol_candle": result["vol_candle"],
            "price":     result["price"],
            "price_chg": result["price_chg"],
            "diagnosis": result["diagnosis"],
            "timeframe": result.get("timeframe", "15m"),
        }).execute()
    except Exception as e:
        logger.error(f"[DB] candle insert 실패 {result['symbol']}: {e}")


# ── signal_log ────────────────────────────
async def sent_within_hours(exchange: str, symbol: str, direction: str, hours: int = 4) -> bool:
    """
    최근 N시간 안에 같은 (exchange, symbol, direction)으로 텔레그램 전송됐는지
    sent=True인 것만 카운트 (차단 기록은 무시)
    """
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        res = get_client().table("signal_log")\
            .select("id")\
            .eq("exchange",  exchange)\
            .eq("symbol",    symbol)\
            .eq("direction", direction)\
            .eq("sent",      True)\
            .gte("sent_at",  cutoff)\
            .limit(1)\
            .execute()
        return len(res.data) > 0
    except Exception as e:
        logger.error(f"[DB] signal_log 조회 실패: {e}")
        return False


async def log_signal(result: dict, direction: str, sent: bool = True):
    """
    신호 기록 저장
    sent=True: 텔레그램 전송됨
    sent=False: 쿨다운으로 차단됨 (관찰용 기록)
    """
    try:
        get_client().table("signal_log").insert({
            "exchange":  result["exchange"],
            "symbol":    result["symbol"],
            "direction": direction,
            "cvd_pct":   result["cvd_pct"],
            "oi_pct":    result["oi_pct"],
            "vol_pct":   result["vol_pct"],
            "price":     result["price"],
            "diagnosis": result["diagnosis"],
            "date_kst":  str(today_kst()),
            "sent":      sent,
        }).execute()
    except Exception as e:
        logger.error(f"[DB] signal_log insert 실패: {e}")


# ── diamond_signals ───────────────────────
async def insert_diamond(symbol: str, direction: str, price: float):
    """TradingView 웹훅 다이아 신호 저장"""
    try:
        get_client().table("diamond_signals").insert({
            "symbol":    symbol,
            "direction": direction,    # 'up' | 'down'
            "price":     price,
            "timeframe": "4H",
            "is_active": True,
        }).execute()
        logger.info(f"[DB] 다이아 저장: {symbol} {direction} ${price}")
    except Exception as e:
        logger.error(f"[DB] diamond insert 실패: {e}")


async def get_active_diamonds() -> dict:
    """
    현재 활성 다이아 조회
    반환: {"SOLUSDT": {"direction": "up", "received_at": ..., "price": ...}}
    """
    try:
        res = get_client().table("diamond_signals")\
            .select("symbol, direction, price, received_at")\
            .eq("is_active", True)\
            .execute()
        result = {}
        for row in res.data:
            result[row["symbol"]] = {
                "direction":   row["direction"],
                "received_at": row["received_at"],
                "price":       row["price"],
            }
        return result
    except Exception as e:
        logger.error(f"[DB] diamond 조회 실패: {e}")
        return {}


# ── Preload ───────────────────────────────
async def preload_history():
    """
    서버 시작 시 Supabase candle_data에서 최근 데이터를 불러와
    state.py의 히스토리에 주입 → 워밍업 없이 즉시 정상 작동

    vol_history  : 최근 96봉 (24시간)
    oi_history   : 최근 192봉 (48시간) — oi_chg 값
    cvd_history  : 최근 10봉 (2.5시간) — cvd_delta 값
    """
    import core.state as state

    logger.info("[Preload] Supabase에서 히스토리 로딩 시작...")

    try:
        # 가장 많이 필요한 192봉 기준으로 조회
        res = get_client().table("candle_data")\
            .select("exchange, symbol, cvd_delta, oi_chg, vol_candle, price")\
            .eq("timeframe", "15m")\
            .order("ts", desc=True)\
            .limit(19200)\
            .execute()

        if not res.data:
            logger.warning("[Preload] DB에 데이터 없음 — 워밍업 필요")
            return

        # exchange+symbol별로 그룹핑 (최신순으로 왔으므로 reverse해서 오래된 것부터)
        from collections import defaultdict
        grouped: dict[tuple, list] = defaultdict(list)
        for row in reversed(res.data):
            key = (row["exchange"], row["symbol"])
            grouped[key].append(row)

        injected = 0
        for (exchange, symbol), rows in grouped.items():
            s = state._state[exchange][symbol]

            # vol_history: vol_candle 최근 96봉
            vol_vals = [r["vol_candle"] for r in rows if r.get("vol_candle") is not None]
            if vol_vals:
                s.vol_history = vol_vals[-96:]

            # oi_history: oi_chg 최근 192봉
            oi_vals = [r["oi_chg"] for r in rows if r.get("oi_chg") is not None]
            if oi_vals:
                s.oi_history = oi_vals[-192:]

            # cvd_history: cvd_delta 최근 10봉
            cvd_vals = [r["cvd_delta"] for r in rows if r.get("cvd_delta") is not None]
            if cvd_vals:
                s.cvd_history = cvd_vals[-20:]

            # price_history: 최근 20봉의 마감가
            price_vals = [r["price"] for r in rows if r.get("price") is not None]
            if price_vals:
                s.price_history = price_vals[-20:]

            injected += 1

        logger.info(f"[Preload] 완료 — {injected}개 심볼 히스토리 복원")

    except Exception as e:
        logger.error(f"[Preload] 실패: {e} — 워밍업 모드로 계속 진행")


# ── 롤링 딜리트 ───────────────────────────
async def run_cleanup():
    """1시간마다 호출 - 오래된 데이터 정리"""
    try:
        get_client().rpc("run_cleanup").execute()
        logger.info("[DB] 롤링 딜리트 완료")
    except Exception as e:
        logger.error(f"[DB] cleanup 실패: {e}")

async def refresh_ticker_counts():
    """ticker_counts materialized view 갱신 (15분 사이클 끝에 호출)"""
    try:
        get_client().rpc("refresh_ticker_counts").execute()
        logger.info("[DB] ticker_counts refresh 완료")
    except Exception as e:
        logger.error(f"[DB] ticker_counts refresh 실패: {e}")
