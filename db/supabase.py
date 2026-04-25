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
    """15분봉 결과값 저장"""
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
            "price":     result["price"],
            "price_chg": result["price_chg"],
            "diagnosis": result["diagnosis"],
        }).execute()
    except Exception as e:
        logger.error(f"[DB] candle insert 실패 {result['symbol']}: {e}")


# ── signal_log ────────────────────────────
async def already_sent_today(exchange: str, symbol: str, direction: str) -> bool:
    """오늘 KST 기준 이미 보냈는지 확인"""
    try:
        res = get_client().table("signal_log")\
            .select("id")\
            .eq("exchange",  exchange)\
            .eq("symbol",    symbol)\
            .eq("direction", direction)\
            .eq("date_kst",  str(today_kst()))\
            .limit(1)\
            .execute()
        return len(res.data) > 0
    except Exception as e:
        logger.error(f"[DB] signal_log 조회 실패: {e}")
        return False


async def log_signal(result: dict, direction: str):
    """텔레그램 전송 기록 저장"""
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


# ── 롤링 딜리트 ───────────────────────────
async def run_cleanup():
    """1시간마다 호출 - 오래된 데이터 정리"""
    try:
        get_client().rpc("run_cleanup").execute()
        logger.info("[DB] 롤링 딜리트 완료")
    except Exception as e:
        logger.error(f"[DB] cleanup 실패: {e}")
