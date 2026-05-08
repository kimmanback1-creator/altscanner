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
            "price_chg_24h": result.get("price_chg_24h", 0.0),
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

async def cleanup_liquidations():
    """24시간 지난 청산 이벤트 삭제"""
    try:
        get_client().rpc("cleanup_liquidation_events").execute()
        logger.info("[DB] 청산 이벤트 cleanup 완료")
    except Exception as e:
        logger.error(f"[DB] 청산 cleanup 실패: {e}")


# ── trade_journal 자동 기록 (OKX private WS) ──────────
async def insert_trade_open(payload: dict):
    """
    포지션 진입 감지 → trade_journal에 INSERT
    payload: {
        "symbol": "BTC-USDT-SWAP", "exchange": "okx",
        "direction": "LONG"|"SHORT",
        "entry_price": float, "entry_amount_usd": float, "leverage": float,
        "scanner_snapshot": dict | None,
        "ext_pos_id": str | None,  # OKX 포지션 ID (중복 방지용)
    }
    """
    try:
        # 같은 외부 포지션 ID + 같은 방향으로 이미 open이면 skip (중복 INSERT 방지)
        # direction이 다르면 옛 row는 청산 처리하고 새 진입으로 인정
        ext_id = payload.get("ext_pos_id")
        new_direction = payload.get("direction")
        if ext_id:
            res = get_client().table("trade_journal")\
                .select("id, direction")\
                .eq("ext_pos_id", ext_id)\
                .eq("status", "open")\
                .limit(1)\
                .execute()
            if res.data:
                old_row = res.data[0]
                if old_row["direction"] == new_direction:
                    logger.info(f"[DB] trade_journal 중복 skip — ext_pos_id={ext_id}, direction={new_direction}")
                    return old_row["id"]
                else:
                    # 방향 전환 — 옛 row를 'closed' 상태로 강제 마무리
                    # (청산가는 새 진입가로 임시 사용 — 더 정확한 건 별도 보정 필요)
                    logger.warning(f"[DB] 방향 전환 감지 — 옛 {old_row['direction']} row 강제 마무리, 새 {new_direction} 진입")
                    get_client().table("trade_journal")\
                        .update({
                            "status": "closed",
                            "closed_at": datetime.now(timezone.utc).isoformat(),
                            "exit_price": payload["entry_price"],  # 임시 — 사용자가 수정 가능
                        })\
                        .eq("id", old_row["id"])\
                        .execute()
                    # 그 후 새 row INSERT 진행

        row = {
            "symbol":    payload["symbol"],
            "exchange":  payload["exchange"],
            "direction": payload["direction"],
            "entry_price":      payload["entry_price"],
            "entry_amount_usd": payload["entry_amount_usd"],
            "leverage":         payload["leverage"],
            "source": "api",
            "status": "open",
            "scanner_snapshot": payload.get("scanner_snapshot"),
            "ext_pos_id":       payload.get("ext_pos_id"),
        }
        res = get_client().table("trade_journal").insert(row).execute()
        new_id = res.data[0]["id"] if res.data else None
        logger.info(f"[DB] trade_journal 진입 기록: {payload['symbol']} {payload['direction']} @ {payload['entry_price']}")
        return new_id
    except Exception as e:
        logger.error(f"[DB] trade_journal insert 실패: {e}")
        return None


async def update_trade_close(ext_pos_id: str, exit_price: float, pnl_pct: float, pnl_usd: float):
    """
    포지션 청산 감지 → trade_journal UPDATE
    """
    try:
        # ext_pos_id로 찾아서 청산 처리
        res = get_client().table("trade_journal")\
            .update({
                "exit_price": exit_price,
                "pnl_pct":    round(pnl_pct, 4),
                "pnl_usd":    round(pnl_usd, 2),
                "closed_at":  datetime.now(timezone.utc).isoformat(),
                "status":     "closed",
            })\
            .eq("ext_pos_id", ext_pos_id)\
            .eq("status", "open")\
            .execute()
        if res.data:
            logger.info(f"[DB] trade_journal 청산 기록: {ext_pos_id} @ {exit_price} ({pnl_pct:+.2f}%)")
        else:
            logger.warning(f"[DB] trade_journal 청산 매칭 실패 — ext_pos_id={ext_pos_id}")
    except Exception as e:
        logger.error(f"[DB] trade_journal update 실패: {e}")


async def update_ai_opinion(trade_id: str, ai_opinion: str):
    """trade_journal에 AI 의견 추가"""
    try:
        get_client().table("trade_journal")\
            .update({"ai_opinion": ai_opinion})\
            .eq("id", trade_id)\
            .execute()
        logger.info(f"[DB] AI 의견 저장: trade_id={trade_id}")
    except Exception as e:
        logger.error(f"[DB] AI 의견 저장 실패: {e}")


async def fetch_latest_scanner_state(exchange: str, symbol: str) -> dict | None:
    """
    백엔드용 스캐너 스냅샷 — 최근 15m + 4h candle 조회해서 JSON 반환
    프론트엔드 captureScannerSnapshot()의 백엔드 버전
    """
    try:
        # 15m
        res15 = get_client().table("candle_data")\
            .select("ts, diagnosis, cvd_pct, oi_pct, vol_pct, price, price_chg, price_chg_24h")\
            .eq("exchange", exchange)\
            .eq("symbol", symbol)\
            .eq("timeframe", "15m")\
            .order("ts", desc=True)\
            .limit(1)\
            .execute()

        # 4h
        res4h = get_client().table("candle_data")\
            .select("ts, diagnosis, cvd_pct, oi_pct, vol_pct, price_chg")\
            .eq("exchange", exchange)\
            .eq("symbol", symbol)\
            .eq("timeframe", "4h")\
            .order("ts", desc=True)\
            .limit(1)\
            .execute()

        d15 = res15.data[0] if res15.data else None
        d4h = res4h.data[0] if res4h.data else None

        if not d15 and not d4h:
            return None

        return {
            "captured_at":    datetime.now(timezone.utc).isoformat(),
            "matched_symbol": symbol,
            "source":         "backend",
            "15m":            d15,
            "4h":             d4h,
        }
    except Exception as e:
        logger.error(f"[DB] scanner snapshot 조회 실패: {e}")
        return None
        
# ── major_hourly ──────────────────────────
async def insert_major_hourly(snap: dict, ts: int):
    """BTC/ETH/SOL 1시간 데이터 저장 (진단 포함)"""
    try:
        get_client().table("major_hourly").insert({
            "ts":         ts,
            "ts_kst":     now_kst().isoformat(),
            "exchange":   snap["exchange"],
            "symbol":     snap["symbol"],
            "cvd_delta":  snap["cvd_delta"],
            "oi_chg":     snap["oi_chg"],
            "vol_candle": snap["vol_candle"],
            "price":      snap["price"],
            "price_chg":  snap["price_chg"],
            "diagnosis":  snap.get("diagnosis"),
            "cvd_pct":    snap.get("cvd_pct"),
            "oi_pct":     snap.get("oi_pct"),
            "vol_pct":    snap.get("vol_pct"),
        }).execute()
    except Exception as e:
        logger.error(f"[DB] major_hourly insert 실패 {snap['symbol']}: {e}")
async def cleanup_major_hourly_db():
    """7일 지난 메이저 데이터 삭제"""
    try:
        get_client().rpc("cleanup_major_hourly").execute()
        logger.info("[DB] major_hourly cleanup 완료")
    except Exception as e:
        logger.error(f"[DB] major_hourly cleanup 실패: {e}")
