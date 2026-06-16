# ══════════════════════════════════════════
#  core/imbalance_resolver.py  –  임밸런스 신호 1:2 결과 판정
#
#  imbalance_signals 테이블의 result='PENDING' 행을 판정.
#  진입가(close) 기준 ±1% 손절 / ±2% 익절 (1:2 고정 RR).
#  신호 후 1H봉 high/low 순회 → 익절·손절 먼저 닿은 쪽으로 판정.
#    SUCCESS    : 익절선 먼저 도달
#    FAIL       : 손절선 먼저 도달 (한 봉에 둘 다 닿으면 보수적으로 FAIL)
#    UNRESOLVED : 48시간 내 둘 다 미도달
#
#  대상: imb_type in ('BUY','SELL') 만. BOTH/NONE 제외.
#  방향: BUY→LONG, SELL→SHORT.
#  15분마다 tracker_loop 사이클에 얹어 실행 (별도 루프).
# ══════════════════════════════════════════

import asyncio
import logging
from datetime import datetime, timedelta, timezone
import requests
from db.supabase import get_client

logger = logging.getLogger(__name__)

# ── 판정 파라미터 ──────────────────────────
SL_PCT      = 1.0    # 손절 -1%
TP_PCT      = 2.0    # 익절 +2% (1:2 고정)
CUTOFF_HRS  = 48     # 48시간 내 미해소 → UNRESOLVED
BTC_INST    = "BTC-USDT-SWAP"   # OKX 무기한 (판정 가격 출처)


# ── 1H봉 OHLC 조회 (신호 시점부터 커버) ─────
def fetch_1h_candles(inst: str = BTC_INST, limit: int = 100) -> list[tuple] | None:
    """OKX 1H봉을 시간 오름차순으로 반환. [(ts_ms, high, low, close), ...]
    limit 100 → 약 4일치. 48시간 컷오프 충분히 커버.
    OKX 응답은 최신→과거 역순이라 reversed로 시간순 정렬."""
    try:
        url = f"https://www.okx.com/api/v5/market/candles?instId={inst}&bar=1H&limit={limit}"
        resp = requests.get(url, timeout=8)
        raw = (resp.json() or {}).get("data") or []
        if not raw:
            return None
        candles = []
        for c in reversed(raw):
            candles.append((
                int(c[0]),     # ts_ms
                float(c[2]),   # high
                float(c[3]),   # low
                float(c[4]),   # close
            ))
        return candles
    except Exception as e:
        logger.warning(f"[imb-resolver] 1H candles 조회 실패: {e}")
        return None


# ── 단일 신호 판정 ─────────────────────────
def resolve_one(row: dict, candles: list) -> bool:
    """PENDING 신호 1개 판정. 결과 확정 시 result 업데이트, 미확정이면 그대로 둠.
    return: 업데이트 했으면 True."""
    imb_type = row.get("imb_type")
    if imb_type not in ("BUY", "SELL"):
        return False  # BOTH/NONE 안전 가드 (쿼리에서 걸러지지만 이중 방어)

    entry = row.get("close")
    if entry is None:
        return False
    entry = float(entry)

    direction = "LONG" if imb_type == "BUY" else "SHORT"

    # 손절/익절 가격선
    if direction == "LONG":
        sl_price = entry * (1 - SL_PCT / 100)
        tp_price = entry * (1 + TP_PCT / 100)
    else:
        sl_price = entry * (1 + SL_PCT / 100)
        tp_price = entry * (1 - TP_PCT / 100)

    # 신호 봉 시각 (UTC) → 진입 봉 다음 봉부터 판정 (거짓 청산 방지)
    bar_time = datetime.fromisoformat(row["bar_time"].replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    elapsed_hrs = (now - bar_time).total_seconds() / 3600

    # 진입 봉(신호 봉)이 속한 1H봉 다음 봉부터 — 진입 시점 이전/당시 가격으로 거짓 판정 방지
    entry_bar_ms = (int(bar_time.timestamp() * 1000) // 3600000 + 1) * 3600000

    result = None
    for ts_ms, high, low, close in candles:
        if ts_ms < entry_bar_ms:
            continue  # 진입 봉 및 이전 봉 스킵

        if direction == "LONG":
            tp_hit = high >= tp_price
            sl_hit = low  <= sl_price
        else:
            tp_hit = low  <= tp_price
            sl_hit = high >= sl_price

        # 한 봉에 둘 다 닿으면 보수적으로 FAIL (손절 먼저 가정)
        if sl_hit and tp_hit:
            result = "FAIL"
            break
        if sl_hit:
            result = "FAIL"
            break
        if tp_hit:
            result = "SUCCESS"
            break

    # 결과 미확정 + 48시간 경과 → UNRESOLVED
    if result is None and elapsed_hrs >= CUTOFF_HRS:
        result = "UNRESOLVED"

    if result is None:
        return False  # 아직 진행 중 — PENDING 유지

    try:
        get_client().table("imbalance_signals").update({
            "result":      result,
            "resolved_at": now.isoformat(),
        }).eq("id", row["id"]).execute()
        logger.info(f"[imb-resolver] {row['id']} {imb_type} → {result} (entry {entry})")
        return True
    except Exception as e:
        logger.error(f"[imb-resolver] {row.get('id')} 업데이트 실패: {e}")
        return False


# ── 전체 PENDING 판정 사이클 ───────────────
def resolve_all_pending():
    """result='PENDING' + imb_type in (BUY,SELL) 행 전체 판정."""
    sb = get_client()
    try:
        res = (
            sb.table("imbalance_signals")
            .select("*")
            .eq("result", "PENDING")
            .in_("imb_type", ["BUY", "SELL"])
            .order("bar_time", desc=False)
            .execute()
        )
        rows = res.data or []
    except Exception as e:
        logger.error(f"[imb-resolver] PENDING 조회 실패: {e}")
        return

    if not rows:
        logger.info("[imb-resolver] 판정 대기 신호 없음")
        return

    # BTC 1H봉 1회 조회 → 모든 행 공유
    candles = fetch_1h_candles()
    if not candles:
        logger.warning("[imb-resolver] 1H candles 없음 — 이번 사이클 스킵")
        return

    resolved = 0
    for r in rows:
        if resolve_one(r, candles):
            resolved += 1

    logger.info(f"[imb-resolver] 사이클 완료 — {len(rows)}건 검토, {resolved}건 확정")


# ── 백그라운드 루프 ─────────────────────────
async def imbalance_resolver_loop():
    """15분마다 PENDING 신호 판정."""
    logger.info("[imb-resolver] 임밸런스 판정 워커 시작")
    await asyncio.sleep(120)  # 부팅 후 2분 대기 (다른 워커 안정화 후)

    while True:
        try:
            resolve_all_pending()
        except Exception as e:
            logger.error(f"[imb-resolver] 사이클 실패: {e}", exc_info=True)

        # 다음 15분봉 마감 + 40초 후
        now = datetime.now(timezone.utc)
        next_quarter = (now.minute // 15 + 1) * 15
        if next_quarter >= 60:
            next_run = now.replace(minute=0, second=40, microsecond=0) + timedelta(hours=1)
        else:
            next_run = now.replace(minute=next_quarter, second=40, microsecond=0)
        sleep_sec = (next_run - now).total_seconds()
        await asyncio.sleep(sleep_sec)
