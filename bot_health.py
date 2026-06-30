# ══════════════════════════════════════════
#  core/bot_health.py
#  OKX 서브계정 웹훅 봇 헬스체크 (read-only)
#  - 서브계정 key로 orders-history(SWAP) 폴링
#  - 봇 살아있음/주문 성공·실패/연결 상태 판정
#  - bot_health(상태 스냅샷) + bot_orders(주문 이력) UPSERT
# ══════════════════════════════════════════

import hmac
import base64
import hashlib
import logging
import requests
from datetime import datetime, timezone

from config import OKX_SUB_API_KEY, OKX_SUB_API_SECRET, OKX_SUB_PASSPHRASE
from db.supabase import get_client

logger = logging.getLogger(__name__)

OKX_BASE = "https://www.okx.com"

# ── 봇 정의 (확장 시 여기에 추가) ──
BOTS = [
    {
        "bot_id":      "eth_p",
        "bot_label":   "ETH.P",
        "sub_account": "ETH.P",
        "inst_type":   "SWAP",
    },
]

# 최근 주문 몇 건까지 조회/판정에 쓸지
ORDER_LIMIT = 20
# warning 판정 임계: 최근 조회분 중 canceled 비율
CANCEL_WARN_RATIO = 0.4


def _iso_ts() -> str:
    """OKX REST 인증용 ISO8601 UTC 밀리초 timestamp"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") \
        + f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z"


def _sign(timestamp: str, method: str, request_path: str) -> str:
    """OKX REST 서명: timestamp + method + requestPath(+body) → HMAC-SHA256 → base64"""
    msg = f"{timestamp}{method}{request_path}"
    mac = hmac.new(
        OKX_SUB_API_SECRET.encode("utf-8"),
        msg.encode("utf-8"),
        hashlib.sha256,
    )
    return base64.b64encode(mac.digest()).decode("utf-8")


def _okx_get(request_path: str) -> tuple[bool, list | str]:
    """
    인증된 OKX REST GET 호출.
    반환: (성공여부, data 리스트 또는 에러 문자열)
    request_path는 쿼리스트링 포함 전체 경로 (서명에 그대로 들어감)
    """
    ts = _iso_ts()
    sign = _sign(ts, "GET", request_path)
    headers = {
        "OK-ACCESS-KEY":        OKX_SUB_API_KEY,
        "OK-ACCESS-SIGN":       sign,
        "OK-ACCESS-TIMESTAMP":  ts,
        "OK-ACCESS-PASSPHRASE": OKX_SUB_PASSPHRASE,
        "Content-Type":         "application/json",
    }
    try:
        resp = requests.get(OKX_BASE + request_path, headers=headers, timeout=10)
        body = resp.json()
        if body.get("code") == "0":
            return True, body.get("data", [])
        # OKX는 인증/권한 문제도 200 + code≠0 으로 줌
        return False, f"code={body.get('code')} msg={body.get('msg')}"
    except Exception as e:
        return False, f"request_error: {e}"


def _parse_ms(ms) -> datetime | None:
    """OKX ms 타임스탬프 문자열 → datetime(UTC)"""
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
    except (TypeError, ValueError):
        return None


def _upsert_orders(bot_id: str, orders: list) -> tuple[int, int, datetime | None]:
    """
    bot_orders에 주문 UPSERT.
    반환: (filled 건수, canceled 건수, 가장 최근 주문 cTime)
    """
    sb = get_client()
    filled = 0
    canceled = 0
    latest_order_at = None

    rows = []
    for o in orders:
        state = o.get("state", "")
        if state == "filled":
            filled += 1
        elif state == "canceled":
            canceled += 1

        c_time = _parse_ms(o.get("cTime"))
        u_time = _parse_ms(o.get("uTime"))
        if c_time and (latest_order_at is None or c_time > latest_order_at):
            latest_order_at = c_time

        rows.append({
            "ord_id":     o.get("ordId", ""),
            "bot_id":     bot_id,
            "inst_id":    o.get("instId", ""),
            "side":       o.get("side", ""),
            "pos_side":   o.get("posSide", ""),
            "ord_type":   o.get("ordType", ""),
            "state":      state,
            "fill_px":    _num(o.get("fillPx")),
            "fill_sz":    _num(o.get("fillSz")),
            "avg_px":     _num(o.get("avgPx")),
            "fee":        _num(o.get("fee")),
            "s_code":     o.get("sCode", "") or "",
            "s_msg":      o.get("sMsg", "") or "",
            "cl_ord_id":  o.get("clOrdId", "") or "",
            "created_at": c_time.isoformat() if c_time else None,
            "updated_at": u_time.isoformat() if u_time else None,
            "synced_at":  datetime.now(timezone.utc).isoformat(),
        })

    if rows:
        try:
            sb.table("bot_orders").upsert(rows, on_conflict="ord_id").execute()
        except Exception as e:
            logger.error(f"[bot_health] bot_orders upsert 실패 ({bot_id}): {e}")

    return filled, canceled, latest_order_at


def _num(v):
    """빈 문자열/None → None, 아니면 float"""
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _upsert_health(bot: dict, api_ok: bool, api_error: str,
                   filled: int, canceled: int, last_order_at: datetime | None):
    """bot_health 상태 스냅샷 UPSERT"""
    sb = get_client()
    now = datetime.now(timezone.utc)

    # 상태 판정
    if not api_ok:
        status = "down"
    else:
        total = filled + canceled
        if total > 0 and (canceled / total) >= CANCEL_WARN_RATIO:
            status = "warning"
        else:
            status = "active"

    row = {
        "bot_id":          bot["bot_id"],
        "bot_label":       bot["bot_label"],
        "sub_account":     bot["sub_account"],
        "status":          status,
        "api_ok":          api_ok,
        "api_error":       api_error or None,
        "last_order_at":   last_order_at.isoformat() if last_order_at else None,
        "last_check_at":   now.isoformat(),
        "recent_filled":   filled,
        "recent_canceled": canceled,
        "updated_at":      now.isoformat(),
    }
    try:
        sb.table("bot_health").upsert(row, on_conflict="bot_id").execute()
        logger.info(
            f"[bot_health] {bot['bot_label']} → {status} "
            f"(api_ok={api_ok}, filled={filled}, canceled={canceled})"
        )
    except Exception as e:
        logger.error(f"[bot_health] bot_health upsert 실패 ({bot['bot_id']}): {e}")


def _check_one_bot(bot: dict):
    """봇 1개 헬스체크"""
    path = f"/api/v5/trade/orders-history?instType={bot['inst_type']}&limit={ORDER_LIMIT}"
    api_ok, result = _okx_get(path)

    if not api_ok:
        # 연결/키/권한 실패 → down 으로 기록, 주문 갱신은 스킵
        logger.warning(f"[bot_health] {bot['bot_label']} API 실패: {result}")
        _upsert_health(bot, False, str(result), 0, 0, None)
        return

    orders = result  # list
    filled, canceled, last_order_at = _upsert_orders(bot["bot_id"], orders)
    _upsert_health(bot, True, "", filled, canceled, last_order_at)


def run_health_check():
    """모든 봇 헬스체크 1사이클 (tracker_loop 등에서 호출)"""
    if not (OKX_SUB_API_KEY and OKX_SUB_API_SECRET and OKX_SUB_PASSPHRASE):
        logger.warning("[bot_health] 서브계정 인증정보 누락 — 헬스체크 비활성화")
        return
    for bot in BOTS:
        try:
            _check_one_bot(bot)
        except Exception as e:
            logger.error(f"[bot_health] {bot.get('bot_id')} 체크 오류: {e}")
