# ══════════════════════════════════════════
#  exchanges/okx_private.py
#  OKX private WS — 주문/포지션 자동 감지
#  진입 → trade_journal INSERT
#  청산 → trade_journal UPDATE
# ══════════════════════════════════════════

import asyncio
import json
import hmac
import base64
import hashlib
import logging
import time
from datetime import datetime, timezone
import websockets

from config import OKX_API_KEY, OKX_API_SECRET, OKX_PASSPHRASE
from db.supabase import (
    insert_trade_open,
    update_trade_close,
    fetch_latest_scanner_state,
)

logger = logging.getLogger(__name__)

EXCHANGE = "okx"
WS_PRIVATE_URL = "wss://ws.okx.com:8443/ws/v5/private"


def _sign(timestamp: str, method: str, request_path: str, secret: str) -> str:
    """OKX WS 인증 서명 생성"""
    msg = f"{timestamp}{method}{request_path}"
    mac = hmac.new(
        secret.encode("utf-8"),
        msg.encode("utf-8"),
        hashlib.sha256,
    )
    return base64.b64encode(mac.digest()).decode("utf-8")


def _build_login_args() -> dict:
    """로그인 메시지 생성"""
    timestamp = str(time.time())
    sign = _sign(timestamp, "GET", "/users/self/verify", OKX_API_SECRET)
    return {
        "op": "login",
        "args": [{
            "apiKey":     OKX_API_KEY,
            "passphrase": OKX_PASSPHRASE,
            "timestamp":  timestamp,
            "sign":       sign,
        }],
    }


def _calc_pnl(entry: float, exit_: float, leverage: float, amount: float, direction: str) -> tuple[float, float]:
    """프론트의 calcPnl과 동일한 공식"""
    if entry <= 0 or amount <= 0:
        return 0.0, 0.0
    dir_mult = 1 if direction == "LONG" else -1
    price_move = (exit_ - entry) / entry
    pnl_pct = price_move * leverage * 100 * dir_mult
    pnl_usd = price_move * leverage * amount * dir_mult
    return pnl_pct, pnl_usd


# 메모리 캐시: 같은 포지션 상태 반복 push 무시
_pos_cache: dict = {}



def _norm(v) -> str:
    """OKX가 보내는 숫자 값 정규화 (precision 차이 흡수)"""
    try:
        return f"{float(v):.8f}"
    except (TypeError, ValueError):
        return str(v) if v is not None else ""


async def _handle_position_msg(positions: list):
    """
    positions 채널 메시지 처리
    OKX positions push: 포지션 변화(open/close/update)
    같은 상태 push는 캐시로 무시 (5초마다 heartbeat 방지)
    """
    # 디버그: 들어오는 모든 push 핵심 필드를 한 줄로 (캐시 hit 여부와 무관하게)
    if positions:
        summary = " | ".join([
            f"{p.get('instId','?')}:{p.get('posSide','?')}:pos={p.get('pos','?')}:avg={p.get('avgPx','?')}"
            for p in positions
        ])
        logger.info(f"[OKX-Private] 📥 push ({len(positions)}건) {summary}")

    for pos in positions:
        try:
            inst_id = pos.get("instId", "")
            pos_id  = pos.get("posId", "")

            # ── 캐시 체크: 같은 (pos_id, pos_size) 조합이면 스킵 ──
            cache_key = pos_id
            cache_val = (_norm(pos.get("pos")), _norm(pos.get("avgPx")))
            cached = _pos_cache.get(cache_key)
            if cached == cache_val:
                continue
            _pos_cache[cache_key] = cache_val

            pos_side_raw = pos.get("posSide", "").lower()  # 'long'|'short'|'net'
            pos_size = float(pos.get("pos") or 0)          # 계약 수 (음수=숏 가능)
            avg_px   = float(pos.get("avgPx")  or 0)
            lever    = float(pos.get("lever")  or 1)
            margin   = float(pos.get("margin") or 0)
            mgn_mode = pos.get("mgnMode", "")              # cross|isolated
            upl      = float(pos.get("upl") or 0)          # unrealized pnl
            
            if not inst_id or not pos_id:
                continue

            # 방향 판정
            # posSide=long/short → 그대로
            # posSide=net이면 pos 부호로 판단 (양수=롱, 음수=숏)
            if pos_side_raw == "long":
                direction = "LONG"
            elif pos_side_raw == "short":
                direction = "SHORT"
            elif pos_size > 0:
                direction = "LONG"
            elif pos_size < 0:
                direction = "SHORT"
            else:
                # pos_size=0 → 청산 신호 (아래 처리)
                direction = None

            # ── 청산 (pos_size == 0) ──
            if abs(pos_size) < 1e-12:
                # 1차: OKX push에서 가격 추출
                close_px = float(pos.get("markPx") or pos.get("last") or 0)

                # 2차 폴백: candle_data에서 최신 가격
                if close_px <= 0 and inst_id:
                    try:
                        from db.supabase import get_client
                        res = get_client().table("candle_data")\
                            .select("price")\
                            .eq("exchange", EXCHANGE)\
                            .eq("symbol", inst_id)\
                            .eq("timeframe", "15m")\
                            .order("ts", desc=True)\
                            .limit(1)\
                            .execute()
                        if res.data and res.data[0].get("price"):
                            close_px = float(res.data[0]["price"])
                            logger.info(f"[OKX-Private] 청산가 폴백 (candle_data): {inst_id} → {close_px}")
                    except Exception as e:
                        logger.warning(f"[OKX-Private] 청산가 폴백 실패: {e}")

                if close_px <= 0:
                    logger.warning(f"[OKX-Private] 청산 가격 없음 (폴백도 실패) — pos_id={pos_id}, inst_id={inst_id}")
                    continue

                await _close_position_from_db(pos_id, close_px)
                _pos_cache.pop(pos_id, None)
                continue

            # ── 진입 또는 보유 중 (pos_size != 0) ──
            if avg_px <= 0 or margin <= 0:
                continue

            # OKX margin은 USD 단위로 들어옴 (격리/교차 모두)
            # entry_amount_usd = margin (사용자가 건 증거금)
            scanner_snap = await fetch_latest_scanner_state(EXCHANGE, inst_id)

            await insert_trade_open({
                "symbol":           inst_id,
                "exchange":         EXCHANGE,
                "direction":        direction,
                "entry_price":      avg_px,
                "entry_amount_usd": margin,
                "leverage":         lever,
                "scanner_snapshot": scanner_snap,
                "ext_pos_id":       pos_id,
            })

        except Exception as e:
            logger.error(f"[OKX-Private] 포지션 메시지 파싱 오류: {e}")


async def _close_position_from_db(ext_pos_id: str, close_px: float):
    """
    청산 — DB에서 진입 정보 가져와 PNL 재계산 후 update
    """
    from db.supabase import get_client
    try:
        res = get_client().table("trade_journal")\
            .select("entry_price, entry_amount_usd, leverage, direction")\
            .eq("ext_pos_id", ext_pos_id)\
            .eq("status", "open")\
            .limit(1)\
            .execute()
        if not res.data:
            logger.warning(f"[OKX-Private] 청산 매칭 실패 — ext_pos_id={ext_pos_id}")
            return
        row = res.data[0]
        pnl_pct, pnl_usd = _calc_pnl(
            float(row["entry_price"]),
            close_px,
            float(row.get("leverage") or 1),
            float(row.get("entry_amount_usd") or 0),
            row["direction"],
        )
        await update_trade_close(ext_pos_id, close_px, pnl_pct, pnl_usd)
    except Exception as e:
        logger.error(f"[OKX-Private] _close_position_from_db 실패: {e}")


async def _ws_loop():
    """Private WS 메인 루프 — 끊어지면 5초 후 재연결"""
    if not (OKX_API_KEY and OKX_API_SECRET and OKX_PASSPHRASE):
        logger.warning("[OKX-Private] 인증 정보 누락 — private WS 비활성화")
        return

    while True:
        try:
            logger.info("[OKX-Private] WS 연결 중...")
            async with websockets.connect(WS_PRIVATE_URL, ping_interval=20) as ws:
                # 1) 로그인
                await ws.send(json.dumps(_build_login_args()))
                login_resp = await asyncio.wait_for(ws.recv(), timeout=10)
                login_data = json.loads(login_resp)
                if login_data.get("event") != "login" or login_data.get("code") != "0":
                    logger.error(f"[OKX-Private] 로그인 실패: {login_data}")
                    await asyncio.sleep(15)
                    continue
                logger.info("[OKX-Private] 로그인 성공")

                # 2) positions 채널 구독 (모든 instType, 전체 instId)
                await ws.send(json.dumps({
                    "op": "subscribe",
                    "args": [{"channel": "positions", "instType": "ANY"}],
                }))
                logger.info("[OKX-Private] positions 채널 구독 요청")

                # 3) 메시지 수신 루프
                async for raw in ws:
                    try:
                        if raw == "pong":
                            continue
                        msg = json.loads(raw)
                        evt = msg.get("event")
                        if evt == "subscribe":
                            logger.info(f"[OKX-Private] 구독 확인: {msg.get('arg')}")
                            continue
                        if evt == "error":
                            logger.error(f"[OKX-Private] WS 에러: {msg}")
                            continue
                        # 데이터 메시지
                        arg = msg.get("arg", {})
                        if arg.get("channel") == "positions":
                            data = msg.get("data", [])
                            if data:
                                await _handle_position_msg(data)
                    except Exception as e:
                        logger.error(f"[OKX-Private] 메시지 처리 오류: {e}")

        except Exception as e:
            logger.error(f"[OKX-Private] WS 끊김: {e} — 5초 후 재연결")
            await asyncio.sleep(5)


async def run():
    """main.py에서 호출"""
    await _ws_loop()
