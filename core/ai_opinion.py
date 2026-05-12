# ══════════════════════════════════════════
#  core/ai_opinion.py
#  매매일지 AI 의견 자동 생성
#  진입 시 스캐너 데이터 + 진입 정보 → Anthropic API → ai_opinion 텍스트
# ══════════════════════════════════════════

import logging
import json
from anthropic import AsyncAnthropic

from config import ANTHROPIC_API_KEY, AI_MODEL

logger = logging.getLogger(__name__)

_client: AsyncAnthropic = None


def _get_client() -> AsyncAnthropic | None:
    global _client
    if not ANTHROPIC_API_KEY:
        return None
    if _client is None:
        _client = AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
    return _client


SYSTEM_PROMPT = """당신은 암호화폐 트레이더의 매매일지에 진단 의견을 다는 분석가입니다.

사용자가 진입한 거래에 대해 **스캐너 데이터**(15분봉 + 4시간봉의 CVD/OI/거래량/가격 추세)와 **진입 정보**를 받습니다.

당신의 역할:
1. 진입한 방향(LONG/SHORT)이 스캐너 데이터와 정합성이 있는지 평가
2. 15분봉과 4시간봉이 같은 방향인지(동의), 반대인지(상충) 분석
3. 진입 시점의 위험 요소나 강점을 한국어로 객관적으로 짚어줌

규칙:
- **3~5문장으로 간결하게** 작성. 매매일지에서 빠르게 읽을 수 있도록.
- 톤: 객관적이고 분석적. 칭찬/응원하지 말고, 비판도 하지 말고, **사실 기반 진단**만.
- "진입가 좋다/나쁘다" 같은 단정적 표현 X. 데이터가 보여주는 신호를 설명.
- 마지막에 한 줄로 **🟢 정합 / 🟡 혼합 / 🔴 역행** 중 하나로 종합 평가.

예시 출력:

"""


def _build_user_prompt(trade: dict, snapshot: dict) -> str:
    """
    trade: trade_journal row 일부
        symbol, direction, entry_price, entry_amount_usd, leverage
    snapshot: scanner_snapshot JSONB
        15m: {ts, diagnosis, cvd_pct, oi_pct, vol_pct, price_chg, ...}
        4h:  {ts, diagnosis, cvd_pct, oi_pct, vol_pct, price_chg, ...}
    """
    import time
    s15 = (snapshot or {}).get("15m") or {}
    s4h = (snapshot or {}).get("4h")  or {}

    def fmt_age(ts) -> str:
        """unix timestamp → '20분 전' 같은 텍스트"""
        if not ts:
            return "시각 미상"
        try:
            ts_int = int(ts)
            diff_min = max(0, int((time.time() - ts_int) / 60))
            if diff_min < 60:
                return f"{diff_min}분 전 마감"
            diff_hour = diff_min // 60
            rem_min = diff_min % 60
            if rem_min == 0:
                return f"{diff_hour}시간 전 마감"
            return f"{diff_hour}시간 {rem_min}분 전 마감"
        except (ValueError, TypeError):
            return "시각 미상"

    def fmt_tf(name: str, d: dict) -> str:
        if not d:
            return f"[{name}] 데이터 없음"
        age = fmt_age(d.get("ts"))
        return (
            f"[{name}] (마지막 봉: {age}) "
            f"진단={d.get('diagnosis','—')}, "
            f"CVD={d.get('cvd_pct','—')}%, "
            f"OI={d.get('oi_pct','—')}%, "
            f"Vol={d.get('vol_pct','—')}%, "
            f"가격변화={d.get('price_chg','—')}%"
        )

    return (
        f"=== 진입 정보 ===\n"
        f"심볼: {trade.get('symbol')}\n"
        f"방향: {trade.get('direction')}\n"
        f"진입가: {trade.get('entry_price')}\n"
        f"마진: ${trade.get('entry_amount_usd')}\n"
        f"레버리지: {trade.get('leverage')}x\n"
        f"\n"
        f"=== 스캐너 스냅샷 (해당 종목) ===\n"
        f"{fmt_tf('15분봉', s15)}\n"
        f"{fmt_tf('4시간봉', s4h)}\n"
        f"\n"
        f"※ 데이터 신선도 참고: 15분봉은 0~14분 전, 4시간봉은 0~3시간 59분 전 마감된 봉의 결과입니다.\n"
        f"마감 시점이 오래됐을수록 현재 시장 상황과 차이가 클 수 있으니 진단에 반영해주세요.\n"
        f"\n"
        f"위 데이터로 진입 정합성을 진단해주세요."
    )


async def generate_opinion(trade: dict, snapshot: dict | None) -> str | None:
    """
    AI 의견 생성. 실패하면 None 반환 (시스템 동작에 영향 X)
    snapshot이 None이면 진단 skip
    """
    client = _get_client()
    if not client:
        logger.warning("[AI] ANTHROPIC_API_KEY 없음 — AI 의견 생성 skip")
        return None

    if not snapshot:
        logger.warning(f"[AI] 스캐너 스냅샷 없음 — {trade.get('symbol')} skip")
        return None

    try:
        prompt = _build_user_prompt(trade, snapshot)
        msg = await client.messages.create(
            model=AI_MODEL,
            max_tokens=400,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        # 응답에서 텍스트만 추출
        if msg.content and len(msg.content) > 0:
            text = "".join(
                block.text for block in msg.content
                if hasattr(block, "text")
            ).strip()
            if text:
                logger.info(f"[AI] 의견 생성: {trade.get('symbol')} ({len(text)}자)")
                return text
        logger.warning(f"[AI] 빈 응답 — {trade.get('symbol')}")
        return None
    except Exception as e:
        logger.error(f"[AI] 호출 실패 {trade.get('symbol')}: {e}")
        return None
