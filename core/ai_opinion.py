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
