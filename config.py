# ══════════════════════════════════════════
#  config.py  –  전체 설정값
#  .env 파일에서 읽어옴
# ══════════════════════════════════════════

import os
from dotenv import load_dotenv

load_dotenv()

# ── Supabase ──────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# ── OKX Private API (read-only) ──────────
OKX_API_KEY    = os.getenv("OKX_API_KEY", "")
OKX_API_SECRET = os.getenv("OKX_API_SECRET", "")
OKX_PASSPHRASE = os.getenv("OKX_PASSPHRASE", "")

# ── Anthropic API (AI 의견 생성) ─────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
AI_MODEL          = os.getenv("AI_MODEL", "claude-haiku-4-5-20251001")

# ── 텔레그램 ──────────────────────────────
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ── 웹훅 ──────────────────────────────────
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
WEBHOOK_PORT   = int(os.getenv("WEBHOOK_PORT", "8000"))

# ── 분석 기준 ─────────────────────────────
CANDLE_MIN     = 15       # 15분봉
VOL_WINDOW     = 96       # 거래량 평균 기준 봉 수 (24시간)
OI_WINDOW      = 192      # OI 평균 기준 봉 수 (48시간)
CVD_WINDOW     = 10       # CVD 기울기 기준 봉 수 (150분)
OI_POLL_SEC    = 30       # OI REST 폴링 주기 (초)
CLEANUP_HOUR   = 1        # 롤링 딜리트 주기 (시간)

# ── 심볼 선정 ─────────────────────────────
TOP_N_SYMBOLS      = 100           # 거래소별 상위 N개
MIN_QUOTE_VOL      = 10_000_000    # 최소 24시간 거래량 ($10M)
SYMBOL_REFRESH_MIN = 15            # 심볼 리스트 갱신 주기 (분)

# ── 텔레그램 알림 조건 ────────────────────
# 하루 1번 제한은 Python 메모리 + signal_log DB 병행
ALERT_COOLDOWN_DAYS = 1

# ── 거래소 엔드포인트 ─────────────────────
BINANCE = {
    "ws":       "wss://fstream.binance.com/stream?streams=",
    "rest_oi":  "https://fapi.binance.com/fapi/v1/openInterest",
    "rest_top": "https://fapi.binance.com/fapi/v1/ticker/24hr",
}

OKX = {
    "ws":       "wss://ws.okx.com:8443/ws/v5/public",
    "rest_oi":  "https://www.okx.com/api/v5/public/open-interest",
    "rest_top": "https://www.okx.com/api/v5/market/tickers?instType=SWAP",
}

BYBIT = {
    "ws":       "wss://stream.bybit.com/v5/public/linear",
    "rest_oi":  "https://api.bybit.com/v5/market/open-interest",
    "rest_top": "https://api.bybit.com/v5/market/tickers?category=linear",
}
