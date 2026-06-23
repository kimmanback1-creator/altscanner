# ══════════════════════════════════════════
#  main.py  –  진입점
#  수집 서버 + 웹훅 서버 동시 실행
# ══════════════════════════════════════════

import asyncio
import logging
import uvicorn
# Binance는 Render Singapore IP throttle로 인해 비활성화
# import exchanges.binance as binance
import exchanges.okx     as okx
import exchanges.bybit   as bybit
import exchanges.okx_private as okx_private
from core.candle    import candle_loop
from core.evaluator import evaluator_loop
from core.recommendation import recommendation_loop
from core.tracker   import tracker_loop
from core.imbalance_resolver import imbalance_resolver_loop
from webhook.server import app
from db.supabase   import preload_history
from config        import WEBHOOK_PORT
from core.options_macro import options_macro_loop

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("main")


async def run_webhook():
    """FastAPI 웹훅 서버"""
    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=WEBHOOK_PORT,
        log_level="warning",
    )
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    logger.info("=" * 50)
    logger.info("  ALTSCANNER 시작")
    logger.info("  거래소: OKX / Bybit (Binance 비활성화)")
    logger.info("  기준: CVD + OI + 거래량 (15분봉)")
    logger.info("=" * 50)

    # 재시작해도 히스토리 유지 — gather 전에 완료되어야 함
    await preload_history()

    await asyncio.gather(
        okx.run(),           # OKX WS + OI
        bybit.run(),         # Bybit WS + OI
        okx_private.run(),   # OKX private WS — 자동 매매 기록
        candle_loop(),       # 15분봉 신호 판정
        run_webhook(),       # TradingView 웹훅 수신
        evaluator_loop(),    # setup_log_auto 자동 평가 (7일 후)
        recommendation_loop(),  # 자동 셋업 진입 추천 + Telegram 푸시
        tracker_loop(),      # 알림 7일 추적 (Phase 0 검증)
        imbalance_resolver_loop(),    # 임밸런스 신호 1:2 결과 판정
        options_macro_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
