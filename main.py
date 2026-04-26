# ══════════════════════════════════════════
#  main.py  –  진입점
#  수집 서버 + 웹훅 서버 동시 실행
# ══════════════════════════════════════════

import asyncio
import logging
import uvicorn

import exchanges.binance as binance
import exchanges.okx     as okx
import exchanges.bybit   as bybit
from core.candle   import candle_loop
from webhook.server import app
from db.supabase   import preload_history
from config        import WEBHOOK_PORT

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
    logger.info("  거래소: Binance / OKX / Bybit")
    logger.info("  기준: CVD + OI + 거래량 (15분봉)")
    logger.info("=" * 50)

    # 재시작해도 히스토리 유지 — gather 전에 완료되어야 함
    await preload_history()

    await asyncio.gather(
        binance.run(),       # Binance WS + OI
        okx.run(),           # OKX WS + OI
        bybit.run(),         # Bybit WS + OI
        candle_loop(),       # 15분봉 신호 판정
        run_webhook(),       # TradingView 웹훅 수신
    )


if __name__ == "__main__":
    asyncio.run(main())
