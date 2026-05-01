# ══════════════════════════════════════════
#  webhook/server.py
#  TradingView 다이아 웹훅 수신 (4H ~ 1M 멀티 TF)
#  FastAPI 경량 서버
# ══════════════════════════════════════════

import logging
from fastapi import FastAPI, Request, HTTPException
from config import WEBHOOK_SECRET, WEBHOOK_PORT
from db.supabase import insert_diamond

logger = logging.getLogger(__name__)
app = FastAPI()

VALID_TFS = {"4H", "1D", "3D", "1W", "3W", "1M"}


@app.post("/webhook/diamond")
async def diamond_webhook(request: Request):
    """
    TradingView Alert payload:
    {
        "secret":    "your_webhook_secret",
        "symbol":    "BTC",        # BTC | ETH | USDT.D | TOTAL
        "timeframe": "1D",         # 4H | 1D | 3D | 1W | 3W | 1M
        "direction": "up",         # up | down
        "price":     70000.0
    }
    """
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # 시크릿 인증
    if WEBHOOK_SECRET and data.get("secret") != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    symbol    = data.get("symbol", "").upper()
    timeframe = data.get("timeframe", "").upper()
    direction = data.get("direction", "").lower()
    price     = float(data.get("price", 0))

    if not symbol or timeframe not in VALID_TFS or direction not in ("up", "down"):
        raise HTTPException(status_code=400, detail="Invalid payload")

    await insert_diamond(symbol, timeframe, direction, price)
    logger.info(f"[Webhook] 다이아: {symbol} {timeframe} {direction} ${price}")

    return {"status": "ok", "symbol": symbol, "timeframe": timeframe, "direction": direction}


@app.get("/health")
async def health():
    return {"status": "ok"}
