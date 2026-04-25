# ══════════════════════════════════════════
#  webhook/server.py
#  TradingView 4H 다이아 웹훅 수신
#  FastAPI 경량 서버
# ══════════════════════════════════════════

import logging
from fastapi import FastAPI, Request, HTTPException
from config import WEBHOOK_SECRET, WEBHOOK_PORT
from db.supabase import insert_diamond

logger = logging.getLogger(__name__)
app = FastAPI()


@app.post("/webhook/diamond")
async def diamond_webhook(request: Request):
    """
    TradingView Alert Message 포맷:
    {
        "secret":    "your_webhook_secret",
        "symbol":    "SOLUSDT",
        "direction": "up",
        "price":     142.3
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
    direction = data.get("direction", "").lower()   # "up" | "down"
    price     = float(data.get("price", 0))

    if not symbol or direction not in ("up", "down"):
        raise HTTPException(status_code=400, detail="Invalid payload")

    await insert_diamond(symbol, direction, price)
    logger.info(f"[Webhook] 다이아 수신: {symbol} {direction} ${price}")

    return {"status": "ok", "symbol": symbol, "direction": direction}


@app.get("/health")
async def health():
    return {"status": "ok"}
