# ══════════════════════════════════════════
#  notify/telegram.py  –  텔레그램 전송
# ══════════════════════════════════════════

import aiohttp
import logging
from config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)
URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"


async def send_message(text: str):
    payload = {
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       text,
        "parse_mode": "Markdown",
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                URL, json=payload,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error(f"[TG] 전송 실패: {resp.status} {body}")
                else:
                    logger.info(f"[TG] 전송 완료")
    except Exception as e:
        logger.error(f"[TG] 예외: {e}")
