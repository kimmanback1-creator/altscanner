# ══════════════════════════════════════════
#  core/scorer.py  –  신호 판정 + 상태 진단
# ══════════════════════════════════════════

from core.percentile import cvd_percentile, oi_percentile, vol_percentile, is_warmed_up


def diagnose(cvd_pct: float, oi_pct: float, price_chg: float, vol_pct: float) -> str:
    """
    CVD / OI / 가격 방향 조합으로 상태 진단 텍스트 반환
    """
    cvd_up   = cvd_pct > 0
    oi_up    = oi_pct > 0
    price_up = price_chg > 0
    vol_high = vol_pct >= 60

    vol_tag = " (거래량 동반)" if vol_high else ""

    if cvd_up and price_up and oi_up:
        return "건강한 상승" + vol_tag
    if not cvd_up and price_up and oi_up:
        return "하락 다이버전스"
    if cvd_up and price_up and not oi_up:
        return "숏스퀴즈" + vol_tag
    if not cvd_up and price_up and not oi_up:
        return "매수 소진 주의"
    if cvd_up and not price_up and oi_up:
        return "매집 가능성"
    if not cvd_up and not price_up and oi_up:
        return "건강한 하락" + vol_tag
    if cvd_up and not price_up and not oi_up:
        return "상승 다이버전스"
    if not cvd_up and not price_up and not oi_up:
        return "롱청산 하락" + vol_tag

    return "신호 불명확"


def calc_score(snap: dict) -> dict | None:
    """
    스냅샷 → 백분위 계산 → 결과 반환
    워밍업 미완료 시 None 반환
    """
    cvd_h  = snap["cvd_history"]
    oi_h   = snap["oi_history"]
    vol_h  = snap["vol_history"]

    # 워밍업 체크
    if not is_warmed_up(vol_h, oi_h, cvd_h):
        return None

    # 거래 없으면 스킵
    if snap["vol_candle"] == 0:
        return None

    cvd_pct = cvd_percentile(cvd_h)
    oi_pct  = oi_percentile(snap["oi_chg"], oi_h)
    vol_pct = vol_percentile(snap["vol_candle"], vol_h)

    diag = diagnose(cvd_pct, oi_pct, snap["price_chg"], vol_pct)

    return {
        "exchange":  snap["exchange"],
        "symbol":    snap["symbol"],
        "cvd_pct":   cvd_pct,
        "oi_pct":    oi_pct,
        "vol_pct":   vol_pct,
        "cvd_delta": snap["cvd_delta"],
        "oi_chg":    snap["oi_chg"],
        "vol_ratio": snap["vol_ratio"],
        "vol_candle": snap["vol_candle"],
        "price":     snap["price"],
        "price_chg": snap["price_chg"],
        "diagnosis": diag,
    }


def check_signal(result: dict, long_params: dict, short_params: dict) -> str | None:
    """
    결과값과 임계값 비교해서 LONG/SHORT/None 반환

    long_params / short_params = {cvd: float, oi: float, vol: float}
    """
    cvd = result["cvd_pct"]
    oi  = result["oi_pct"]
    vol = result["vol_pct"]

    # LONG: CVD/OI 양수, 임계값 이상
    if (cvd >= long_params["cvd"] and
        oi  >= long_params["oi"] and
        vol >= long_params["vol"]):
        return "LONG"

    # SHORT: CVD/OI 음수, 절대값 임계값 이상
    if (cvd <= -short_params["cvd"] and
        oi  <= -short_params["oi"] and
        vol >= short_params["vol"]):
        return "SHORT"

    return None


def format_telegram(result: dict, direction: str) -> str:
    """텔레그램 메시지 포맷"""
    emoji   = "🚀" if direction == "LONG" else "🔻"
    ex_map  = {"binance": "🟡 Binance", "okx": "⚫ OKX", "bybit": "🟠 Bybit"}
    ex_str  = ex_map.get(result["exchange"], result["exchange"])

    cvd_sign = "+" if result["cvd_pct"] >= 0 else ""
    oi_sign  = "+" if result["oi_pct"]  >= 0 else ""
    pc_sign  = "+" if result["price_chg"] >= 0 else ""

    return "\n".join([
        f"{emoji} *{result['symbol']}* — {direction}",
        f"{ex_str}  |  15분봉",
        f"━━━━━━━━━━━━━━━━━━",
        f"💰 가격:    ${result['price']:,.4f}  ({pc_sign}{result['price_chg']:.2f}%)",
        f"⚡ CVD:    {cvd_sign}{result['cvd_pct']:.1f}%",
        f"📈 OI:     {oi_sign}{result['oi_pct']:.1f}%",
        f"📊 거래량: {result['vol_pct']:.1f}%",
        f"🔍 진단:   {result['diagnosis']}",
        f"━━━━━━━━━━━━━━━━━━",
    ])
