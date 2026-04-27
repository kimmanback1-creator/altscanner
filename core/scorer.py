# ══════════════════════════════════════════
#  core/scorer.py  –  추세 기반 진단
# ══════════════════════════════════════════

from core.percentile import cvd_percentile, oi_percentile, vol_percentile, is_warmed_up
from core.trend import trend_price, trend_cvd, trend_oi


def diagnose_v2(price_t: str, oi_t: str, cvd_t: str) -> str | None:
    """
    추세 조합 → 진단
    flat 끼면 None (진단 보류)
    
    참고: 블로그 4가지 표준 + 보조 4가지 = 8가지
    """
    if 'flat' in (price_t, oi_t, cvd_t):
        return None

    # 가격 상승 4가지
    if price_t == 'up' and oi_t == 'up' and cvd_t == 'up':
        return "신규 롱 진입"           # 건강한 상승
    if price_t == 'up' and oi_t == 'down' and cvd_t == 'up':
        return "숏커버 (숏스퀴즈)"
    if price_t == 'up' and oi_t == 'up' and cvd_t == 'down':
        return "하락 다이버전스"        # 가격↑인데 매도 우위 + 신규 숏 누적
    if price_t == 'up' and oi_t == 'down' and cvd_t == 'down':
        return "매수 소진 (천장 의심)"

    # 가격 하락 4가지
    if price_t == 'down' and oi_t == 'up' and cvd_t == 'down':
        return "신규 숏 진입"           # 건강한 하락
    if price_t == 'down' and oi_t == 'down' and cvd_t == 'down':
        return "롱 청산 (투매)"
    if price_t == 'down' and oi_t == 'up' and cvd_t == 'up':
        return "매집 가능성"             # 가격↓인데 매수 + 신규 포지션
    if price_t == 'down' and oi_t == 'down' and cvd_t == 'up':
        return "상승 다이버전스"        # 가격↓인데 매수 우위 + 청산

    return None


def calc_score(snap: dict) -> dict | None:
    """
    스냅샷 → 추세 분석 → 진단
    추세 판정 불가(flat) 시 None
    """
    cvd_h   = snap["cvd_history"]
    oi_h    = snap["oi_history"]
    vol_h   = snap["vol_history"]
    price_h = snap["price_history"]

    # 워밍업 체크 (현재는 항상 True 반환하도록 수정해놨음)
    if not is_warmed_up(vol_h, oi_h, cvd_h):
        return None

    # 거래 없으면 스킵
    if snap["vol_candle"] == 0:
        return None

    # ── 추세 판정 ──
    price_t = trend_price(price_h)
    oi_t    = trend_oi(oi_h)
    cvd_t   = trend_cvd(cvd_h)

    # ── 진단 (flat 끼면 None) ──
    diagnosis = diagnose_v2(price_t, oi_t, cvd_t)
    if diagnosis is None:
        return None  # 횡보 또는 데이터 부족

    # ── 백분위 (참고용으로 유지, 알림 조건엔 사용) ──
    cvd_pct = cvd_percentile(cvd_h)
    oi_pct  = oi_percentile(snap["oi_chg"], oi_h)
    vol_pct = vol_percentile(snap["vol_candle"], vol_h)

    return {
        "exchange":   snap["exchange"],
        "symbol":     snap["symbol"],
        "cvd_pct":    cvd_pct,
        "oi_pct":     oi_pct,
        "vol_pct":    vol_pct,
        "cvd_delta":  snap["cvd_delta"],
        "oi_chg":     snap["oi_chg"],
        "vol_ratio":  snap["vol_ratio"],
        "vol_candle": snap["vol_candle"],
        "price":      snap["price"],
        "price_chg":  snap["price_chg"],
        "diagnosis":  diagnosis,
        "price_trend": price_t,
        "cvd_trend":   cvd_t,
        "oi_trend":    oi_t,
    }


def check_signal(result: dict, long_params: dict, short_params: dict) -> str | None:
    """
    LONG/SHORT 판정
    추세 진단 + 백분위 임계값 결합
    """
    cvd = result["cvd_pct"]
    oi  = result["oi_pct"]
    vol = result["vol_pct"]
    diag = result["diagnosis"]

    # LONG: 가격↑ 진단 + percentile 임계값
    long_diags = ("신규 롱 진입", "숏커버 (숏스퀴즈)", "매집 가능성")
    if diag in long_diags:
        if (cvd >= long_params["cvd"] and
            oi  >= long_params["oi"] and
            vol >= long_params["vol"]):
            return "LONG"

    # SHORT: 가격↓ 진단 + percentile 임계값
    short_diags = ("신규 숏 진입", "롱 청산 (투매)", "매수 소진 (천장 의심)", "하락 다이버전스")
    if diag in short_diags:
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

    # 추세 화살표
    arrow = {'up': '↑', 'down': '↓', 'flat': '→'}
    p_arr = arrow.get(result.get("price_trend", "flat"))
    c_arr = arrow.get(result.get("cvd_trend",   "flat"))
    o_arr = arrow.get(result.get("oi_trend",    "flat"))

    return "\n".join([
        f"{emoji} *{result['symbol']}* — {direction}",
        f"{ex_str}  |  15분봉",
        f"━━━━━━━━━━━━━━━━━━",
        f"💰 가격:    ${result['price']:,.4f}  ({pc_sign}{result['price_chg']:.2f}%) {p_arr}",
        f"⚡ CVD:    {cvd_sign}{result['cvd_pct']:.1f}% {c_arr}",
        f"📈 OI:     {oi_sign}{result['oi_pct']:.1f}% {o_arr}",
        f"📊 거래량: {result['vol_pct']:.1f}%",
        f"🔍 진단:   {result['diagnosis']}",
        f"━━━━━━━━━━━━━━━━━━",
    ])
