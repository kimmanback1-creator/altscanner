# ══════════════════════════════════════════
#  core/trend.py  –  추세 판정
#  최근 N봉 history → 'up' / 'down' / 'flat'
# ══════════════════════════════════════════

# 추세 판정 임계값 (선형회귀 기울기 기준)
# 데이터 쌓이면 조정 필요
TREND_THRESHOLD = {
    'price': 0.005,   # 봉당 ±0.01% (20봉 = ±0.2%)
    'cvd':   0.05,    # 봉당 ±0.1단위
    'oi':    0.001,  # 봉당 ±0.004% (20봉 = ±0.08%)
}

# 추세 판정 최소 데이터 길이
MIN_HISTORY = 2


def linear_slope(values: list) -> float:
    """
    리스트에 대한 선형회귀 기울기 계산
    음수도 정상 처리됨 (CVD용)
    """
    n = len(values)
    if n < 2:
        return 0.0
    x_mean = (n - 1) / 2
    y_mean = sum(values) / n
    num = sum((i - x_mean) * (values[i] - y_mean) for i in range(n))
    den = sum((i - x_mean) ** 2 for i in range(n))
    if den == 0:
        return 0.0
    return num / den


def trend(history: list, threshold: float) -> str:
    """
    history 추세 판정
    반환: 'up' | 'down' | 'flat'
    데이터 부족 시 'flat'
    """
    if len(history) < MIN_HISTORY:
        return 'flat'

    slope = linear_slope(history)

    if slope > threshold:
        return 'up'
    if slope < -threshold:
        return 'down'
    return 'flat'


def trend_price(price_history: list) -> str:
    """
    가격 추세
    history 값이 그대로 가격이라 절대 기울기 → 비율 변환 필요
    """
    if len(price_history) < MIN_HISTORY:
        return 'flat'
    avg = sum(price_history) / len(price_history)
    if avg == 0:
        return 'flat'
    slope = linear_slope(price_history)
    slope_pct = (slope / avg) * 100  # 봉당 % 변화로 정규화
    if slope_pct > TREND_THRESHOLD['price']:
        return 'up'
    if slope_pct < -TREND_THRESHOLD['price']:
        return 'down'
    return 'flat'


def trend_cvd(cvd_history: list) -> str:
    """CVD 추세 — 음수 값 가능"""
    return trend(cvd_history, TREND_THRESHOLD['cvd'])


def trend_oi(oi_history: list) -> str:
    """OI 변화율 추세 — 이미 % 단위"""
    return trend(oi_history, TREND_THRESHOLD['oi'])
