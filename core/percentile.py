
# ══════════════════════════════════════════
#  core/percentile.py  –  백분위 계산
#  0~100% 변환 (CVD/OI는 -100~+100)
# ══════════════════════════════════════════

def to_percentile(current: float, history: list) -> float:
    """현재값이 히스토리 중 상위 몇 %인지 반환 (0~100)"""
    if not history:
        return 50.0
    below = sum(1 for h in history if h < current)
    equal = sum(1 for h in history if h == current)
    # 같은 값이 많을 때 중간값 반환
    return round((below + equal * 0.5) / len(history) * 100, 1)


def cvd_percentile(cvd_history: list) -> float:
    """
    CVD delta 백분위 (-100 ~ +100)
    선형회귀 대신 현재 delta를 히스토리와 비교
    """
    if not cvd_history:
        return 0.0

    current = cvd_history[-1]

    if len(cvd_history) == 1:
        # 첫 봉은 방향만 반환
        if current > 0:
            return 50.0
        elif current < 0:
            return -50.0
        return 0.0

    # 이전 봉들과 비교
    history = cvd_history[:-1]
    abs_pct = to_percentile(abs(current), [abs(h) for h in history])
    return round(abs_pct if current >= 0 else -abs_pct, 1)


def oi_percentile(oi_chg: float, oi_history: list) -> float:
    """OI 변화율 백분위 (-100 ~ +100)"""
    if not oi_history:
        return 0.0

    if len(oi_history) == 1:
        if oi_chg > 0:
            return 50.0
        elif oi_chg < 0:
            return -50.0
        return 0.0

    abs_pct = to_percentile(abs(oi_chg), [abs(h) for h in oi_history[:-1]])
    return round(abs_pct if oi_chg >= 0 else -abs_pct, 1)


def vol_percentile(vol_ratio: float, vol_history: list) -> float:
    """거래량 백분위 (0 ~ 100)"""
    if not vol_history:
        return 50.0

    if len(vol_history) == 1:
        return 50.0

    # 이전 봉들과 비교
    history = vol_history[:-1]
    return to_percentile(vol_ratio, history)


def is_warmed_up(vol_history, oi_history, cvd_history) -> bool:
    return True
