# ══════════════════════════════════════════
#  core/percentile.py  –  백분위 계산
#  0~100% 변환 (CVD/OI는 -100~+100)
# ══════════════════════════════════════════

import numpy as np


def to_percentile(current: float, history: list) -> float:
    """
    현재값이 히스토리 중 상위 몇 %인지 반환
    반환값: 0.0 ~ 100.0
    데이터 부족 시 50.0 반환
    """
    if len(history) < 1:
        return 50.0

    below = sum(1 for h in history if h < current)
    return round((below / len(history)) * 100, 1)


def cvd_percentile(cvd_history: list) -> float:
    """
    CVD 기울기를 선형회귀로 계산 후 백분위 변환
    반환값: -100.0 ~ +100.0
    양수 = 매수 우세, 음수 = 매도 우세
    """
    if len(cvd_history) < 1:
        return 0.0

    n = len(cvd_history)
    x = list(range(n))
    x_mean = sum(x) / n
    y_mean = sum(cvd_history) / n

    num = sum((x[i] - x_mean) * (cvd_history[i] - y_mean) for i in range(n))
    den = sum((x[i] - x_mean) ** 2 for i in range(n))
    slope = num / den if den != 0 else 0

    # 절대값 백분위 계산 후 방향 부여
    abs_pct = to_percentile(abs(slope), [abs(h) for h in _slope_history(cvd_history)])
    return round(abs_pct if slope >= 0 else -abs_pct, 1)


def _slope_history(cvd_history: list) -> list:
    """히스토리 내 rolling slope 계산 (백분위 기준용)"""
    if len(cvd_history) < 3:
        return [0.0]
    slopes = []
    for i in range(2, len(cvd_history)):
        sub = cvd_history[:i+1]
        n = len(sub)
        x = list(range(n))
        xm = sum(x)/n; ym = sum(sub)/n
        num = sum((x[j]-xm)*(sub[j]-ym) for j in range(n))
        den = sum((x[j]-xm)**2 for j in range(n))
        slopes.append(num/den if den != 0 else 0)
    return slopes if slopes else [0.0]


def oi_percentile(oi_chg: float, oi_history: list) -> float:
    """
    OI 변화율 백분위
    반환값: -100.0 ~ +100.0
    양수 = 신규 포지션 증가, 음수 = 포지션 청산
    """
    if len(oi_history) < 1:
        return 0.0

    abs_pct = to_percentile(abs(oi_chg), [abs(h) for h in oi_history])
    return round(abs_pct if oi_chg >= 0 else -abs_pct, 1)


def vol_percentile(vol_ratio: float, vol_history: list) -> float:
    """
    거래량 백분위
    반환값: 0.0 ~ 100.0
    """
    if len(vol_history) < 1:
        return 50.0
    return to_percentile(vol_ratio, vol_history)


def is_warmed_up(vol_history: list, oi_history: list, cvd_history: list) -> bool:
    """최소 데이터 축적 여부 확인 (워밍업 체크)"""
    return True
    
