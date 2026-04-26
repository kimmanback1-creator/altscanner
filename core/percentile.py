# ══════════════════════════════════════════
#  core/percentile.py  –  백분위 계산
#  0~100% 변환 (CVD/OI는 -100~+100)
# ══════════════════════════════════════════

def to_percentile(current: float, history: list) -> float:
    """
    현재값이 히스토리 중 상위 몇 %인지 반환
    반환값: 0.0 ~ 100.0
    데이터 부족 시 50.0 반환
    """
    if len(history) < 5:
        return 50.0

    below = sum(1 for h in history if h < current)
    return round((below / len(history)) * 100, 1)


def cvd_percentile(cvd_history: list) -> float:
    """
    현재 캔들 CVD delta(마지막 값)를 히스토리 분포와 비교
    반환값: -100.0 ~ +100.0
    양수 = 매수 우위, 음수 = 매도 우위

    선형회귀 대신 현재 delta값 자체를 기준으로 백분위 계산:
    - 현재 delta > 히스토리 중 90% → +90.0
    - 현재 delta < 히스토리 중 70% (즉 음수 강함) → -70.0
    """
    if len(cvd_history) < 3:
        return 0.0

    current = cvd_history[-1]
    history = cvd_history[:-1]  # 현재 제외한 과거값들

    if not history:
        return 0.0

    abs_pct = to_percentile(abs(current), [abs(h) for h in history])
    return round(abs_pct if current >= 0 else -abs_pct, 1)


def oi_percentile(oi_chg: float, oi_history: list) -> float:
    """
    OI 변화율 백분위
    반환값: -100.0 ~ +100.0
    양수 = 신규 포지션 증가, 음수 = 포지션 청산
    """
    if len(oi_history) < 5:
        return 0.0

    abs_pct = to_percentile(abs(oi_chg), [abs(h) for h in oi_history])
    return round(abs_pct if oi_chg >= 0 else -abs_pct, 1)


def vol_percentile(vol_ratio: float, vol_history: list) -> float:
    """
    거래량 백분위
    vol_ratio = 이번 캔들 / 과거평균 (예: 1.5 = 평균의 150%)
    vol_history = 과거 캔들들의 원시 거래량
    → vol_history로 과거 vol_ratio들을 재계산해서 분포 구성
    반환값: 0.0 ~ 100.0
    """
    if len(vol_history) < 5:
        return 50.0
    return to_percentile(vol_ratio, vol_history)
    # 과거 vol_ratio 분포 재구성
    ratios = []
    for i in range(1, len(vol_history)):
        past_avg = sum(vol_history[:i]) / i
        if past_avg > 0:
            ratios.append(vol_history[i] / past_avg)

    if not ratios:
        return 50.0

    return to_percentile(vol_ratio, ratios)


def is_warmed_up(vol_history: list, oi_history: list, cvd_history: list) -> bool:
    """최소 데이터 축적 여부 확인 (워밍업 체크)"""
    return (
        len(vol_history) >= 20 and
        len(oi_history)  >= 10 and
        len(cvd_history) >= 5
    )
