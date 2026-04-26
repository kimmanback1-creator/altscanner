# ══════════════════════════════════════════
#  core/state.py  –  공유 상태 저장소
#  모든 거래소 틱 데이터가 여기로 집결
# ══════════════════════════════════════════

import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import List, Dict

lock = threading.Lock()


@dataclass
class SymbolState:
    # ── CVD ──────────────────────────────
    cvd_cum:     float = 0.0   # 세션 누적 CVD
    cvd_candle:  float = 0.0   # 현재 캔들 CVD delta
    cvd_history: List[float] = field(default_factory=list)  # 최근 N봉 delta

    # ── 거래량 ───────────────────────────
    vol_candle:  float = 0.0
    vol_history: List[float] = field(default_factory=list)  # 최근 96봉

    # ── OI ───────────────────────────────
    oi_current:  float = 0.0
    oi_prev:     float = 0.0
    oi_history:  List[float] = field(default_factory=list)  # 최근 192봉 변화율

    # ── 가격 ─────────────────────────────
    price_open:    float = 0.0
    price_current: float = 0.0

    # ── 메타 ─────────────────────────────
    exchange: str = ""


# 거래소별 심볼 상태
_state: Dict[str, Dict[str, SymbolState]] = {
    "binance": defaultdict(SymbolState),
    "okx":     defaultdict(SymbolState),
    "bybit":   defaultdict(SymbolState),
}


def update_trade(exchange: str, symbol: str, price: float, qty: float, is_buy: bool):
    """틱 수신 시 CVD + 거래량 업데이트"""
    with lock:
        s = _state[exchange][symbol]
        delta = qty if is_buy else -qty
        s.cvd_cum    += delta
        s.cvd_candle += delta
        s.vol_candle += qty
        s.price_current = price
        s.exchange = exchange
        if s.price_open == 0:
            s.price_open = price


def update_oi(exchange: str, symbol: str, oi: float):
    """OI 폴링 결과 반영"""
    with lock:
        s = _state[exchange][symbol]
        if s.oi_current != 0:
            chg_pct = (oi - s.oi_current) / s.oi_current * 100
            s.oi_history.append(chg_pct)
            if len(s.oi_history) > 192:
                s.oi_history.pop(0)
        s.oi_prev    = s.oi_current
        s.oi_current = oi


def snapshot_and_reset(exchange: str, symbol: str) -> dict:
    """캔들 마감 시 스냅샷 반환 + 캔들 값 초기화"""
    with lock:
        s = _state[exchange][symbol]

        # vol_ratio: append 전에 계산 (자기 자신을 평균에 포함시키지 않음)
        vol_avg = sum(s.vol_history) / len(s.vol_history) if s.vol_history else None
        vol_ratio = (s.vol_candle / vol_avg) if vol_avg else 0.0

        # 히스토리 업데이트 (ratio 계산 후)
        s.vol_history.append(s.vol_candle)
        if len(s.vol_history) > 96:
            s.vol_history.pop(0)

        s.cvd_history.append(s.cvd_candle)
        if len(s.cvd_history) > 10:
            s.cvd_history.pop(0)

        # 가격 변화율
        price_chg = 0.0
        if s.price_open > 0:
            price_chg = (s.price_current - s.price_open) / s.price_open * 100

        # OI 평균 변화율
        oi_chg = s.oi_history[-1] if s.oi_history else 0.0

        snap = {
            "exchange":    exchange,
            "symbol":      symbol,
            "cvd_delta":   s.cvd_candle,
            "cvd_history": list(s.cvd_history),
            "vol_candle":  s.vol_candle,
            "vol_history": list(s.vol_history),
            "vol_ratio":   vol_ratio,
            "oi_chg":      oi_chg,
            "oi_history":  list(s.oi_history),
            "price_chg":   price_chg,
            "price":       s.price_current,
        }

        # 캔들 초기화
        s.cvd_candle = 0.0
        s.vol_candle = 0.0
        s.price_open = s.price_current

        return snap


def get_all_symbols(exchange: str) -> list:
    with lock:
        return list(_state[exchange].keys())
