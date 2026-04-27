# ══════════════════════════════════════════
#  core/state.py  –  공유 상태 저장소
#  모든 거래소 틱 데이터가 여기로 집결
#  15분봉 + 4시간봉 동시 누적
# ══════════════════════════════════════════

import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import List, Dict

lock = threading.Lock()


@dataclass
class SymbolState:
    # ── CVD (15분) ───────────────────────
    cvd_cum:     float = 0.0   # 세션 누적 CVD
    cvd_candle:  float = 0.0   # 현재 15분 캔들 CVD delta
    cvd_history: List[float] = field(default_factory=list)  # 최근 20봉 delta

    # ── CVD (4시간) ──────────────────────
    cvd_4h_candle:  float = 0.0
    cvd_4h_history: List[float] = field(default_factory=list)  # 최근 20봉

    # ── 거래량 (15분) ────────────────────
    vol_candle:  float = 0.0
    vol_history: List[float] = field(default_factory=list)  # 최근 96봉

    # ── 거래량 (4시간) ───────────────────
    vol_4h_candle:  float = 0.0
    vol_4h_history: List[float] = field(default_factory=list)  # 최근 20봉

    # ── OI ───────────────────────────────
    oi_current:    float = 0.0
    oi_prev:       float = 0.0
    oi_history:    List[float] = field(default_factory=list)  # 최근 192봉 변화율 (15분 호환)
    oi_4h_history: List[float] = field(default_factory=list)  # 최근 20봉 (4H 마감 시 append)

    # ── 가격 (15분) ──────────────────────
    price_open:    float = 0.0
    price_current: float = 0.0
    price_history: List[float] = field(default_factory=list)  # 최근 20봉

    # ── 가격 (4시간) ─────────────────────
    price_4h_open:    float = 0.0
    price_4h_history: List[float] = field(default_factory=list)  # 최근 20봉

    # ── 메타 ─────────────────────────────
    exchange: str = ""


# 거래소별 심볼 상태
_state: Dict[str, Dict[str, SymbolState]] = {
    "binance": defaultdict(SymbolState),
    "okx":     defaultdict(SymbolState),
    "bybit":   defaultdict(SymbolState),
}


def update_trade(exchange: str, symbol: str, price: float, qty: float, is_buy: bool):
    """틱 수신 시 CVD + 거래량 업데이트 (15분 + 4시간 동시)"""
    with lock:
        s = _state[exchange][symbol]
        delta = qty if is_buy else -qty
        # 누적 (15분 + 4시간)
        s.cvd_cum       += delta
        s.cvd_candle    += delta
        s.cvd_4h_candle += delta
        s.vol_candle    += qty
        s.vol_4h_candle += qty
        # 가격 + 메타
        s.price_current = price
        s.exchange = exchange
        if s.price_open == 0:
            s.price_open = price
        if s.price_4h_open == 0:
            s.price_4h_open = price


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
    """15분봉 마감 시 스냅샷 반환 + 캔들 값 초기화"""
    with lock:
        s = _state[exchange][symbol]

        # vol_ratio: append 전에 계산
        vol_avg = sum(s.vol_history) / len(s.vol_history) if s.vol_history else None
        vol_ratio = (s.vol_candle / vol_avg) if vol_avg else 0.0

        # 히스토리 업데이트 (raw 거래량)
        s.vol_history.append(s.vol_candle)
        if len(s.vol_history) > 96:
            s.vol_history.pop(0)

        s.cvd_history.append(s.cvd_candle)
        if len(s.cvd_history) > 20:
            s.cvd_history.pop(0)

        # 가격 history 업데이트 (마감가 저장)
        s.price_history.append(s.price_current)
        if len(s.price_history) > 20:
            s.price_history.pop(0)

        # 가격 변화율
        price_chg = 0.0
        if s.price_open > 0:
            price_chg = (s.price_current - s.price_open) / s.price_open * 100

        # OI 최근 변화율
        oi_chg = s.oi_history[-1] if s.oi_history else 0.0

        snap = {
            "exchange":      exchange,
            "symbol":        symbol,
            "cvd_delta":     s.cvd_candle,
            "cvd_history":   list(s.cvd_history),
            "vol_candle":    s.vol_candle,
            "vol_history":   list(s.vol_history),
            "vol_ratio":     vol_ratio,
            "oi_chg":        oi_chg,
            "oi_history":    list(s.oi_history),
            "price_chg":     price_chg,
            "price":         s.price_current,
            "price_history": list(s.price_history),
        }

        # 15분 캔들 초기화
        s.cvd_candle = 0.0
        s.vol_candle = 0.0
        s.price_open = s.price_current

        return snap


def snapshot_and_reset_4h(exchange: str, symbol: str) -> dict:
    """4시간봉 마감 시 스냅샷 반환 + 4H 캔들 값 초기화"""
    with lock:
        s = _state[exchange][symbol]

        # vol_ratio (4H 기준)
        vol_avg = sum(s.vol_4h_history) / len(s.vol_4h_history) if s.vol_4h_history else None
        vol_ratio = (s.vol_4h_candle / vol_avg) if vol_avg else 0.0

        # 히스토리 업데이트 (raw 거래량)
        s.vol_4h_history.append(s.vol_4h_candle)
        if len(s.vol_4h_history) > 20:
            s.vol_4h_history.pop(0)

        s.cvd_4h_history.append(s.cvd_4h_candle)
        if len(s.cvd_4h_history) > 20:
            s.cvd_4h_history.pop(0)

        s.price_4h_history.append(s.price_current)
        if len(s.price_4h_history) > 20:
            s.price_4h_history.pop(0)

        # OI 4H history: 가장 최근 oi_chg 값 사용
        oi_chg = s.oi_history[-1] if s.oi_history else 0.0
        s.oi_4h_history.append(oi_chg)
        if len(s.oi_4h_history) > 20:
            s.oi_4h_history.pop(0)

        # 가격 변화율 (4H 기준)
        price_chg = 0.0
        if s.price_4h_open > 0:
            price_chg = (s.price_current - s.price_4h_open) / s.price_4h_open * 100

        snap = {
            "exchange":      exchange,
            "symbol":        symbol,
            "cvd_delta":     s.cvd_4h_candle,
            "cvd_history":   list(s.cvd_4h_history),
            "vol_candle":    s.vol_4h_candle,
            "vol_history":   list(s.vol_4h_history),
            "vol_ratio":     vol_ratio,
            "oi_chg":        oi_chg,
            "oi_history":    list(s.oi_4h_history),
            "price_chg":     price_chg,
            "price":         s.price_current,
            "price_history": list(s.price_4h_history),
        }

        # 4H 캔들 초기화
        s.cvd_4h_candle = 0.0
        s.vol_4h_candle = 0.0
        s.price_4h_open = s.price_current

        return snap


def get_all_symbols(exchange: str) -> list:
    with lock:
        return list(_state[exchange].keys())
