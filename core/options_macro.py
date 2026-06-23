# ══════════════════════════════════════════
#  core/options_macro.py  –  Deribit 옵션 거시 심리 워커
#
#  Deribit public API(인증 불필요)로 BTC/ETH 옵션의
#  GEX(감마 익스포저), IV 스큐, Put/Call 비율을 계산해
#  options_macro 테이블에 시계열로 저장.
#
#  목적: 임밸런스/스캐너 신호와 confluence 검증용 거시 레이어.
#        "방향 예언"이 아니라 "시장 포지셔닝 구조" 스냅샷.
#
#  대상: BTC, ETH (Deribit은 알트 옵션 거의 없음)
#  주기: 30분마다 (옵션 IV/OI는 15분봉만큼 빨리 안 변함)
#  구조: imbalance_resolver.py 패턴 복제 (requests 동기 + 주기 루프)
#
#  ⚠️ Render Singapore IP에서 Deribit 접근 가능 여부는 배포 후 확인 필요.
#     실패 시 로깅만 하고 다음 사이클 진행 (다른 워커 영향 없음).
# ══════════════════════════════════════════

import asyncio
import logging
import math
from datetime import datetime, timezone
from collections import defaultdict

import requests

from db.supabase import insert_options_macro

logger = logging.getLogger(__name__)

# ── 설정 ───────────────────────────────────
DERIBIT_BASE = "https://www.deribit.com/api/v2/public"
CURRENCIES   = ["BTC", "ETH"]
POLL_MIN     = 30          # 30분 주기
REQ_TIMEOUT  = 15
# 메인 만기 선택: 가까운 N개 만기 중 OI 최대
NEAR_EXPIRY_POOL = 4


# ── Deribit REST 헬퍼 ──────────────────────
def _api(method: str, **params) -> dict | list | None:
    """Deribit public 호출. 실패 시 None."""
    try:
        r = requests.get(f"{DERIBIT_BASE}/{method}", params=params, timeout=REQ_TIMEOUT)
        r.raise_for_status()
        return r.json().get("result")
    except Exception as e:
        logger.warning(f"[opt-macro] API 실패 {method}: {e}")
        return None


def _get_spot(currency: str) -> float | None:
    res = _api("get_index_price", index_name=f"{currency.lower()}_usd")
    if not res:
        return None
    return res.get("index_price")


def _get_option_summary(currency: str) -> list | None:
    """모든 옵션의 IV/OI/마크가격 한 번에"""
    return _api("get_book_summary_by_currency", currency=currency, kind="option")


# ── BS 감마 ────────────────────────────────
def _norm_pdf(x: float) -> float:
    return math.exp(-x * x / 2) / math.sqrt(2 * math.pi)


def _bs_gamma(S: float, K: float, T: float, sigma: float) -> float:
    if sigma <= 0 or T <= 0 or S <= 0 or K <= 0:
        return 0.0
    try:
        d1 = (math.log(S / K) + (sigma * sigma / 2) * T) / (sigma * math.sqrt(T))
        return _norm_pdf(d1) / (S * sigma * math.sqrt(T))
    except (ValueError, ZeroDivisionError):
        return 0.0


def _parse_expiry_days(expiry_str: str) -> float | None:
    """'27JUN25' → 잔존일수"""
    try:
        exp = datetime.strptime(expiry_str, "%d%b%y").replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        days = (exp - now).total_seconds() / 86400
        return max(days, 0.001)
    except Exception:
        return None


# ── 단일 통화 분석 ─────────────────────────
def analyze_currency(currency: str) -> dict | None:
    """Deribit 옵션 → GEX/스큐/PCR 계산. 실패 시 None."""
    spot = _get_spot(currency)
    if not spot:
        return None

    summary = _get_option_summary(currency)
    if not summary:
        return None

    # 파싱: instrument_name = "BTC-27JUN25-70000-C"
    rows = []
    for d in summary:
        parts = (d.get("instrument_name") or "").split("-")
        if len(parts) != 4:
            continue
        try:
            strike = float(parts[2])
        except ValueError:
            continue
        rows.append({
            "expiry": parts[1],
            "strike": strike,
            "type":   parts[3],                       # 'C' | 'P'
            "iv":     (d.get("mark_iv") or 0) / 100,  # %를 소수로
            "oi":     d.get("open_interest") or 0,    # BTC 단위 계약수
        })

    if not rows:
        return None

    # ── [1] 전체 Put/Call OI 비율 ──
    call_oi = sum(r["oi"] for r in rows if r["type"] == "C")
    put_oi  = sum(r["oi"] for r in rows if r["type"] == "P")
    pcr = round(put_oi / call_oi, 3) if call_oi else None

    # ── 메인 만기 선택 (가까운 N개 중 OI 최대) ──
    by_exp = defaultdict(list)
    for r in rows:
        by_exp[r["expiry"]].append(r)

    exps = []
    for exp, items in by_exp.items():
        days = _parse_expiry_days(exp)
        if days is None:
            continue
        exps.append((exp, days, sum(i["oi"] for i in items), items))
    if not exps:
        return None
    exps.sort(key=lambda x: x[1])  # 잔존일수 오름차순
    main_exp, main_days, main_oi, items = max(
        exps[:NEAR_EXPIRY_POOL], key=lambda x: x[2]
    )

    # ── [2] 25-delta 근사 스큐 (OTM 콜 vs OTM 풋 IV) ──
    calls = {r["strike"]: r for r in items if r["type"] == "C"}
    puts  = {r["strike"]: r for r in items if r["type"] == "P"}
    common = sorted(set(calls) & set(puts))

    atm_iv = skew_25d = otm_call_iv = otm_put_iv = None
    atm_strike = None
    if common:
        atm_strike = min(common, key=lambda k: abs(k - spot))
        atm_iv = round((calls[atm_strike]["iv"] + puts[atm_strike]["iv"]) / 2 * 100, 2)

        otm_call_k = min((k for k in common if k > spot * 1.05),
                         default=None, key=lambda k: abs(k - spot * 1.10))
        otm_put_k = min((k for k in common if k < spot * 0.95),
                        default=None, key=lambda k: abs(k - spot * 0.90))
        if otm_call_k and otm_put_k:
            otm_call_iv = round(calls[otm_call_k]["iv"] * 100, 2)
            otm_put_iv  = round(puts[otm_put_k]["iv"] * 100, 2)
            skew_25d = round(otm_call_iv - otm_put_iv, 2)  # +면 콜 비쌈(탐욕), -면 풋 비쌈(공포)

    # ── [3] GEX (행사가별 감마 × OI) ──
    gex_by_strike = defaultdict(float)
    T = main_days / 365.0
    for r in items:
        if r["iv"] <= 0:
            continue
        g = _bs_gamma(spot, r["strike"], T, r["iv"])
        # 관례: 딜러가 콜 매도(+)/풋 매도(-) 가정 (실제 포지션 비공개 → 부호는 참고용)
        sign = 1 if r["type"] == "C" else -1
        gex_by_strike[r["strike"]] += sign * g * r["oi"] * spot

    net_gex = sum(gex_by_strike.values())
    # 최대 |감마×OI| 행사가 = 자석 후보 (부호 무관, 분포가 신뢰도 높음)
    max_gamma_strike = None
    if gex_by_strike:
        max_gamma_strike = max(gex_by_strike.items(), key=lambda x: abs(x[1]))[0]

    # 상위 자석 구간 (대시보드 표시용, 행사가:값)
    top_strikes = sorted(gex_by_strike.items(), key=lambda x: abs(x[1]), reverse=True)[:8]
    gex_profile = [{"strike": k, "gex": round(v, 1)} for k, v in sorted(top_strikes, key=lambda x: x[0])]

    # ── 해석 라벨 ──
    gex_regime = None
    if net_gex is not None:
        gex_regime = "long_gamma" if net_gex < 0 else "short_gamma"  # 롱=억제/횡보, 숏=증폭/추세
    skew_sentiment = None
    if skew_25d is not None:
        skew_sentiment = "greed" if skew_25d > 0 else "fear"

    return {
        "asset":            currency,
        "spot":             round(spot, 2),
        "main_expiry":      main_exp,
        "days_to_exp":      round(main_days, 2),
        "main_expiry_oi":   round(main_oi, 1),
        "pc_ratio":         pcr,
        "call_oi":          round(call_oi, 1),
        "put_oi":           round(put_oi, 1),
        "atm_strike":       atm_strike,
        "atm_iv":           atm_iv,
        "otm_call_iv":      otm_call_iv,
        "otm_put_iv":       otm_put_iv,
        "skew_25d":         skew_25d,
        "skew_sentiment":   skew_sentiment,
        "net_gex":          round(net_gex, 1) if net_gex is not None else None,
        "gex_regime":       gex_regime,
        "max_gamma_strike": max_gamma_strike,
        "gex_profile":      gex_profile,   # JSONB
    }


# ── 백그라운드 루프 ─────────────────────────
async def options_macro_loop():
    """30분마다 BTC/ETH 옵션 거시 분석."""
    logger.info("[opt-macro] 옵션 거시 워커 시작")
    await asyncio.sleep(60)  # 부팅 후 1분 대기 (다른 워커 안정화 후)

    while True:
        try:
            # requests는 블로킹 → 이벤트 루프 양보 위해 executor에서 실행
            loop = asyncio.get_event_loop()
            for currency in CURRENCIES:
                result = await loop.run_in_executor(None, analyze_currency, currency)
                if result is None:
                    logger.warning(f"[opt-macro] {currency} 분석 실패 — 스킵")
                    continue
                await insert_options_macro(result)
                logger.info(
                    f"[opt-macro] {currency} 저장 — spot={result['spot']} "
                    f"skew={result['skew_25d']} regime={result['gex_regime']} "
                    f"magnet={result['max_gamma_strike']}"
                )
        except Exception as e:
            logger.error(f"[opt-macro] 사이클 실패: {e}", exc_info=True)

        await asyncio.sleep(POLL_MIN * 60)
