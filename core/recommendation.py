# ══════════════════════════════════════════
#  core/recommendation.py  –  자동 셋업 진입 추천 엔진 (Python)
#
#  index.html의 computeRecommendation 로직을 1:1 포팅.
#  - signal_state 최신 봉 → 활성 시그널 추출
#  - setup_log_auto 누적 통계로 적중률 계산 (n≥5)
#  - 환경 일치 보너스 1.5x
#  - 65%+ 추천 발생 시 Telegram 푸시 (중복 방지)
#
#  프론트(JS)와 로직 정합성 필수 — 한쪽 수정 시 양쪽 동기화.
# ══════════════════════════════════════════

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from db.supabase import get_client
from notify.telegram import send_message
from core.tracker import create_rec_performance_row

logger = logging.getLogger(__name__)

# ── 상수 (JS COMBO_BONUS와 동일) ────────────
COMBO_BONUS = {
    "long": [
        {"sig": "diamond",   "val": "up",              "env": ["강세", "약세→강세 전환"]},
        {"sig": "obv",       "val": "bull_divergence", "env": ["약세"]},
        {"sig": "obv",       "val": "oversold_release","env": ["약세→강세 전환"]},
        {"sig": "fvg",       "val": "new_up",          "env": ["강세", "약세→강세 전환"]},
        {"sig": "imbalance", "val": "new_buy",         "env": ["강세", "약세→강세 전환"]},
        {"sig": "va_weekly", "val": "val_support",     "env": ["강세", "약세→강세 전환"]},
    ],
    "short": [
        {"sig": "diamond",   "val": "down",               "env": ["약세", "강세→약세 전환"]},
        {"sig": "obv",       "val": "bear_divergence",    "env": ["강세"]},
        {"sig": "obv",       "val": "overbought_release", "env": ["강세→약세 전환"]},
        {"sig": "fvg",       "val": "new_down",           "env": ["약세", "강세→약세 전환"]},
        {"sig": "imbalance", "val": "new_sell",           "env": ["약세", "강세→약세 전환"]},
        {"sig": "va_weekly", "val": "vah_resistance",     "env": ["약세", "강세→약세 전환"]},
    ],
}

UP_RESULTS   = ["상승", "하락 후 상승", "매수 임밸런스 흡수 후 상승"]
DOWN_RESULTS = ["하락", "상승 후 하락", "매도 임밸런스 흡수 후 하락"]

MIN_SAMPLE = 5       # n≥5
CONF_DIR   = 0.55    # 방향 결정 최소 적중률
CONF_STRONG = 65     # STRONG 임계 (%)
CONF_WEAK   = 55     # WEAK 임계 (%)
BONUS_MULT  = 1.5    # 환경 일치 보너스

PUSH_THRESHOLD = 65  # Telegram 푸시 신뢰도 임계 (%)


# ── 활성 시그널 추출 (JS extractActiveSignals와 동일) ──
def extract_active_signals(signals: dict) -> list:
    """signals JSONB → [{'sig':..., 'val':...}, ...]"""
    if not signals:
        return []
    out = []

    # 1. trendline — state
    tl = signals.get("trendline") or {}
    if tl.get("state"):
        out.append({"sig": "trendline", "val": tl["state"]})

    # 2. obv — boolean 플래그
    obv = signals.get("obv") or {}
    for key, val in [
        ("overbought", "overbought"), ("oversold", "oversold"),
        ("overbought_release", "overbought_release"),
        ("oversold_release", "oversold_release"),
        ("bear_divergence", "bear_divergence"),
        ("bull_divergence", "bull_divergence"),
    ]:
        if obv.get(key):
            out.append({"sig": "obv", "val": val})

    # 3. diamond
    dia = signals.get("diamond") or {}
    if dia.get("green"):
        out.append({"sig": "diamond", "val": "up"})
    if dia.get("red"):
        out.append({"sig": "diamond", "val": "down"})

    # 4. trend_change
    tc = signals.get("trend_change") or {}
    if tc.get("active"):
        out.append({"sig": "trend_change", "val": "active"})

    # 5. va_weekly — 지지/저항 boolean
    va = signals.get("va_weekly") or {}
    for key in ["vah_support", "vah_resistance", "poc_support",
                "poc_resistance", "val_support", "val_resistance"]:
        if va.get(key):
            out.append({"sig": "va_weekly", "val": key})

    # 6. fvg — 신규
    fvg = signals.get("fvg") or {}
    if fvg.get("new_up_this_bar"):
        out.append({"sig": "fvg", "val": "new_up"})
    if fvg.get("new_down_this_bar"):
        out.append({"sig": "fvg", "val": "new_down"})

    # 7. london_box — 신규
    lb = signals.get("london_box") or {}
    if lb.get("new_up_this_bar"):
        out.append({"sig": "london_box", "val": "new_up"})
    if lb.get("new_down_this_bar"):
        out.append({"sig": "london_box", "val": "new_down"})

    # 8. imbalance — 신규 + 흡수
    imb = signals.get("imbalance") or {}
    if imb.get("new_buy_this_bar"):
        out.append({"sig": "imbalance", "val": "new_buy"})
    if imb.get("new_sell_this_bar"):
        out.append({"sig": "imbalance", "val": "new_sell"})
    if (imb.get("absorbed_buy_count") or 0) > 0:
        out.append({"sig": "imbalance", "val": "absorbed_buy"})
    if (imb.get("absorbed_sell_count") or 0) > 0:
        out.append({"sig": "imbalance", "val": "absorbed_sell"})

    return out


# ── 단일 시그널 통계 (JS getSignalStats와 동일) ──
def get_signal_stats(tf: str, sig: str, val: str):
    """
    setup_log_auto에서 같은 TF + 같은 시그널-값 + result 입력된 행 조회.
    n<5 또는 둘 다 55% 미만이면 None.
    """
    sb = get_client()
    try:
        res = (
            sb.table("setup_log_auto")
            .select("result")
            .eq("timeframe", tf)
            .filter(f"events->>{sig}", "ilike", f"%{val}%")
            .execute()
        )
        rows = [r for r in (res.data or []) if r.get("result")]
    except Exception as e:
        logger.error(f"[stats] {sig}={val}: {e}")
        return None

    if len(rows) < MIN_SAMPLE:
        return None

    counts = {"up": 0, "down": 0, "flat": 0}
    for r in rows:
        result = r.get("result")
        if result in UP_RESULTS:
            counts["up"] += 1
        elif result in DOWN_RESULTS:
            counts["down"] += 1
        else:
            counts["flat"] += 1

    n = len(rows)
    up_pct   = counts["up"] / n
    down_pct = counts["down"] / n

    direction = None
    confidence = 0.0
    if up_pct > down_pct and up_pct >= CONF_DIR:
        direction = "LONG"
        confidence = up_pct
    elif down_pct > up_pct and down_pct >= CONF_DIR:
        direction = "SHORT"
        confidence = down_pct

    return {
        "n": n, "up_pct": up_pct, "down_pct": down_pct,
        "direction": direction, "confidence": confidence, "counts": counts,
    }


# ── 환경 보너스 (JS hasComboBonus와 동일) ──
def has_combo_bonus(direction: str, sig: str, val: str, trendline_state) -> bool:
    lst = COMBO_BONUS["long"] if direction == "LONG" else COMBO_BONUS["short"]
    for combo in lst:
        if combo["sig"] == sig and combo["val"] == val and trendline_state in combo["env"]:
            return True
    return False


# ── 진입 레벨 (JS computeEntryLevels와 동일) ──
def compute_entry_levels(direction: str, row: dict) -> dict:
    signals = row.get("signals") or {}
    imb = signals.get("imbalance") or {}
    va  = signals.get("va_weekly") or {}

    current = float(row.get("bar_close") or row.get("entry_price") or 0)
    support    = _num(imb.get("nearest_support"))
    resistance = _num(imb.get("nearest_resistance"))
    val_lvl    = _num(va.get("val"))
    vah_lvl    = _num(va.get("vah"))

    tp2 = None
    if direction == "LONG":
        entry = current
        stop_loss   = support or val_lvl or current * 0.985
        take_profit = resistance or vah_lvl or current * 1.03
        if resistance and vah_lvl and vah_lvl > resistance:
            tp2 = vah_lvl
    else:
        entry = current
        stop_loss   = resistance or vah_lvl or current * 1.015
        take_profit = support or val_lvl or current * 0.97
        if support and val_lvl and val_lvl < support:
            tp2 = val_lvl

    risk   = abs(entry - stop_loss)
    reward = abs(entry - take_profit)
    rr = reward / risk if risk > 0 else 0

    return {
        "entry": round(entry, 2),
        "stop_loss": round(stop_loss, 2),
        "take_profit": round(take_profit, 2),
        "take_profit2": round(tp2, 2) if tp2 else None,
        "rr": round(rr, 2),
        "stop_loss_pct": round(abs((stop_loss - entry) / entry * 100), 2) if entry else 0,
        "take_profit_pct": round(abs((take_profit - entry) / entry * 100), 2) if entry else 0,
    }


def _num(v):
    try:
        return float(v) if v is not None else None
    except (ValueError, TypeError):
        return None


# ── 추천 계산 (JS computeRecommendation와 동일) ──
def compute_recommendation(row: dict, tf: str) -> dict:
    if not row:
        return {"verdict": "NO_DATA", "evidences": []}

    active = extract_active_signals(row.get("signals") or {})
    if not active:
        return {"verdict": "NO_DATA", "reason": "활성 시그널 없음", "evidences": []}

    tl = (row.get("signals") or {}).get("trendline") or {}
    trendline_state = tl.get("state")

    valid = []
    for a in active:
        stats = get_signal_stats(tf, a["sig"], a["val"])
        if stats is not None:
            valid.append({**a, "stats": stats})

    if not valid:
        return {
            "verdict": "NO_DATA",
            "reason": "활성 시그널 중 통계 충분(n≥5) 없음",
            "active_count": len(active),
            "evidences": [],
        }

    long_score = 0.0
    short_score = 0.0
    evidences = []

    for v in valid:
        st = v["stats"]
        if not st["direction"]:
            continue
        weight = st["confidence"]
        bonus = has_combo_bonus(st["direction"], v["sig"], v["val"], trendline_state)
        if bonus:
            weight *= BONUS_MULT
        if st["direction"] == "LONG":
            long_score += weight
        else:
            short_score += weight
        evidences.append({
            "sig": v["sig"], "val": v["val"], "n": st["n"],
            "direction": st["direction"], "hit_pct": st["confidence"] * 100,
            "weight": weight, "bonus": bonus,
        })

    evidences.sort(key=lambda e: e["weight"], reverse=True)

    total = long_score + short_score
    if total == 0:
        return {
            "verdict": "NO_DATA",
            "reason": "신뢰 가능한 방향성 시그널 없음",
            "evidences": evidences,
        }

    max_score = max(long_score, short_score)
    direction = "LONG" if long_score > short_score else "SHORT"
    conf_pct = max_score / total * 100

    if conf_pct >= CONF_STRONG:
        verdict = "STRONG_LONG" if direction == "LONG" else "STRONG_SHORT"
    elif conf_pct >= CONF_WEAK:
        verdict = "WEAK_LONG" if direction == "LONG" else "WEAK_SHORT"
    else:
        verdict = "MIXED"

    levels = compute_entry_levels(direction, row)

    return {
        "verdict": verdict,
        "direction": direction,
        "confidence_pct": round(conf_pct),
        "long_score": round(long_score, 2),
        "short_score": round(short_score, 2),
        "evidences": evidences,
        "levels": levels,
    }


# ── 최신 봉 조회 ───────────────────────────
def fetch_latest_bar(tf: str):
    sb = get_client()
    try:
        res = (
            sb.table("signal_state")
            .select("*")
            .eq("symbol", "BTCUSDT")
            .eq("timeframe", tf)
            .order("bar_ts", desc=True)
            .limit(1)
            .execute()
        )
        rows = res.data or []
        return rows[0] if rows else None
    except Exception as e:
        logger.error(f"[rec] {tf} 최신봉 조회 실패: {e}")
        return None


# ── 텔레그램 메시지 빌드 ───────────────────
def build_push_message(tf: str, rec: dict) -> str:
    is_long = rec["direction"] == "LONG"
    arrow = "▲" if is_long else "▼"
    dir_txt = "LONG" if is_long else "SHORT"
    icon = "🟢" if is_long else "🔴"
    lv = rec["levels"]

    lines = [
        f"{icon} *{tf} 진입 추천 — {arrow} {dir_txt}*",
        f"신뢰도 *{rec['confidence_pct']}%* (L:{rec['long_score']} / S:{rec['short_score']})",
        "",
        "💰 *진입 레벨*",
        f"• 진입: ${lv['entry']:,.2f}",
        f"• 손절: ${lv['stop_loss']:,.2f} (−{lv['stop_loss_pct']}%)",
        f"• 익절: ${lv['take_profit']:,.2f} (+{lv['take_profit_pct']}%)",
    ]
    if lv["take_profit2"]:
        lines.append(f"• 2차 익절: ${lv['take_profit2']:,.2f}")
    lines.append(f"• R:R *{lv['rr']}*")

    if rec["evidences"]:
        lines += ["", "📊 *근거 시그널*"]
        for ev in rec["evidences"][:5]:
            ev_arrow = "▲" if ev["direction"] == "LONG" else "▼"
            bonus = " ⚡환경보너스" if ev["bonus"] else ""
            lines.append(
                f"• {ev_arrow} {ev['sig']}={ev['val']} "
                f"(n={ev['n']}, {ev['hit_pct']:.0f}%){bonus}"
            )

    return "\n".join(lines)


# ── 현황 한 줄 (daily_report용) ────────────
def rec_summary_line(tf: str, rec: dict) -> str:
    v = rec["verdict"]
    if v == "NO_DATA":
        return f"• {tf}: 🔒 보류 ({rec.get('reason', '표본 부족')})"
    if v == "MIXED":
        return f"• {tf}: ⚪ 방향 모호 (L:{rec.get('long_score')} / S:{rec.get('short_score')})"
    is_long = rec["direction"] == "LONG"
    arrow = "▲" if is_long else "▼"
    strong = "강한" if v.startswith("STRONG") else "약한"
    icon = ("🟢" if is_long else "🔴") if v.startswith("STRONG") else "🟡"
    return f"• {tf}: {icon} {strong} {arrow} {rec['direction']} (신뢰도 {rec['confidence_pct']}%)"


def build_autosetup_section() -> str:
    """daily_report 현황 섹션 — 1D/4H 추천 한 줄씩"""
    lines = ["⚡ *자동 셋업 추천 (BTC)*"]
    for tf in ["1D", "4H"]:
        row = fetch_latest_bar(tf)
        if not row:
            lines.append(f"• {tf}: 데이터 없음")
            continue
        try:
            rec = compute_recommendation(row, tf)
            lines.append(rec_summary_line(tf, rec))
        except Exception as e:
            logger.error(f"[rec] {tf} 현황 계산 실패: {e}")
            lines.append(f"• {tf}: 계산 오류")
    return "\n".join(lines)


# ── 중복 방지 — 이미 푸시한 봉인지 확인 ──────
def already_pushed(tf: str, bar_ts: str) -> bool:
    sb = get_client()
    try:
        res = (
            sb.table("rec_push_log")
            .select("id")
            .eq("timeframe", tf)
            .eq("bar_ts", bar_ts)
            .limit(1)
            .execute()
        )
        return bool(res.data)
    except Exception as e:
        logger.error(f"[rec] push_log 조회 실패: {e}")
        return True  # 조회 실패 시 안전하게 푸시 안 함


def mark_pushed(tf: str, bar_ts: str, rec: dict):
    sb = get_client()
    try:
        sb.table("rec_push_log").insert({
            "timeframe": tf,
            "bar_ts": bar_ts,
            "verdict": rec["verdict"],
            "confidence_pct": rec["confidence_pct"],
            "direction": rec["direction"],
        }).execute()
    except Exception as e:
        logger.error(f"[rec] push_log 기록 실패: {e}")


# ── 추천 체크 + 푸시 ───────────────────────
async def check_and_push():
    """최신 1D/4H 봉 추천 계산 → 65%+면 Telegram 푸시 (중복 방지)"""
    for tf in ["1D", "4H"]:
        row = fetch_latest_bar(tf)
        if not row:
            continue

        bar_ts = row.get("bar_ts")
        if already_pushed(tf, bar_ts):
            continue

        try:
            rec = compute_recommendation(row, tf)
        except Exception as e:
            logger.error(f"[rec] {tf} 추천 계산 실패: {e}", exc_info=True)
            continue

        # STRONG만 푸시 (신뢰도 65%+)
        if not rec["verdict"].startswith("STRONG"):
            continue
        if rec["confidence_pct"] < PUSH_THRESHOLD:
            continue

        msg = build_push_message(tf, rec)
        await send_message(msg)
        mark_pushed(tf, bar_ts, rec)
        # 추적 행 생성 (rec_performance) — STRONG 푸시 1회당 1건
        try:
            create_rec_performance_row(tf, bar_ts, rec, datetime.now(timezone.utc))
        except Exception as e:
            logger.error(f"[rec] 추적 행 생성 실패: {e}")
        logger.info(f"[rec] {tf} 추천 푸시 완료 — {rec['verdict']} {rec['confidence_pct']}%")


# ── 백그라운드 루프 ─────────────────────────
async def recommendation_loop():
    """UTC 4H 정각 + 7분마다 추천 체크 (evaluator보다 2분 늦게 — 봉 마감 후 안정화)"""
    logger.info("[rec] 추천 워커 시작")

    await asyncio.sleep(60)  # 부팅 후 1분 대기
    try:
        await check_and_push()
    except Exception as e:
        logger.error(f"[rec] 초기 실행 실패: {e}", exc_info=True)

    while True:
        now = datetime.now(timezone.utc)
        next_block = (now.hour // 4 + 1) * 4
        if next_block >= 24:
            next_run = now.replace(hour=0, minute=7, second=0, microsecond=0) + timedelta(days=1)
        else:
            next_run = now.replace(hour=next_block, minute=7, second=0, microsecond=0)

        sleep_sec = (next_run - now).total_seconds()
        logger.info(f"[rec] 다음 체크 {next_run.isoformat()} ({sleep_sec/60:.0f}분 후)")
        await asyncio.sleep(sleep_sec)

        try:
            await check_and_push()
        except Exception as e:
            logger.error(f"[rec] 실행 실패: {e}", exc_info=True)
