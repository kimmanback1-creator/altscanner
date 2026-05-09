# ══════════════════════════════════════════
#  notify/daily_report.py – 일일/주간 텔레그램 리포트
#  KST 09:00 cron으로 호출됨 (Render Cron Job)
# ══════════════════════════════════════════
import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# 프로젝트 루트 import path 설정
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.supabase import get_client
from notify.telegram import send_message

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))


# ── 포맷 헬퍼 ─────────────────────────────
def fmt_usd(v):
    if v is None:
        return "—"
    sign = "+" if v >= 0 else "-"
    return f"{sign}${abs(v):.2f}"


def fmt_pct(v, decimals=2):
    if v is None:
        return "—"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.{decimals}f}%"


def parse_ai_judge(opinion):
    if not opinion:
        return None
    for emoji in ["🟢", "🟡", "🔴"]:
        if emoji in opinion:
            return emoji
    return None


def classify_flow(price_chg, cvd_delta, oi_chg):
    """
    가격/CVD/OI 변화로 매집/분배 자동 라벨
    각 메이저(BTC/ETH/SOL) 24h 변화량으로 분류
    """
    if price_chg is None:
        return "⚪ 데이터 부족"

    p_threshold = 0.5
    p_up = price_chg > p_threshold
    p_dn = price_chg < -p_threshold
    c_up = (cvd_delta or 0) > 0
    o_up = (oi_chg or 0) > 0

    if p_up and c_up and o_up:
        return "🟢 신규 매수 유입"
    if p_up and c_up and not o_up:
        return "🔵 매집 (숏 손절)"
    if p_up and not c_up:
        return "🟡 약한 상승 (의심)"
    if p_dn and not c_up and o_up:
        return "🔴 분배 (숏 진입)"
    if p_dn and not c_up and not o_up:
        return "🟠 청산 동반 하락"
    if p_dn and c_up:
        return "🟣 매집 가능성 (역행)"
    return "⚪ 횡보"


# ── 데이터 fetch ──────────────────────────
def fetch_trades_in_range(start_iso, end_iso):
    """진입 시각 기준 [start, end) 범위 거래"""
    try:
        res = get_client().table("trade_journal") \
            .select("*") \
            .gte("created_at", start_iso) \
            .lt("created_at", end_iso) \
            .execute()
        return res.data or []
    except Exception as e:
        logger.error(f"[Report] fetch_trades 실패: {e}")
        return []


def fetch_closed_in_range(start_iso, end_iso):
    """청산 시각 기준 [start, end) 범위 거래"""
    try:
        res = get_client().table("trade_journal") \
            .select("*") \
            .eq("status", "closed") \
            .gte("closed_at", start_iso) \
            .lt("closed_at", end_iso) \
            .execute()
        return res.data or []
    except Exception as e:
        logger.error(f"[Report] fetch_closed 실패: {e}")
        return []


def fetch_open_positions():
    try:
        res = get_client().table("trade_journal") \
            .select("id") \
            .eq("status", "open") \
            .execute()
        return res.data or []
    except Exception as e:
        logger.error(f"[Report] fetch_open 실패: {e}")
        return []


def fetch_total_stats():
    """전체 누적 통계 (모든 closed)"""
    try:
        res = get_client().table("trade_journal") \
            .select("pnl_usd") \
            .eq("status", "closed") \
            .execute()
        rows = res.data or []
        if not rows:
            return {"total": 0, "wins": 0, "total_pnl": 0.0, "win_rate": 0.0}
        total_pnl = sum((r.get("pnl_usd") or 0) for r in rows)
        wins = sum(1 for r in rows if (r.get("pnl_usd") or 0) > 0)
        return {
            "total": len(rows),
            "wins": wins,
            "total_pnl": total_pnl,
            "win_rate": (wins / len(rows) * 100),
        }
    except Exception as e:
        logger.error(f"[Report] fetch_total 실패: {e}")
        return {"total": 0, "wins": 0, "total_pnl": 0.0, "win_rate": 0.0}


def fetch_major_range(start_ts, end_ts):
    """
    [start_ts, end_ts) 범위의 major_hourly 조회
    BTC/ETH/SOL 각각의 24h 누적 흐름 계산용
    ts 컬럼은 unix초 정수
    """
    try:
        res = get_client().table("major_hourly") \
            .select("symbol, ts, price, price_chg, cvd_delta, oi_chg, vol_candle, diagnosis") \
            .gte("ts", start_ts) \
            .lt("ts", end_ts) \
            .order("ts", desc=False) \
            .execute()
        return res.data or []
    except Exception as e:
        logger.error(f"[Report] fetch_major 실패: {e}")
        return []


def fetch_alt_24h_changes(end_ts):
    """
    가장 최근 15m 캔들 사용 — ts 필터 없이 최신 데이터 우선
    cron이 09:00에 돌 때 직전 캔들이 가장 최신이라 자연스럽게 09:00 시점 반영
    각 (exchange, symbol)별 최신 1건씩
    """
    try:
        res = get_client().table("candle_data") \
            .select("exchange, symbol, ts, price_chg_24h, diagnosis, cvd_pct, oi_pct, vol_pct, price") \
            .eq("timeframe", "15m") \
            .order("ts", desc=True) \
            .limit(5000) \
            .execute()
        rows = res.data or []
        # (exchange, symbol)별 가장 최신 1건만
        latest = {}
        for r in rows:
            key = (r["exchange"], r["symbol"])
            if key not in latest:
                latest[key] = r
        
        # 디버깅: 양수/음수 분포 로깅
        all_changes = [r["price_chg_24h"] for r in latest.values() if r.get("price_chg_24h") is not None]
        pos_count = sum(1 for v in all_changes if v > 0)
        neg_count = sum(1 for v in all_changes if v < 0)
        logger.info(f"[Report] 알트 데이터: 총 {len(all_changes)}개 (상승 {pos_count} / 하락 {neg_count})")
        
        return list(latest.values())
    except Exception as e:
        logger.error(f"[Report] fetch_alt 실패: {e}")
        return []


# ── 메이저 흐름 요약 ─────────────────────
def summarize_majors(major_rows):
    """
    BTC/ETH/SOL 각각의 24h 누적 가격 변화 + CVD/OI 합계 + 흐름 분류
    """
    by_base = {"BTC": [], "ETH": [], "SOL": []}
    for r in major_rows:
        sym = (r.get("symbol") or "").upper()
        for base in ("BTC", "ETH", "SOL"):
            if sym.startswith(base):
                by_base[base].append(r)
                break

    summary = {}
    for base, rows in by_base.items():
        if not rows:
            summary[base] = None
            continue

        # 거래소 무관, 시간순 정렬됨
        # 24h 누적: 첫 가격 vs 마지막 가격
        sorted_rows = sorted(rows, key=lambda x: x.get("ts") or 0)
        first_price = sorted_rows[0].get("price")
        last_price = sorted_rows[-1].get("price")
        price_chg_24h = None
        if first_price and last_price and first_price > 0:
            price_chg_24h = (last_price - first_price) / first_price * 100

        cvd_sum = sum((r.get("cvd_delta") or 0) for r in rows)
        oi_sum = sum((r.get("oi_chg") or 0) for r in rows)

        summary[base] = {
            "price": last_price,
            "price_chg_24h": price_chg_24h,
            "cvd_sum": cvd_sum,
            "oi_sum": oi_sum,
            "flow_label": classify_flow(price_chg_24h, cvd_sum, oi_sum),
            "count": len(rows),
        }
    return summary


def fmt_major_block(majors):
    lines = []
    for base in ("BTC", "ETH", "SOL"):
        s = majors.get(base)
        if not s or s["price_chg_24h"] is None:
            lines.append(f"• {base}: 데이터 없음")
            continue
        cvd_arrow = "↑" if s["cvd_sum"] > 0 else "↓"
        oi_arrow = "↑" if s["oi_sum"] > 0 else "↓"
        lines.append(
            f"• *{base}*: {fmt_pct(s['price_chg_24h'], 2)} "
            f"(CVD {cvd_arrow} / OI {oi_arrow})\n"
            f"  → {s['flow_label']}"
        )
    return "\n".join(lines)


# ── 알트 TOP ─────────────────────────────
def is_major_symbol(symbol):
    s = (symbol or "").upper()
    for base in ("BTC", "ETH", "SOL"):
        if s.startswith(base):
            return True
    return False


def top_alts(alt_rows, n=5, ascending=False):
    """
    price_chg_24h 기준 상위(상승) 또는 하위(하락) N개
    같은 베이스 심볼이 거래소별 중복되면 가장 적합한 변화율만 유지
    - 상승 모드: 가장 큰 양수
    - 하락 모드: 가장 작은 음수
    """
    # 메이저 제외 + 변화율 있는 것만
    rows = [r for r in alt_rows if not is_major_symbol(r.get("symbol")) and r.get("price_chg_24h") is not None]

    # 모드별로 적합한 것만 필터: 상승은 양수, 하락은 음수
    if ascending:
        # 하락 — 음수만
        rows = [r for r in rows if r["price_chg_24h"] < 0]
    else:
        # 상승 — 양수만
        rows = [r for r in rows if r["price_chg_24h"] > 0]

    # 베이스 심볼별 1건만 — 모드에 맞게 극단값 유지
    by_base = {}
    for r in rows:
        sym = r["symbol"]
        # OKX는 "BTC-USDT-SWAP" 같은 형식 → "BTC" 추출
        base = sym.split("-")[0] if "-" in sym else sym.replace("USDT", "")
        cur = by_base.get(base)
        if cur is None:
            by_base[base] = {**r, "_base": base}
            continue
        # 상승: 더 큰 값 / 하락: 더 작은 값
        if ascending:
            if r["price_chg_24h"] < cur["price_chg_24h"]:
                by_base[base] = {**r, "_base": base}
        else:
            if r["price_chg_24h"] > cur["price_chg_24h"]:
                by_base[base] = {**r, "_base": base}

    sorted_rows = sorted(by_base.values(), key=lambda x: x["price_chg_24h"], reverse=not ascending)
    return sorted_rows[:n]


def fmt_alt_block(alts, mode="up"):
    if not alts:
        if mode == "up":
            return "  (24h 상승 종목 없음)"
        else:
            return "  (24h 하락 종목 없음)"
    lines = []
    ex_map = {"binance": "BNC", "okx": "OKX", "bybit": "BYB"}
    for i, a in enumerate(alts, 1):
        ex = ex_map.get(a.get("exchange"), a.get("exchange", "?"))
        diag = a.get("diagnosis") or "—"
        lines.append(
            f"  {i}. {a['_base']} {fmt_pct(a['price_chg_24h'], 1)} ({ex}) {diag}"
        )
    return "\n".join(lines)


# ── 일일 리포트 빌더 ─────────────────────
async def build_daily_report(start_kst, end_kst):
    """
    [start_kst, end_kst) 범위 (KST 기준 24h)
    """
    start_iso = start_kst.astimezone(timezone.utc).isoformat()
    end_iso = end_kst.astimezone(timezone.utc).isoformat()
    start_ts = int(start_kst.timestamp())
    end_ts = int(end_kst.timestamp())

    # 데이터 fetch
    new_entries = fetch_trades_in_range(start_iso, end_iso)
    closed_today = fetch_closed_in_range(start_iso, end_iso)
    open_pos = fetch_open_positions()
    total = fetch_total_stats()

    # 메이저 + 알트
    major_rows = fetch_major_range(start_ts, end_ts)
    majors = summarize_majors(major_rows)
    alt_rows = fetch_alt_24h_changes(end_ts)
    top_up = top_alts(alt_rows, n=5, ascending=False)
    top_dn = top_alts(alt_rows, n=5, ascending=True)

    # 거래 통계
    long_count = sum(1 for t in new_entries if t.get("direction") == "LONG")
    short_count = sum(1 for t in new_entries if t.get("direction") == "SHORT")
    wins_today = [t for t in closed_today if (t.get("pnl_usd") or 0) > 0]
    losses_today = [t for t in closed_today if (t.get("pnl_usd") or 0) < 0]
    daily_pnl = sum((t.get("pnl_usd") or 0) for t in closed_today)
    contra_entries = [t for t in new_entries if parse_ai_judge(t.get("ai_opinion")) == "🔴"]

    # 텍스트 빌드
    weekday_kr = ["월", "화", "수", "목", "금", "토", "일"][start_kst.weekday()]
    lines = [
        f"📊 *{start_kst.month}/{start_kst.day} ({weekday_kr}) 매매 리포트*",
        f"_{start_kst.strftime('%H:%M')} ~ {end_kst.strftime('%m/%d %H:%M')} KST_",
        "",
        "📈 *거래 요약*",
        f"• 신규 진입: {len(new_entries)}건 (LONG {long_count} / SHORT {short_count})",
        f"• 청산: {len(closed_today)}건 (승 {len(wins_today)} / 패 {len(losses_today)})",
        f"• 일일 PnL: *{fmt_usd(daily_pnl)}*",
        "",
        "📌 *현재 상태*",
        f"• 오픈 포지션: {len(open_pos)}건",
        f"• 누적 PnL: *{fmt_usd(total['total_pnl'])}*",
        f"• 전체 승률: {total['win_rate']:.1f}% ({total['wins']}/{total['total']})",
        "",
        "🪙 *메이저 24h 흐름*",
        fmt_major_block(majors),
        "",
        "🚀 *알트 TOP 5 상승*",
        fmt_alt_block(top_up, mode="up"),
        "",
        "📉 *알트 TOP 5 하락*",
        fmt_alt_block(top_dn, mode="down"),
    ]

    # 역행 경고
    if contra_entries:
        lines += [
            "",
            f"🔴 *AI 역행 진입: {len(contra_entries)}건*",
        ]
        for t in contra_entries[:3]:
            lines.append(f"  • {t['symbol']} {t['direction']}")

    # 청산 내역
    if closed_today:
        lines += ["", "▼ *청산 내역*"]
        for i, t in enumerate(closed_today[:5], 1):
            pnl_emoji = "🟢" if (t.get("pnl_usd") or 0) > 0 else "🔴"
            lines.append(
                f"[{i}] {pnl_emoji} {t['symbol']} {t['direction']} "
                f"{fmt_usd(t.get('pnl_usd'))} ({fmt_pct(t.get('pnl_pct'))})"
            )
        if len(closed_today) > 5:
            lines.append(f"  ... 외 {len(closed_today) - 5}건")

    if not new_entries and not closed_today:
        lines += ["", "💤 어제는 거래가 없었어요."]

    return "\n".join(lines)


# ── 주간 리포트 빌더 ─────────────────────
async def build_weekly_report(start_kst, end_kst):
    start_iso = start_kst.astimezone(timezone.utc).isoformat()
    end_iso = end_kst.astimezone(timezone.utc).isoformat()
    start_ts = int(start_kst.timestamp())
    end_ts = int(end_kst.timestamp())

    new_entries = fetch_trades_in_range(start_iso, end_iso)
    closed_week = fetch_closed_in_range(start_iso, end_iso)

    # 데이터 거의 없으면 스킵
    if not new_entries and not closed_week:
        return None

    long_count = sum(1 for t in new_entries if t.get("direction") == "LONG")
    short_count = sum(1 for t in new_entries if t.get("direction") == "SHORT")
    wins = [t for t in closed_week if (t.get("pnl_usd") or 0) > 0]
    losses = [t for t in closed_week if (t.get("pnl_usd") or 0) < 0]
    week_pnl = sum((t.get("pnl_usd") or 0) for t in closed_week)
    win_rate = (len(wins) / len(closed_week) * 100) if closed_week else 0

    best = max(closed_week, key=lambda t: (t.get("pnl_usd") or 0), default=None)
    worst = min(closed_week, key=lambda t: (t.get("pnl_usd") or 0), default=None)

    # 종목별 통계
    sym_stats = {}
    for t in closed_week:
        sym = t["symbol"]
        d = sym_stats.setdefault(sym, {"total": 0, "wins": 0, "pnl": 0})
        d["total"] += 1
        if (t.get("pnl_usd") or 0) > 0:
            d["wins"] += 1
        d["pnl"] += t.get("pnl_usd") or 0

    best_sym = None
    candidates = [(s, d) for s, d in sym_stats.items() if d["total"] >= 2]
    if candidates:
        best_sym = max(candidates, key=lambda x: x[1]["wins"] / x[1]["total"])

    # AI 진단별 통계
    judge_stats = {"🟢": {"total": 0, "wins": 0}, "🟡": {"total": 0, "wins": 0}, "🔴": {"total": 0, "wins": 0}}
    for t in closed_week:
        j = parse_ai_judge(t.get("ai_opinion"))
        if j and j in judge_stats:
            judge_stats[j]["total"] += 1
            if (t.get("pnl_usd") or 0) > 0:
                judge_stats[j]["wins"] += 1

    # 메이저 7일 흐름
    major_rows = fetch_major_range(start_ts, end_ts)
    majors = summarize_majors(major_rows)

    # 알트 7일 변화 — end_ts 시점 기준 24h만 표시 (한 주 누적은 candle_data에서 계산 어려움)
    alt_rows = fetch_alt_24h_changes(end_ts)
    top_up = top_alts(alt_rows, n=5, ascending=False)
    top_dn = top_alts(alt_rows, n=5, ascending=True)

    s_date = start_kst.strftime("%m/%d")
    e_date = (end_kst - timedelta(days=1)).strftime("%m/%d")

    lines = [
        f"📊 *주간 리포트 ({s_date} ~ {e_date})*",
        "",
        "📈 *종합*",
        f"• 거래: {len(new_entries)}건 (LONG {long_count} / SHORT {short_count})",
        f"• 청산: {len(closed_week)}건 (승 {len(wins)} / 패 {len(losses)})",
        f"• 승률: {win_rate:.1f}%",
        f"• 주간 PnL: *{fmt_usd(week_pnl)}*",
        "",
        "🪙 *메이저 주간 흐름 (7일 누적)*",
        fmt_major_block(majors),
    ]

    # 베스트
    if best and (best.get("pnl_usd") or 0) > 0:
        lines += [
            "",
            "🎯 *베스트*",
            f"• 최대 수익: {best['symbol']} {best['direction']} {fmt_usd(best.get('pnl_usd'))}",
        ]
        if best_sym:
            sym, d = best_sym
            wr = d["wins"] / d["total"] * 100
            lines.append(f"• 최고 종목: {sym} (승률 {wr:.0f}%, {d['total']}건)")

    if worst and (worst.get("pnl_usd") or 0) < 0:
        lines += [
            "",
            "⚠️ *워스트*",
            f"• 최대 손실: {worst['symbol']} {worst['direction']} {fmt_usd(worst.get('pnl_usd'))}",
        ]

    has_judge = any(d["total"] > 0 for d in judge_stats.values())
    if has_judge:
        lines += ["", "🤖 *AI 진단별 승률*"]
        for emoji in ("🟢", "🟡", "🔴"):
            d = judge_stats[emoji]
            if d["total"] > 0:
                wr = d["wins"] / d["total"] * 100
                label = {"🟢": "정합", "🟡": "혼합", "🔴": "역행"}[emoji]
                lines.append(f"• {emoji} {label}: {d['total']}건 (승률 {wr:.0f}%)")

    # 알트 — 어제 24h 기준
    lines += [
        "",
        "🚀 *주말 기준 24h 알트 TOP 5 상승*",
        fmt_alt_block(top_up, mode="up"),
        "",
        "📉 *주말 기준 24h 알트 TOP 5 하락*",
        fmt_alt_block(top_dn, mode="down"),
    ]

    return "\n".join(lines)


# ── 메인 ─────────────────────────────────
async def main():
    """
    KST 09:00 cron 실행
    어제 09:00 ~ 오늘 09:00 범위 일일 리포트
    오늘이 일요일이면 지난 일~토 주간 리포트 추가
    """
    now_kst = datetime.now(KST)
    today_9am = now_kst.replace(hour=9, minute=0, second=0, microsecond=0)
    yesterday_9am = today_9am - timedelta(days=1)

    logger.info(f"[Report] 실행 시각: {now_kst.isoformat()}")
    logger.info(f"[Report] 일일 범위: {yesterday_9am} ~ {today_9am}")

    # 일일 리포트
    try:
        daily = await build_daily_report(yesterday_9am, today_9am)
        logger.info(f"[Report] 일일 길이: {len(daily)}자")
        await send_message(daily)
        logger.info("[Report] 일일 전송 완료")
    except Exception as e:
        logger.error(f"[Report] 일일 실패: {e}")
        import traceback
        traceback.print_exc()

    # 일요일이면 주간 리포트
    if today_9am.weekday() == 6:
        week_start = today_9am - timedelta(days=7)
        logger.info(f"[Report] 주간 범위: {week_start} ~ {today_9am}")
        try:
            weekly = await build_weekly_report(week_start, today_9am)
            if weekly:
                await send_message(weekly)
                logger.info("[Report] 주간 전송 완료")
            else:
                logger.info("[Report] 주간 거래 없음 - 스킵")
        except Exception as e:
            logger.error(f"[Report] 주간 실패: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
