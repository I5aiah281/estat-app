#!/usr/bin/env python3
"""
UD Framework — Seasonality Flags Module (Production)
Reads current date, flags active/approaching validated seasonal windows,
reads Script 1 JSON for DXY context, computes framework alignment.

Outputs:
  - Console summary
  - seasonality_context.txt (LLM context block)
  - seasonality_context.json (structured data)

Validated patterns from:
  - Approach B: Structural + Statistical (3 patterns)
  - Approach A: Monthly Statistical + Bi-weekly Drilldown (7 patterns)
  - Combined: 9 unique actionable windows after dedup

Requirements: No external packages beyond stdlib + json
"""

import json, os
from datetime import datetime, timedelta
from pathlib import Path

# ============================================================
# CONFIGURATION
# ============================================================

OUTPUT_DIR = os.path.expanduser("~/Documents/UD_Framework")
SCRIPT1_JSON = os.path.join(OUTPUT_DIR, "ud_snapshot.json")
LOOKAHEAD_DAYS = 14  # Flag windows approaching within 2 weeks

# ============================================================
# VALIDATED SEASONAL PATTERNS
# 9 windows surviving both Approach A and B validation
# Each has: structural driver, statistical backing, bi-weekly peak
# ============================================================

VALIDATED_PATTERNS = [
    # --- APRIL CLUSTER (3 patterns, same macro driver) ---
    {
        "id": "gbpusd_april",
        "pair": "GBP/USD",
        "direction": "GBP_STRENGTHENS",
        "direction_sign": "POSITIVE",  # GBP/USD rises
        "month": 4,
        "day_start": 1,
        "day_end": 30,
        "peak_window": "Early April (1st-15th)",
        "peak_validated": True,  # H1 passed: t=2.24, 70%
        "avg_bps": 139,
        "median_bps": 158,
        "t_stat": 2.64,
        "consistency": 0.80,
        "years_tested": 20,
        "structural_driver": "UK tax year ends April 5. Corporate and fund repatriation into GBP ahead of deadline. Strongest validated seasonal pattern in the universe.",
        "category": "FISCAL_FLOW",
        "strength": "STRONG",
        "dxy_interaction": "AMPLIFIED_BY_WEAK_USD"
    },
    {
        "id": "audusd_april",
        "pair": "AUD/USD",
        "direction": "AUD_STRENGTHENS",
        "direction_sign": "POSITIVE",
        "month": 4,
        "day_start": 1,
        "day_end": 15,  # Only H1 validated (t=3.15)
        "peak_window": "Early April (1st-15th)",
        "peak_validated": True,
        "avg_bps": 120,  # H1 avg, not full month
        "median_bps": 78,
        "t_stat": 3.15,  # H1 t-stat
        "consistency": 0.79,  # H1 consistency
        "years_tested": 19,
        "structural_driver": "Broad USD weakness in early April from fiscal year-end flows and spring rebalancing lifts non-USD currencies. AUD as risk proxy benefits. Late April dead (-4bps).",
        "category": "REBALANCING",
        "strength": "STRONG",
        "dxy_interaction": "AMPLIFIED_BY_WEAK_USD"
    },
    {
        "id": "dxy_april",
        "pair": "DXY",
        "direction": "USD_WEAKENS",
        "direction_sign": "NEGATIVE",
        "month": 4,
        "day_start": 1,
        "day_end": 30,
        "peak_window": "Full month (spread across April)",
        "peak_validated": False,  # Neither H1 nor H2 passed individually
        "avg_bps": -85,
        "median_bps": -89,
        "t_stat": -1.66,
        "consistency": 0.70,
        "years_tested": 20,
        "structural_driver": "Broadest USD seasonal weakness. Combination of UK/Japan fiscal year-end repatriation, spring rebalancing, and reduced Treasury demand. Confirmed by GBP and AUD April patterns.",
        "category": "REBALANCING",
        "strength": "BORDERLINE",
        "dxy_interaction": "IS_THE_DXY_SIGNAL"
    },
    # --- MAY (2 patterns from Approach B) ---
    {
        "id": "dxy_may",
        "pair": "DXY",
        "direction": "USD_STRENGTHENS",
        "direction_sign": "POSITIVE",
        "month": 5,
        "day_start": 1,
        "day_end": 15,
        "peak_window": "Early-Mid May (1st-15th)",
        "peak_validated": True,
        "avg_bps": 85,
        "median_bps": 95,
        "t_stat": 1.97,
        "consistency": 0.70,
        "years_tested": 20,
        "structural_driver": "US Treasury May quarterly refunding. Large auction issuance creates USD demand. Only refunding quarter that passed validation (Feb/Aug/Nov all failed).",
        "category": "ISSUANCE",
        "strength": "MODERATE",
        "dxy_interaction": "IS_THE_DXY_SIGNAL"
    },
    {
        "id": "eurusd_may",
        "pair": "EUR/USD",
        "direction": "EUR_WEAKENS",
        "direction_sign": "NEGATIVE",
        "month": 5,
        "day_start": 1,
        "day_end": 15,
        "peak_window": "Early-Mid May (1st-15th)",
        "peak_validated": True,
        "avg_bps": -86,
        "median_bps": -66,
        "t_stat": -1.78,
        "consistency": 0.65,
        "years_tested": 20,
        "structural_driver": "Mirror of DXY May strength. Treasury refunding USD demand weighs on EUR/USD. Approach A full-month test failed (t=-0.99), confirming the effect is concentrated in early-mid May.",
        "category": "ISSUANCE",
        "strength": "MODERATE",
        "dxy_interaction": "AMPLIFIED_BY_STRONG_USD"
    },
    # --- JUNE ---
    {
        "id": "usdchf_june",
        "pair": "USD/CHF",
        "direction": "CHF_STRENGTHENS",
        "direction_sign": "NEGATIVE",
        "month": 6,
        "day_start": 1,
        "day_end": 30,
        "peak_window": "Full month (spread across June)",
        "peak_validated": False,  # Neither half passed individually
        "avg_bps": -110,
        "median_bps": -85,
        "t_stat": -2.54,
        "consistency": 0.80,
        "years_tested": 20,
        "structural_driver": "CHF strengthening in June. Likely SNB meeting cycle plus mid-year safe haven rebalancing into CHF. Second highest t-stat in the dataset. Pattern diffuse across full month.",
        "category": "REBALANCING",
        "strength": "STRONG",
        "dxy_interaction": "AMPLIFIED_BY_WEAK_USD"
    },
    # --- JULY ---
    {
        "id": "usdjpy_july",
        "pair": "USD/JPY",
        "direction": "JPY_STRENGTHENS",
        "direction_sign": "NEGATIVE",
        "month": 7,
        "day_start": 16,
        "day_end": 31,  # Only H2 validated
        "peak_window": "Late July (16th-31st)",
        "peak_validated": True,
        "avg_bps": -76,  # H2 avg
        "median_bps": -118,  # Full month median
        "t_stat": -1.90,  # H2 t-stat
        "consistency": 0.65,  # H2 consistency
        "years_tested": 20,
        "structural_driver": "JPY strengthening in late July. Japanese institutional mid-year portfolio adjustments. Weakest validated pattern (60% monthly consistency). Early July dead (-11bps).",
        "category": "FISCAL_FLOW",
        "strength": "WEAK",
        "dxy_interaction": "AMPLIFIED_BY_WEAK_USD"
    },
    # --- AUGUST ---
    {
        "id": "audusd_august",
        "pair": "AUD/USD",
        "direction": "AUD_WEAKENS",
        "direction_sign": "NEGATIVE",
        "month": 8,
        "day_start": 1,
        "day_end": 15,  # Only H1 validated
        "peak_window": "Early August (1st-15th)",
        "peak_validated": True,
        "avg_bps": -106,  # H1 avg
        "median_bps": -152,  # Full month median
        "t_stat": -1.95,  # H1 t-stat
        "consistency": 0.70,  # H1 consistency
        "years_tested": 20,
        "structural_driver": "AUD weakness in early August. Risk-off seasonality (Aug historically volatile for equities) combined with reduced Asian demand during northern hemisphere summer. AUD as risk proxy gets hit. Late August fades (-22bps).",
        "category": "RISK_REGIME",
        "strength": "MODERATE",
        "dxy_interaction": "AMPLIFIED_BY_STRONG_USD"
    },
    # --- DECEMBER ---
    {
        "id": "usdchf_december",
        "pair": "USD/CHF",
        "direction": "CHF_STRENGTHENS",
        "direction_sign": "NEGATIVE",
        "month": 12,
        "day_start": 16,
        "day_end": 31,  # Only H2 validated
        "peak_window": "Late December (16th-31st)",
        "peak_validated": True,
        "avg_bps": -115,  # H2 avg
        "median_bps": -142,  # Full month median
        "t_stat": -2.15,  # H2 t-stat
        "consistency": 0.85,  # H2 consistency
        "years_tested": 20,
        "structural_driver": "CHF strengthening in late December. Year-end safe haven rebalancing and Swiss repatriation. Highest consistency in the dataset (85%). Early December weaker (-45bps, failed).",
        "category": "REBALANCING",
        "strength": "MODERATE",
        "dxy_interaction": "AMPLIFIED_BY_WEAK_USD"
    }
]


# ============================================================
# DATE LOGIC
# ============================================================

def get_window_status(pattern, today):
    """Determine if a seasonal window is ACTIVE, APPROACHING, or INACTIVE."""
    year = today.year
    m = pattern["month"]
    d_start = pattern["day_start"]
    d_end = pattern["day_end"]

    # Build window start/end dates
    # Handle months with fewer days
    import calendar
    max_day = calendar.monthrange(year, m)[1]
    d_end_actual = min(d_end, max_day)

    window_start = datetime(year, m, d_start)
    window_end = datetime(year, m, d_end_actual)

    # Check if currently active
    if window_start <= today <= window_end:
        days_remaining = (window_end - today).days
        return "ACTIVE", days_remaining, window_start, window_end

    # Check if approaching (within lookahead)
    days_until = (window_start - today).days

    # Handle year wrap (e.g., checking December patterns in November)
    if days_until < 0:
        # Window already passed this year, check next year
        next_year_start = datetime(year + 1, m, d_start)
        days_until = (next_year_start - today).days
        window_start = next_year_start
        max_day_next = calendar.monthrange(year + 1, m)[1]
        window_end = datetime(year + 1, m, min(d_end, max_day_next))

    if 0 < days_until <= LOOKAHEAD_DAYS:
        return "APPROACHING", days_until, window_start, window_end

    return "INACTIVE", days_until, window_start, window_end


# ============================================================
# FRAMEWORK ALIGNMENT
# ============================================================

def read_dxy_context():
    """Read Script 1 JSON for DXY bias context."""
    if not os.path.exists(SCRIPT1_JSON):
        return None

    try:
        with open(SCRIPT1_JSON) as f:
            data = json.load(f)

        # Try to extract DXY-relevant signals
        context = {}

        # Look for rate differentials
        yields = data.get("yields_credit", {})
        if yields:
            dgs2 = yields.get("DGS2", {})
            dgs10 = yields.get("DGS10", {})
            if dgs2:
                context["us_2y"] = dgs2.get("value")
            if dgs10:
                context["us_10y"] = dgs10.get("value")

        # Look for NFCI
        monetary = data.get("monetary_liquidity", {})
        if monetary:
            nfci = monetary.get("NFCI", {})
            if nfci:
                context["nfci"] = nfci.get("value")

        # Look for COT DXY positioning
        cot = data.get("cot_percentiles", {})
        if cot:
            context["cot_percentiles"] = cot

        # Snapshot date
        context["snapshot_date"] = data.get("collection_date", "unknown")

        return context if context else None

    except Exception as e:
        return None


def read_gap_context():
    """Read gap module JSON for market expectations."""
    gap_path = os.path.join(OUTPUT_DIR, "fed_policy_gap.json")
    if not os.path.exists(gap_path):
        return None

    try:
        with open(gap_path) as f:
            data = json.load(f)
        return {
            "gap_classification": data.get("gap_classification"),
            "gap_score": data.get("gap_score"),
            "trade_implication": data.get("trade_implication")
        }
    except:
        return None


def compute_alignment(pattern, dxy_context, gap_context):
    """
    Compute whether the macro framework REINFORCES or CONFLICTS
    with the seasonal pattern.

    Logic:
    - For patterns where USD weakens (DXY down, cross pairs up):
      Framework USD bearish = REINFORCING
      Framework USD bullish = CONFLICTING
    - For patterns where USD strengthens (DXY up, cross pairs down):
      Framework USD bullish = REINFORCING
      Framework USD bearish = CONFLICTING
    - If no framework data available: UNKNOWN
    """
    if not gap_context:
        return "UNKNOWN", "No gap module data available. Run fed_policy_gap.py first."

    gap_class = gap_context.get("gap_classification", "")
    gap_score = gap_context.get("gap_score", 0)

    interaction_type = pattern["dxy_interaction"]

    if interaction_type == "IS_THE_DXY_SIGNAL":
        # This IS the DXY pattern — alignment depends on whether
        # the framework agrees with the seasonal direction
        if pattern["direction_sign"] == "NEGATIVE":  # DXY seasonal says USD weak
            if gap_class == "MARKET_DOVISH":
                # Market too dovish = USD likely stronger than market thinks
                return "CONFLICTING", f"Seasonal says USD weak in {_month_name(pattern['month'])}, but gap module says MARKET_DOVISH (gap score {gap_score}) — market may be underpricing USD strength."
            elif gap_class == "MARKET_HAWKISH":
                return "REINFORCING", f"Seasonal says USD weak and gap module says MARKET_HAWKISH (gap score {gap_score}) — both point to USD downside."
            else:
                return "NEUTRAL", f"Gap module says ALIGNED (gap score {gap_score}). Seasonal is the tiebreaker — mild USD weakness bias."
        else:  # DXY seasonal says USD strong
            if gap_class == "MARKET_DOVISH":
                return "REINFORCING", f"Seasonal says USD strong and gap module says MARKET_DOVISH (gap score {gap_score}) — both point to USD upside when market reprices."
            elif gap_class == "MARKET_HAWKISH":
                return "CONFLICTING", f"Seasonal says USD strong in {_month_name(pattern['month'])}, but gap module says MARKET_HAWKISH (gap score {gap_score}) — macro headwind."
            else:
                return "NEUTRAL", f"Gap module says ALIGNED. Seasonal provides mild USD strength bias."

    elif interaction_type == "AMPLIFIED_BY_WEAK_USD":
        # Pattern benefits from USD weakness
        if gap_class == "MARKET_HAWKISH":
            return "REINFORCING", f"Seasonal {pattern['pair']} tailwind + gap module MARKET_HAWKISH (gap score {gap_score}) suggests USD weakness ahead. Seasonal AMPLIFIED."
        elif gap_class == "MARKET_DOVISH":
            return "CONFLICTING", f"Seasonal {pattern['pair']} tailwind active but gap module MARKET_DOVISH (gap score {gap_score}) — USD may be stronger than market expects. Seasonal headwind from macro."
        else:
            return "NEUTRAL", f"Gap ALIGNED. Seasonal flow is the marginal factor."

    elif interaction_type == "AMPLIFIED_BY_STRONG_USD":
        # Pattern benefits from USD strength
        if gap_class == "MARKET_DOVISH":
            return "REINFORCING", f"Seasonal {pattern['pair']} pressure + gap module MARKET_DOVISH (gap score {gap_score}) — USD strength repricing reinforces seasonal weakness."
        elif gap_class == "MARKET_HAWKISH":
            return "CONFLICTING", f"Seasonal {pattern['pair']} pressure active but gap module MARKET_HAWKISH (gap score {gap_score}) — USD weakness may offset seasonal."
        else:
            return "NEUTRAL", f"Gap ALIGNED. Seasonal is the marginal factor."

    return "UNKNOWN", "Could not determine alignment."


def _month_name(m):
    names = ["", "January", "February", "March", "April", "May", "June",
             "July", "August", "September", "October", "November", "December"]
    return names[m]


# ============================================================
# OUTPUT FORMATTING
# ============================================================

def format_console_output(flags, dxy_context, gap_context):
    """Print console summary."""
    today = datetime.now()

    print(f"\n{'='*60}")
    print(f"SEASONALITY FLAGS — {today.strftime('%B %d, %Y')}")
    print(f"{'='*60}")

    if gap_context:
        gc = gap_context.get("gap_classification", "N/A")
        gs = gap_context.get("gap_score", "N/A")
        print(f"\n  Framework context: Gap = {gc} (score: {gs})")
    else:
        print(f"\n  Framework context: No gap data available")

    active = [f for f in flags if f["status"] == "ACTIVE"]
    approaching = [f for f in flags if f["status"] == "APPROACHING"]
    inactive_count = len(flags) - len(active) - len(approaching)

    if not active and not approaching:
        print(f"\n  No seasonal windows active or approaching.")
        print(f"  All {len(flags)} validated patterns are inactive.")

        # Show next upcoming
        upcoming = sorted(
            [f for f in flags if f["status"] == "INACTIVE"],
            key=lambda x: x["days_until"]
        )
        if upcoming:
            nxt = upcoming[0]
            print(f"\n  Next window: {nxt['pair']} {nxt['peak_window']} in {nxt['days_until']} days")
        return

    if active:
        print(f"\n  ACTIVE SEASONAL WINDOWS ({len(active)}):")
        print(f"  {'─'*55}")
        for f in active:
            icon = "▲" if f["direction_sign"] == "POSITIVE" else "▼"
            print(f"\n  {icon} {f['pair']} — {f['direction']}")
            print(f"    Window: {f['peak_window']} | {f['days_remaining']} days remaining")
            print(f"    Stats: {f['avg_bps']:+d}bps avg | t={f['t_stat']:.2f} | {f['consistency']:.0%} consistent | {f['years_tested']}yr")
            print(f"    Strength: {f['strength']}")
            print(f"    Driver: {f['structural_driver'][:80]}...")
            print(f"    Framework alignment: {f['alignment']} — {f['alignment_reason'][:80]}")

            # Execution guidance
            if f["alignment"] == "REINFORCING":
                print(f"    → EXECUTION: Seasonal + macro aligned. Higher conviction window. Consider sizing up or timing entry for this window.")
            elif f["alignment"] == "CONFLICTING":
                print(f"    → EXECUTION: Seasonal fighting macro. Expect reduced magnitude or failure. Do NOT override UD1/UD2 for seasonal pattern.")
            else:
                print(f"    → EXECUTION: Neutral macro backdrop. Seasonal is marginal factor. Standard sizing.")

    if approaching:
        print(f"\n  APPROACHING SEASONAL WINDOWS ({len(approaching)}):")
        print(f"  {'─'*55}")
        for f in approaching:
            icon = "▲" if f["direction_sign"] == "POSITIVE" else "▼"
            print(f"\n  {icon} {f['pair']} — {f['direction']} (in {f['days_until']} days)")
            print(f"    Window: {f['peak_window']}")
            print(f"    Stats: {f['avg_bps']:+d}bps avg | {f['consistency']:.0%} consistent | Strength: {f['strength']}")
            print(f"    Pre-positioning note: {f['structural_driver'][:80]}...")


def format_llm_context(flags, dxy_context, gap_context):
    """Format context block for LLM snapshot."""
    today = datetime.now()
    lines = []
    a = lines.append

    a(f"SEASONAL CONTEXT (Week of {today.strftime('%B %d, %Y')})")
    a(f"  Source: 20-year validation, Approach A (monthly) + Approach B (structural)")
    a(f"  Rule: Seasonality modifies EXECUTION ONLY — never overrides UD1/UD2 direction")
    a("")

    if gap_context:
        gc = gap_context.get("gap_classification", "N/A")
        gs = gap_context.get("gap_score", "N/A")
        a(f"  Framework backdrop: Gap = {gc} (score: {gs})")
        a("")

    active = [f for f in flags if f["status"] == "ACTIVE"]
    approaching = [f for f in flags if f["status"] == "APPROACHING"]

    if not active and not approaching:
        a("  STATUS: No seasonal windows active or approaching.")
        upcoming = sorted(
            [f for f in flags if f["status"] == "INACTIVE"],
            key=lambda x: x["days_until"]
        )
        if upcoming:
            nxt = upcoming[0]
            a(f"  Next window: {nxt['pair']} {nxt['peak_window']} in {nxt['days_until']} days")
        a("")
        a("  No seasonal adjustment needed this week.")
        return "\n".join(lines)

    if active:
        a("  ACTIVE WINDOWS:")
        for f in active:
            icon = "▲" if f["direction_sign"] == "POSITIVE" else "▼"
            a(f"    {icon} {f['pair']}: {f['direction']}")
            a(f"      Peak: {f['peak_window']} | {f['days_remaining']} days remaining")
            a(f"      Historical: {f['avg_bps']:+d}bps avg | {f['consistency']:.0%} consistent over {f['years_tested']} years | Strength: {f['strength']}")
            a(f"      Driver: {f['structural_driver']}")
            a(f"      Framework alignment: {f['alignment']}")
            a(f"      Alignment detail: {f['alignment_reason']}")

            if f["alignment"] == "REINFORCING":
                a(f"      EXECUTION ADJUSTMENT: Seasonal reinforces macro thesis. Higher conviction window for {f['pair']} trades in the seasonal direction. Consider timing entries within this window and/or modest size increase.")
            elif f["alignment"] == "CONFLICTING":
                a(f"      EXECUTION ADJUSTMENT: Seasonal conflicts with macro thesis. Do NOT override UD1/UD2 direction. Expect the seasonal pattern to be muted or fail this year. If positioned against the seasonal, expect temporary chop but hold if macro thesis intact.")
            else:
                a(f"      EXECUTION ADJUSTMENT: Neutral macro context. Seasonal is a marginal tiebreaker. No size adjustment, but can inform entry timing within the week.")
            a("")

    if approaching:
        a("  APPROACHING (within 2 weeks):")
        for f in approaching:
            icon = "▲" if f["direction_sign"] == "POSITIVE" else "▼"
            a(f"    {icon} {f['pair']}: {f['direction']} (starts in {f['days_until']} days)")
            a(f"      Window: {f['peak_window']}")
            a(f"      Historical: {f['avg_bps']:+d}bps avg | {f['consistency']:.0%} consistent | Strength: {f['strength']}")
            a(f"      Driver: {f['structural_driver']}")
            a(f"      Pre-positioning: Be aware of this window when planning {f['pair']} entries/exits over the next 2 weeks.")
            a("")

    return "\n".join(lines)


# ============================================================
# MAIN
# ============================================================

def main():
    today = datetime.now()

    print(f"\n{'='*60}")
    print(f"UD FRAMEWORK — SEASONALITY FLAGS")
    print(f"Date: {today.strftime('%A, %B %d, %Y')}")
    print(f"Validated patterns: {len(VALIDATED_PATTERNS)}")
    print(f"Lookahead: {LOOKAHEAD_DAYS} days")
    print(f"{'='*60}")

    # Step 1: Read framework context
    print(f"\n  Reading framework context...")
    dxy_context = read_dxy_context()
    gap_context = read_gap_context()

    if dxy_context:
        print(f"    Script 1 JSON: loaded (snapshot: {dxy_context.get('snapshot_date', 'unknown')})")
    else:
        print(f"    Script 1 JSON: not found at {SCRIPT1_JSON}")
        print(f"    (Run ud_fundamental_collector.py first for full alignment assessment)")

    if gap_context:
        print(f"    Gap module: {gap_context.get('gap_classification')} (score: {gap_context.get('gap_score')})")
    else:
        print(f"    Gap module: not found")
        print(f"    (Run fed_policy_gap.py first for full alignment assessment)")

    # Step 2: Check each pattern
    flags = []
    for pattern in VALIDATED_PATTERNS:
        status, days, w_start, w_end = get_window_status(pattern, today)

        alignment, alignment_reason = compute_alignment(pattern, dxy_context, gap_context)

        flag = {
            "id": pattern["id"],
            "pair": pattern["pair"],
            "direction": pattern["direction"],
            "direction_sign": pattern["direction_sign"],
            "status": status,
            "month": pattern["month"],
            "peak_window": pattern["peak_window"],
            "avg_bps": pattern["avg_bps"],
            "median_bps": pattern["median_bps"],
            "t_stat": pattern["t_stat"],
            "consistency": pattern["consistency"],
            "years_tested": pattern["years_tested"],
            "structural_driver": pattern["structural_driver"],
            "category": pattern["category"],
            "strength": pattern["strength"],
            "alignment": alignment,
            "alignment_reason": alignment_reason,
            "window_start": w_start.strftime("%Y-%m-%d"),
            "window_end": w_end.strftime("%Y-%m-%d")
        }

        if status == "ACTIVE":
            flag["days_remaining"] = days
        else:
            flag["days_until"] = days

        flags.append(flag)

    # Step 3: Console output
    format_console_output(flags, dxy_context, gap_context)

    # Step 4: Save LLM context
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    llm_text = format_llm_context(flags, dxy_context, gap_context)
    txt_path = os.path.join(OUTPUT_DIR, "seasonality_context.txt")
    with open(txt_path, "w") as f:
        f.write(llm_text)
    print(f"\n  LLM context saved: {txt_path}")

    # Step 5: Save JSON
    json_path = os.path.join(OUTPUT_DIR, "seasonality_context.json")
    with open(json_path, "w") as f:
        json.dump({
            "generated": today.isoformat(),
            "framework_context": {
                "gap_classification": gap_context.get("gap_classification") if gap_context else None,
                "gap_score": gap_context.get("gap_score") if gap_context else None,
                "snapshot_date": dxy_context.get("snapshot_date") if dxy_context else None
            },
            "flags": flags,
            "active_count": len([f for f in flags if f["status"] == "ACTIVE"]),
            "approaching_count": len([f for f in flags if f["status"] == "APPROACHING"])
        }, f, indent=2)
    print(f"  JSON saved: {json_path}")

    # Step 6: Summary
    active = [f for f in flags if f["status"] == "ACTIVE"]
    approaching = [f for f in flags if f["status"] == "APPROACHING"]

    print(f"\n{'='*60}")
    print(f"SUMMARY: {len(active)} active | {len(approaching)} approaching | {len(flags) - len(active) - len(approaching)} inactive")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
