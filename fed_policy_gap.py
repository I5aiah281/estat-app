"""
UD Framework — Fed Policy Expectations Gap Module v2.1
=======================================================
Reads Script 1's JSON output automatically. Zero manual input required.

PURPOSE:
  Systematically quantifies the gap between:
    A) What the market expects the Fed to do (from 2Y yield + curve)
    B) What your UD1 fundamental regime says the Fed SHOULD do

USAGE:
  1. Run Script 1 as normal (produces ud_fundamental_YYYYMMDD_HHMM.json)
  2. Run: python fed_policy_gap.py
     Auto-finds the latest JSON in the current directory.
  Or specify: python fed_policy_gap.py ud_fundamental_20260217_2158.json

  FedWatch: Update Section 7 below weekly (30 sec on CME's free website).
  Set FEDWATCH_ENABLED = False to skip.

v2.1 — February 2026
"""

import json
import sys
import os
import glob
from datetime import datetime
from typing import Optional, Dict, Any


# ============================================================
# SECTION 1: JSON LOADER
# ============================================================

def find_latest_json(directory="."):
    pattern = os.path.join(directory, "ud_fundamental_*.json")
    files = glob.glob(pattern)
    return max(files, key=os.path.getmtime) if files else None


def load_snapshot(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def extract_value(snap, key, field="value"):
    try:
        e = snap["fred_data"][key]
        if "error" in e:
            return None
        return float(e["context"]["current"][field])
    except (KeyError, TypeError, ValueError):
        return None


def extract_yoy(snap, key):
    try:
        return float(snap["fred_data"][key]["context"]["yoy_pct"])
    except (KeyError, TypeError, ValueError):
        return None


def extract_mom_ann(snap, key):
    try:
        return float(snap["fred_data"][key]["context"]["mom_annualized"])
    except (KeyError, TypeError, ValueError):
        return None


# ============================================================
# SECTION 2: MARKET-IMPLIED RATE PATH
# ============================================================

def compute_market_implied_path(effr, dgs2, dgs10, t10y3m, fed_upper, fed_lower):
    midpoint = (fed_upper + fed_lower) / 2
    tp = 0.15
    implied_avg = dgs2 - tp
    cuts_bp = round((implied_avg - effr) * 100, 1)
    terminal = round(implied_avg, 3)

    direction = "EASING" if cuts_bp < -25 else "TIGHTENING" if cuts_bp > 25 else "ON_HOLD"

    if t10y3m < -0.5: curve = "DEEP_INVERSION"
    elif t10y3m < 0: curve = "INVERTED"
    elif t10y3m < 0.5: curve = "FLAT"
    else: curve = "NORMAL"

    mag = abs(cuts_bp)
    conf = "HIGH" if mag > 75 else "MODERATE" if mag > 35 else "LOW"

    curve_confirms = (
        (direction == "EASING" and curve in ("INVERTED", "DEEP_INVERSION"))
        or (direction == "TIGHTENING" and curve in ("NORMAL", "FLAT"))
        or (direction == "ON_HOLD" and curve == "FLAT")
    )

    return {
        "effr": effr, "fed_midpoint": midpoint, "dgs2": dgs2,
        "term_premium_est": tp, "implied_terminal": terminal,
        "implied_cuts_bp": cuts_bp, "implied_direction": direction,
        "curve_state": curve, "t10y3m": t10y3m,
        "curve_confirms_direction": curve_confirms,
        "signal_confidence": conf,
    }


# ============================================================
# SECTION 3: FRAMEWORK-IMPLIED POLICY DIRECTION
# ============================================================

def compute_framework_implied_path(
    core_pce_yoy=None, ppi_yoy=None, cpi_mom_ann=None,
    unemployment=None, claims=None, ahe_yoy=None,
    nfci=None, ig_oas=None, gdp_yoy=None,
):
    score = 0
    signals = []

    # --- INFLATION (40pts max) ---
    if core_pce_yoy is not None:
        if core_pce_yoy > 3.5:
            score += 25; signals.append(f"Core PCE {core_pce_yoy:.1f}% -- far above target -> HAWKISH (+25)")
        elif core_pce_yoy > 2.8:
            score += 15; signals.append(f"Core PCE {core_pce_yoy:.1f}% -- above target, sticky -> HAWKISH (+15)")
        elif core_pce_yoy > 2.3:
            score += 5; signals.append(f"Core PCE {core_pce_yoy:.1f}% -- slightly above target -> MILD HAWKISH (+5)")
        elif core_pce_yoy >= 1.7:
            signals.append(f"Core PCE {core_pce_yoy:.1f}% -- at/near target -> NEUTRAL (0)")
        else:
            score -= 15; signals.append(f"Core PCE {core_pce_yoy:.1f}% -- below target -> DOVISH (-15)")

    if ppi_yoy is not None:
        if ppi_yoy > 4.0:
            score += 10; signals.append(f"PPI {ppi_yoy:.1f}% -- pipeline pressure -> HAWKISH (+10)")
        elif ppi_yoy > 2.5:
            score += 3; signals.append(f"PPI {ppi_yoy:.1f}% -- mild pipeline -> MILD HAWKISH (+3)")
        elif ppi_yoy < 1.0:
            score -= 8; signals.append(f"PPI {ppi_yoy:.1f}% -- pipeline deflating -> DOVISH (-8)")
        else:
            signals.append(f"PPI {ppi_yoy:.1f}% -- neutral pipeline (0)")

    if cpi_mom_ann is not None:
        if cpi_mom_ann > 4.0:
            score += 8; signals.append(f"CPI MoM ann. {cpi_mom_ann:.1f}% -- accelerating -> HAWKISH (+8)")
        elif cpi_mom_ann < 1.5:
            score -= 5; signals.append(f"CPI MoM ann. {cpi_mom_ann:.1f}% -- decelerating -> DOVISH (-5)")

    # --- LABOUR (35pts max) ---
    if claims is not None:
        if claims > 300000:
            score -= 20; signals.append(f"Claims {claims/1000:.0f}K -- stress -> DOVISH (-20)")
        elif claims > 250000:
            score -= 10; signals.append(f"Claims {claims/1000:.0f}K -- elevated -> DOVISH (-10)")
        elif claims < 210000:
            score += 8; signals.append(f"Claims {claims/1000:.0f}K -- very tight -> HAWKISH (+8)")
        elif claims < 230000:
            score += 3; signals.append(f"Claims {claims/1000:.0f}K -- healthy -> MILD HAWKISH (+3)")
        else:
            signals.append(f"Claims {claims/1000:.0f}K -- neutral range (0)")

    if unemployment is not None:
        if unemployment > 5.5:
            score -= 15; signals.append(f"UR {unemployment:.1f}% -- elevated -> DOVISH (-15)")
        elif unemployment > 4.5:
            score -= 5; signals.append(f"UR {unemployment:.1f}% -- softening -> MILD DOVISH (-5)")
        elif unemployment < 3.8:
            score += 8; signals.append(f"UR {unemployment:.1f}% -- tight -> HAWKISH (+8)")
        elif unemployment < 4.2:
            score += 3; signals.append(f"UR {unemployment:.1f}% -- healthy -> MILD HAWKISH (+3)")
        else:
            signals.append(f"UR {unemployment:.1f}% -- neutral range (0)")

    if ahe_yoy is not None:
        if ahe_yoy > 5.0:
            score += 8; signals.append(f"Wages {ahe_yoy:.1f}% -- hot -> HAWKISH (+8)")
        elif ahe_yoy > 4.0:
            score += 3; signals.append(f"Wages {ahe_yoy:.1f}% -- firm -> MILD HAWKISH (+3)")
        elif ahe_yoy < 3.0:
            score -= 5; signals.append(f"Wages {ahe_yoy:.1f}% -- cooling -> DOVISH (-5)")
        else:
            signals.append(f"Wages {ahe_yoy:.1f}% -- neutral range (0)")

    # --- GROWTH / FINANCIAL CONDITIONS (25pts max) ---
    if nfci is not None:
        if nfci > 0.5:
            score -= 15; signals.append(f"NFCI {nfci:.2f} -- tight, stress -> DOVISH (-15)")
        elif nfci > 0:
            score -= 5; signals.append(f"NFCI {nfci:.2f} -- tightening -> MILD DOVISH (-5)")
        elif nfci < -0.5:
            score += 5; signals.append(f"NFCI {nfci:.2f} -- very loose -> HAWKISH (+5)")
        else:
            signals.append(f"NFCI {nfci:.2f} -- neutral conditions (0)")

    if ig_oas is not None:
        if ig_oas > 1.8:
            score -= 10; signals.append(f"IG OAS {ig_oas:.2f}% -- credit stress -> DOVISH (-10)")
        elif ig_oas > 1.3:
            score -= 3; signals.append(f"IG OAS {ig_oas:.2f}% -- widening -> MILD DOVISH (-3)")
        elif ig_oas < 0.8:
            score += 3; signals.append(f"IG OAS {ig_oas:.2f}% -- tight, no stress -> MILD HAWKISH (+3)")
        else:
            signals.append(f"IG OAS {ig_oas:.2f}% -- neutral (0)")

    if gdp_yoy is not None:
        if gdp_yoy < 0:
            score -= 15; signals.append(f"GDP {gdp_yoy:.1f}% -- contraction -> DOVISH (-15)")
        elif gdp_yoy < 1.0:
            score -= 5; signals.append(f"GDP {gdp_yoy:.1f}% -- stall speed -> MILD DOVISH (-5)")
        elif gdp_yoy > 3.5:
            score += 5; signals.append(f"GDP {gdp_yoy:.1f}% -- strong -> HAWKISH (+5)")

    score = max(-100, min(100, score))

    if score > 20:
        d, s = "HOLD_OR_HIKE", "Inflation above target and/or labour tight -- restrictive bias"
    elif score > 5:
        d, s = "HOLD", "Roughly balanced -- no clear case for either direction"
    elif score > -20:
        d, s = "HOLD_OR_CUT", "Tilting dovish -- some softening but not urgent"
    else:
        d, s = "CUT", "Growth/labour weakening and/or inflation at target -- easing case"

    return {"score": score, "direction": d, "stance_summary": s,
            "signals": signals, "signal_count": len(signals)}


# ============================================================
# SECTION 4: GAP CALCULATOR
# ============================================================

def compute_policy_gap(market, framework, fedwatch=None):
    mdir = {"EASING": -2, "ON_HOLD": 0, "TIGHTENING": 2}
    fdir = {"CUT": -2, "HOLD_OR_CUT": -1, "HOLD": 0, "HOLD_OR_HIKE": 1}

    ms = mdir.get(market["implied_direction"], 0)
    fs = fdir.get(framework["direction"], 0)
    direction_gap = ms - fs

    bp_gap = market["implied_cuts_bp"] - (framework["score"] * 1.5)
    gap_score = max(-100, min(100, round(bp_gap / 2)))

    if gap_score < -25:
        cls = "MARKET_DOVISH"
        desc = ("Market pricing significantly more easing than fundamentals support. "
                "If framework is right -> USD underpriced. Look for long USD if UD2 confirms.")
    elif gap_score < -10:
        cls = "MARKET_SLIGHTLY_DOVISH"
        desc = ("Market pricing somewhat more easing than fundamentals. "
                "Mild USD tailwind if data holds. Monitor claims and PPI.")
    elif gap_score > 25:
        cls = "MARKET_HAWKISH"
        desc = ("Market pricing significantly less easing than fundamentals support. "
                "If framework is right -> USD overpriced. Look for short USD if growth confirms.")
    elif gap_score > 10:
        cls = "MARKET_SLIGHTLY_HAWKISH"
        desc = ("Market pricing somewhat less easing than fundamentals. "
                "Mild USD headwind if softening continues. Monitor NFCI and IG OAS.")
    else:
        cls = "ALIGNED"
        desc = ("Market expectations roughly aligned with fundamentals. "
                "Low policy-gap edge. Look for superposition in other bricks.")

    factors = []
    if market["curve_confirms_direction"]:
        factors.append("Curve confirms market direction")
    if market["signal_confidence"] == "HIGH":
        factors.append("Large 2Y-EFFR gap (high conviction)")
    if abs(framework["score"]) > 40:
        factors.append("Strong framework directional signal")
    if framework["signal_count"] >= 8:
        factors.append("High data coverage (8+ signals)")

    conf = "HIGH" if len(factors) >= 3 else "MODERATE" if len(factors) >= 1 else "LOW"

    # FedWatch cross-check
    fw_note = None
    if fedwatch:
        parts = []
        if fedwatch.get("next_meeting"):
            parts.append(f"Next FOMC: {fedwatch['next_meeting']}")
        if fedwatch.get("next_meeting_cut_prob") is not None:
            parts.append(f"Cut: {fedwatch['next_meeting_cut_prob']:.0f}%")
        if fedwatch.get("next_meeting_hold_prob") is not None:
            parts.append(f"Hold: {fedwatch['next_meeting_hold_prob']:.0f}%")
        if fedwatch.get("cuts_by_june") is not None:
            parts.append(f"Cuts by Jun: {fedwatch['cuts_by_june']}")
        if fedwatch.get("cuts_by_dec") is not None:
            parts.append(f"Cuts by Dec: {fedwatch['cuts_by_dec']}")
        if fedwatch.get("terminal_rate_implied") is not None:
            parts.append(f"YE rate: {fedwatch['terminal_rate_implied']:.2f}%")
        fw_note = " | ".join(parts)
        if fedwatch.get("notes"):
            fw_note += f"\n    Note: {fedwatch['notes']}"
        if fedwatch.get("date_checked"):
            fw_note += f"\n    (Checked: {fedwatch['date_checked']})"

    return {
        "gap_score": gap_score, "classification": cls, "description": desc,
        "confidence": conf, "confidence_factors": factors,
        "direction_gap": direction_gap,
        "market_summary": {
            "implied_direction": market["implied_direction"],
            "implied_cuts_bp": market["implied_cuts_bp"],
            "implied_terminal": market["implied_terminal"],
            "curve_state": market["curve_state"],
        },
        "framework_summary": {
            "direction": framework["direction"],
            "score": framework["score"],
            "stance": framework["stance_summary"],
        },
        "fedwatch_crosscheck": fw_note,
    }


# ============================================================
# SECTION 5: LLM CONTEXT FORMATTER
# ============================================================

def format_gap_for_llm(gap):
    lines = [
        "=" * 60,
        "FED POLICY EXPECTATIONS GAP",
        "=" * 60,
        "",
        f"  Classification: {gap['classification']}",
        f"  Gap Score: {gap['gap_score']:+d}/100",
        f"    (negative = market more dovish than fundamentals = USD underpriced)",
        f"    (positive = market more hawkish than fundamentals = USD overpriced)",
        f"  Confidence: {gap['confidence']}",
        "",
        "  MARKET EXPECTATIONS (from 2Y yield -- leads Fed by 3m, r=0.97):",
        f"    Direction: {gap['market_summary']['implied_direction']}",
        f"    Implied rate change: {gap['market_summary']['implied_cuts_bp']:+.0f}bp from EFFR",
        f"    Implied terminal: {gap['market_summary']['implied_terminal']:.2f}%",
        f"    Curve state: {gap['market_summary']['curve_state']}",
        "",
        "  FRAMEWORK REGIME READ (from UD1 fundamentals):",
        f"    Direction: {gap['framework_summary']['direction']}",
        f"    Score: {gap['framework_summary']['score']:+d}/100",
        f"    Stance: {gap['framework_summary']['stance']}",
        "",
    ]

    if gap.get("fedwatch_crosscheck"):
        lines.append(f"  CME FedWatch (manual):")
        lines.append(f"    {gap['fedwatch_crosscheck']}")
        lines.append("")

    lines += [
        f"  TRADE IMPLICATION:",
        f"    {gap['description']}",
        "",
    ]

    if gap["confidence_factors"]:
        lines.append("  CONFIDENCE FACTORS:")
        for f in gap["confidence_factors"]:
            lines.append(f"    + {f}")
        lines.append("")

    lines += [
        "  --- LLM USAGE ---",
        "  MARKET_DOVISH -> data disappointment = USD rally. Check claims, PPI.",
        "  MARKET_HAWKISH -> data weakening = USD sell. Check NFCI, IG OAS.",
        "  ALIGNED -> low policy-gap edge. Check positioning, geo, relative value.",
        "  Gap magnitude: +/-10 = noise, +/-25-50 = actionable, +/-50+ = high conviction.",
        "  This module finds the GAP. UD2 confirms transmission. UD3 finds the trigger.",
        "=" * 60,
    ]

    return "\n".join(lines)


# ============================================================
# SECTION 6: MAIN RUNNER
# ============================================================

def run(filepath, fedwatch=None):
    print(f"\n  Loading: {filepath}")
    snap = load_snapshot(filepath)
    print(f"  Script version: {snap.get('script_version', '?')}")
    print(f"  Run date: {snap.get('run_date', '?')}")
    print()

    # --- Extract from JSON ---
    effr = extract_value(snap, "EFFR")
    dgs2 = extract_value(snap, "DGS2")
    dgs10 = extract_value(snap, "DGS10")
    t10y3m = extract_value(snap, "T10Y3M")
    fed_upper = extract_value(snap, "DFEDTARU")
    fed_lower = fed_upper - 0.25 if fed_upper else None

    core_pce_yoy = extract_yoy(snap, "PCEPILFE")
    ppi_yoy = extract_yoy(snap, "PPIFIS")
    cpi_mom_ann = extract_mom_ann(snap, "CPIAUCSL")
    unemployment = extract_value(snap, "UNRATE")
    claims = extract_value(snap, "ICSA")
    ahe_yoy = extract_yoy(snap, "CES0500000003")
    nfci = extract_value(snap, "NFCI")
    ig_oas = extract_value(snap, "BAMLC0A0CM")
    gdp_yoy = extract_yoy(snap, "GDPC1")

    # --- Validate ---
    missing = []
    if effr is None: missing.append("EFFR")
    if dgs2 is None: missing.append("DGS2")
    if t10y3m is None: missing.append("T10Y3M")
    if fed_upper is None: missing.append("DFEDTARU")
    if missing:
        print(f"  ERROR: Missing critical data: {', '.join(missing)}")
        return

    # --- Compute ---
    market = compute_market_implied_path(effr, dgs2, dgs10 or 0, t10y3m, fed_upper, fed_lower)
    framework = compute_framework_implied_path(
        core_pce_yoy=core_pce_yoy, ppi_yoy=ppi_yoy, cpi_mom_ann=cpi_mom_ann,
        unemployment=unemployment, claims=claims, ahe_yoy=ahe_yoy,
        nfci=nfci, ig_oas=ig_oas, gdp_yoy=gdp_yoy,
    )
    gap = compute_policy_gap(market, framework, fedwatch)

    # --- Print ---
    print("=" * 60)
    print("  FRAMEWORK SIGNAL BREAKDOWN")
    print("=" * 60)
    for sig in framework["signals"]:
        print(f"  {sig}")
    print(f"\n  TOTAL: {framework['score']:+d}/100 -> {framework['direction']}")
    print()

    llm_block = format_gap_for_llm(gap)
    print(llm_block)

    # --- Save ---
    base = os.path.splitext(filepath)[0]

    gap_txt = base.replace("ud_fundamental_", "ud_gap_") + ".txt"
    with open(gap_txt, "w", encoding="utf-8") as f:
        f.write(llm_block)
    print(f"\n  Saved LLM block: {gap_txt}")

    gap_json = base.replace("ud_fundamental_", "ud_gap_") + ".json"
    output = {
        "timestamp": datetime.now().isoformat(),
        "source_file": os.path.basename(filepath),
        "market": market,
        "framework": {
            "score": framework["score"],
            "direction": framework["direction"],
            "stance": framework["stance_summary"],
            "signal_count": framework["signal_count"],
            "signals": framework["signals"],
        },
        "gap": {
            "score": gap["gap_score"],
            "classification": gap["classification"],
            "confidence": gap["confidence"],
            "confidence_factors": gap["confidence_factors"],
            "description": gap["description"],
            "market_summary": gap["market_summary"],
            "framework_summary": gap["framework_summary"],
            "fedwatch_crosscheck": gap["fedwatch_crosscheck"],
        },
    }
    with open(gap_json, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
    print(f"  Saved JSON: {gap_json}")
    print()


# ============================================================
# SECTION 7: CME FEDWATCH INPUT (update weekly)
# ============================================================
# Check: https://www.cmegroup.com/markets/interest-rates/cme-fedwatch-tool.html
# Set FEDWATCH_ENABLED = True when you have fresh data.
# Set FEDWATCH_ENABLED = False to skip (module runs fine without it).

FEDWATCH_ENABLED = False

FEDWATCH = {
    "date_checked":           "2026-02-17",     # when you last checked
    "next_meeting":           "2026-03-19",     # next FOMC date
    "next_meeting_cut_prob":  35.0,             # % probability of cut
    "next_meeting_hold_prob": 65.0,             # % probability of hold
    "next_meeting_hike_prob": 0.0,              # % probability of hike
    "cuts_by_june":           1.0,              # total 25bp cuts priced through June
    "cuts_by_dec":            2.5,              # total 25bp cuts priced through Dec
    "terminal_rate_implied":  3.00,             # year-end implied rate
    "notes": "",                                # your notes e.g. "March split, June fully priced"
}


# ============================================================
# SECTION 8: CLI ENTRY POINT
# ============================================================

if __name__ == "__main__":
    filepath = None
    for a in sys.argv[1:]:
        if a.endswith(".json"):
            filepath = a

    if not filepath:
        filepath = find_latest_json()
        if not filepath:
            print("  ERROR: No ud_fundamental_*.json found in current directory.")
            print("  Run Script 1 first, or specify: python fed_policy_gap.py <file.json>")
            sys.exit(1)

    fw = FEDWATCH if FEDWATCH_ENABLED else None
    run(filepath, fw)
