"""
UD Framework — Fundamental Collector v2.8
==========================================
Script 1 of 2 (Fundamental Layer ONLY — no price data)

v2.8 CHANGES (from v2.7):
  - NEW: Foreign PMI manual inputs — Eurozone, UK, Japan composite PMIs
    added to manual inputs section (same pattern as foreign yields).
  - NEW: calculate_pmi_divergence() — computes US ISM vs foreign composite
    PMI differential for EUR/USD, GBP/USD, USD/JPY pairs. Classifies:
    US_OUTPERFORMING / US_UNDERPERFORMING / ALIGNED (within 2pts).
    Also flags BOTH_EXPANDING / BOTH_CONTRACTING / DIVERGENT regime combos.
  - NEW: PMI GROWTH DIFFERENTIAL section in LLM snapshot — inserted after
    UD2 Brick 2 rate differentials. Provides growth differential quality
    modifier for Brick 2 confidence scoring.
  - LLM RULE: PMI divergence CONFIRMS or UNCONFIRMS rate differential
    direction. ALIGNED = neutral modifier. Never standalone signal (Tier 2).

[v2.7 changes: COT fix | WoW yield deltas | .txt fix | GDP flag]
[v2.6 changes: China Credit | SLOOS | PhillyFed CapEx new indicators]
[Prior version history — see Build Log V12]
"""

import requests
import json
import os
import re
import time
import glob
import warnings
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

warnings.filterwarnings("ignore")

try:
    import yfinance as yf
except ImportError:
    print("ERROR: yfinance not installed. Run: pip install yfinance")
    exit(1)

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas not installed. Run: pip install pandas")
    exit(1)

# =================================================================
# CONFIGURATION
# =================================================================

FRED_API_KEY = "0bbfbbc8d12decc56c27c1bc5199a444"
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
LOOKBACK_DAYS = 730
OUTPUT_DIR = "ud_data"
SCRIPT_VERSION = "v2.8"

# =================================================================
# FRED SERIES
# =================================================================

FRED_SERIES = {
    "CPIAUCSL":   {"name": "CPI All Items",         "module": "inflation",    "freq": "monthly", "transform": "yoy",       "timing": "LAGGING",    "use": "Confirm past trend, headline noise"},
    "CPILFESL":   {"name": "Core CPI",              "module": "inflation",    "freq": "monthly", "transform": "yoy",       "timing": "LAGGING",    "use": "Fed-relevant anchor, confirms trend"},
    "PCEPI":      {"name": "PCE Price Index",        "module": "inflation",    "freq": "monthly", "transform": "yoy",       "timing": "LAGGING",    "use": "Fed's preferred measure"},
    "PCEPILFE":   {"name": "Core PCE",               "module": "inflation",    "freq": "monthly", "transform": "yoy",       "timing": "LAGGING",    "use": "Fed target reference, confirms trend"},
    "PPIFIS":     {"name": "PPI Final Demand",       "module": "inflation",    "freq": "monthly", "transform": "yoy",       "timing": "LEADING",    "use": "Pipeline signal, predicts CPI 2-3m forward. A-tier validated (r=0.73, +0.39 R2)"},
    "CUSR0000SAH1":  {"name": "CPI Shelter",         "module": "inflation_components", "freq": "monthly", "transform": "yoy", "timing": "LAGGING",  "use": "36% CPI weight. Most persistent component. A-tier validated — autocorrelation >0.4 for 12m"},
    "CPIENGSL":      {"name": "CPI Energy",          "module": "inflation_components", "freq": "monthly", "transform": "yoy", "timing": "LAGGING",  "use": "7% CPI weight. Most volatile swing component. Near-deterministic from gasoline prices"},
    "CUSR0000SACL1E":{"name": "CPI Core Goods",      "module": "inflation_components", "freq": "monthly", "transform": "yoy", "timing": "LAGGING",  "use": "21% CPI weight. Tracks Manheim used cars with ~2m lag. A-tier validated (r=0.68)"},
    "CUSR0000SASLE": {"name": "CPI Services ex-Shelter (Supercore)", "module": "inflation_components", "freq": "monthly", "transform": "yoy", "timing": "LAGGING", "use": "25% CPI weight. Fed's focus measure. Stickiest component — wages set floor. B-tier directional only"},
    "CPIUFDSL":      {"name": "CPI Food",            "module": "inflation_components", "freq": "monthly", "transform": "yoy", "timing": "LAGGING",  "use": "14% CPI weight. No validated leading indicator — momentum only"},
    "CUSR0000SETA02":{"name": "CPI Used Cars",       "module": "inflation_components", "freq": "monthly", "transform": "yoy", "timing": "LAGGING",  "use": "Transmission channel for Manheim into core goods. A-tier validated (r=0.68, +0.24 R2)"},
    "GASREGW":       {"name": "Gasoline Price (Regular)", "module": "inflation_leaders", "freq": "weekly", "transform": "level", "timing": "LEADING", "use": "A-tier validated (r=0.68, +0.45 R2). Near-deterministic predictor of CPI Energy. Use reference month average"},
    "CES0500000003": {"name": "Avg Hourly Earnings (Private)", "module": "inflation_leaders", "freq": "monthly", "transform": "yoy", "timing": "LEADING", "use": "B-tier validated (92.5% directional, +0.04 R2). Directional confirmation for supercore only — does NOT predict magnitude. Floor signal: >3.5% YoY = services inflation has a floor"},
    "HOUST":      {"name": "Housing Starts",         "module": "growth",       "freq": "monthly", "transform": "level",     "timing": "COINCIDENT/NOWCAST", "use": "Confirms rate stress in real time. Does NOT lead GDP (lag 1m, r=0.47, +0.5% R2). Rate-sensitivity canary, not GDP predictor"},
    "PERMIT":     {"name": "Building Permits",       "module": "growth",       "freq": "monthly", "transform": "level",     "timing": "LEADING/NOWCAST",    "use": "Sub-monthly lead over starts (lag 0, r=0.87, 99% strong accuracy). Rate-sensitivity canary. Does NOT predict GDP (r=0.47)"},
    "DGORDER":    {"name": "Durable Goods Orders",   "module": "growth",       "freq": "monthly", "transform": "yoy",       "timing": "COINCIDENT",         "use": "Moves WITH Industrial Production (lag 0, r=0.78, 61.4% R2). High-value manufacturing confirmation, not a leading signal"},
    "RSAFS":      {"name": "Retail Sales",           "module": "growth",       "freq": "monthly", "transform": "yoy",       "timing": "COINCIDENT", "use": "97% directional accuracy to GDP. Real-time GDP proxy. Best regime classification signal for current spending"},
    "PCE":        {"name": "Personal Consumption",   "module": "growth",       "freq": "monthly", "transform": "yoy",       "timing": "COINCIDENT", "use": "Current demand, largest GDP component"},
    "INDPRO":     {"name": "Industrial Production",  "module": "growth",       "freq": "monthly", "transform": "yoy",       "timing": "COINCIDENT", "use": "r=0.83 to GDP, 69.5% R2 concurrent. Manufacturing regime gauge"},
    "UMCSENT":    {"name": "Consumer Sentiment",     "module": "growth",       "freq": "monthly", "transform": "level",     "timing": "SOFT DATA (UNRELIABLE)", "use": "ZERO predictive power for spending (r=0.11, coin-flip accuracy, grade F across 36 years). IGNORE for decisions. Context/colour only"},
    "GDPC1":      {"name": "Real GDP",               "module": "growth",       "freq": "quarterly","transform": "yoy",      "timing": "LAGGING",    "use": "Confirms where economy WAS, not where going. Surface latest print as confirmation/contradiction of expansion classification"},
    "NFCI":       {"name": "Financial Conditions",   "module": "growth",       "freq": "weekly",  "transform": "level",     "timing": "LEADING",    "use": "ONLY strategic forecaster in growth module. 10-11 month lead confirmed (GDP r=0.27 +8.2% R2, IP r=0.33 +13.2% R2, 76.5% strong-signal accuracy). When NFCI tightens and hard data is fine = 3-4 quarter early warning"},
    "ICSA":       {"name": "Initial Claims",         "module": "labour",       "freq": "weekly",  "transform": "level",     "timing": "LEADING/NOWCAST", "use": "Fastest labour signal. 1-month lead validated (r=0.85, +0.39 R2). Highest-priority in labour module by massive margin. When claims spikes and rest is calm = genuine early warning"},
    "CCSA":       {"name": "Continuing Claims",      "module": "labour",       "freq": "weekly",  "transform": "level",     "timing": "LAGGING",    "use": "Duration of unemployment, confirms stress"},
    "JTSJOL":     {"name": "JOLTS Openings",         "module": "labour",       "freq": "monthly", "transform": "level",     "timing": "COINCIDENT", "use": "Demand signal but NOT leading (lag=0 validated). Only useful at extremes (>1std: 87-92% accuracy). Gradual normalisation does not predict payroll/unemployment changes"},
    "JTSHIL":     {"name": "JOLTS Hires",            "module": "labour",       "freq": "monthly", "transform": "level",     "timing": "COINCIDENT", "use": "Current absorption rate"},
    "JTSQUL":     {"name": "JOLTS Quits",            "module": "labour",       "freq": "monthly", "transform": "level",     "timing": "COINCIDENT", "use": "Worker confidence, voluntary turnover. Concurrent with wages (lag 0, r=0.78)"},
    "JTSLDL":     {"name": "JOLTS Layoffs",          "module": "labour",       "freq": "monthly", "transform": "level",     "timing": "COINCIDENT", "use": "Concurrent with claims (r=0.94), NOT leading. Same event different measurement. Use as confirmation signal, not independent early warning"},
    "PAYEMS":     {"name": "Nonfarm Payrolls",       "module": "labour",       "freq": "monthly", "transform": "mom_change","timing": "COINCIDENT", "use": "Current employment level (stock). r=0.71 to GDP"},
    "UNRATE":     {"name": "Unemployment Rate",      "module": "labour",       "freq": "monthly", "transform": "level",     "timing": "LAGGING",    "use": "Rises AFTER recession starts. Decompose before interpreting (entrant vs job-loss)"},
    "AWHMAN":     {"name": "Avg Weekly Hours (Mfg)", "module": "labour",       "freq": "monthly", "transform": "level",     "timing": "COINCIDENT", "use": "Hours do NOT lead payrolls (lag=0 validated, 57% directional accuracy). Confirmation only, not early warning. Low fire rate in automated signals"},
    "SAHMREALTIME":{"name": "Sahm Rule",             "module": "labour",       "freq": "monthly", "transform": "level",     "timing": "COINCIDENT", "use": "Recession gate, triggers at 0.5"},
    "UNEMPLOY":   {"name": "Unemployed Persons",     "module": "labour",       "freq": "monthly", "transform": "level",     "timing": "LAGGING",    "use": "Labour supply for tightness calc"},
    "FEDFUNDS":   {"name": "Fed Funds Rate",         "module": "monetary",     "freq": "monthly", "transform": "level",     "timing": "LAGGING",    "use": "Policy response, not forward signal. 2Y yield leads Fed Funds by 3 months (r=0.97)"},
    "WALCL":      {"name": "Fed Balance Sheet",      "module": "monetary",     "freq": "weekly",  "transform": "level",     "timing": "LAGGING",    "use": "Reflects past QE/QT decisions"},
    "DFEDTARU":   {"name": "Fed Target Upper",       "module": "monetary",     "freq": "daily",   "transform": "level",     "timing": "LAGGING",    "use": "Current policy stance"},
    "EFFR":       {"name": "Effective Fed Funds",    "module": "liquidity",    "freq": "daily",   "transform": "level",     "timing": "NOWCAST",    "use": "Where rates actually trade NOW"},
    "IORB":       {"name": "Interest on Reserves",   "module": "liquidity",    "freq": "daily",   "transform": "level",     "timing": "NOWCAST",    "use": "Floor reference, current"},
    "DPCREDIT":   {"name": "Discount Rate",          "module": "liquidity",    "freq": "daily",   "transform": "level",     "timing": "NOWCAST",    "use": "Ceiling reference, current"},
    "SOFR":       {"name": "SOFR",                   "module": "liquidity",    "freq": "daily",   "transform": "level",     "timing": "NOWCAST",    "use": "Funding stress indicator, real-time"},
    "RRPONTSYD":  {"name": "ON RRP Usage ($B)",      "module": "liquidity",    "freq": "daily",   "transform": "level",     "timing": "NOWCAST",    "use": "Excess liquidity gauge, real-time"},
    "WRESBAL":    {"name": "Reserve Balances",       "module": "liquidity",    "freq": "weekly",  "transform": "level",     "timing": "COINCIDENT", "use": "Current system liquidity level"},
    "M2SL":       {"name": "M2 Money Supply",        "module": "liquidity",    "freq": "monthly", "transform": "yoy",       "timing": "LAGGING",    "use": "Reflects past policy, slow signal"},
    "TB3MS":      {"name": "3-Month T-Bill",         "module": "yields",       "freq": "monthly", "transform": "level",     "timing": "COINCIDENT", "use": "Front-end anchor, policy proxy"},
    "DGS2":       {"name": "US 2Y Yield",            "module": "yields",       "freq": "daily",   "transform": "level",     "timing": "LEADING",    "use": "Leads Fed Funds by 3 months (r=0.97, 69% strong-signal accuracy). When 2Y reprices aggressively and Fed hasn't moved = gap closing signal"},
    "DGS5":       {"name": "US 5Y Yield",            "module": "yields",       "freq": "daily",   "transform": "level",     "timing": "LEADING",    "use": "Medium-term growth/inflation expectations"},
    "DGS10":      {"name": "US 10Y Yield",           "module": "yields",       "freq": "daily",   "transform": "level",     "timing": "COINCIDENT", "use": "Term premium + growth expectations. 10Y US-DE spread tracks EUR/USD level at r=-0.72 (equilibrium anchor)"},
    "DGS30":      {"name": "US 30Y Yield",           "module": "yields",       "freq": "daily",   "transform": "level",     "timing": "COINCIDENT", "use": "Long-term inflation/fiscal risk"},
    "T10Y2Y":     {"name": "10Y-2Y Spread",          "module": "yields",       "freq": "daily",   "transform": "level",     "timing": "LEADING",    "use": "Recession signal, inversion = elevated risk for 1-2 years"},
    "T10Y3M":     {"name": "10Y-3M Spread",          "module": "yields",       "freq": "daily",   "transform": "level",     "timing": "LEADING",    "use": "Primary recession predictor. 3/4 recessions predicted, avg 14-month lead (range 9-23m). Regime flag, not timing tool"},
    "DFII10":     {"name": "10Y TIPS Real Yield",    "module": "yields",       "freq": "daily",   "transform": "level",     "timing": "LEADING",    "use": "Real rate expectations, policy tightness"},
    "BAMLH0A0HYM2":  {"name": "HY OAS",             "module": "credit",       "freq": "daily",   "transform": "level",     "timing": "COINCIDENT", "use": "Moves WITH equity stress, not before it (lag 0, r=-0.61, 37% R2 concurrent). Real-time risk gauge, NOT early warning. When calm = genuine stability confirmation"},
    "BAMLC0A0CM":    {"name": "IG OAS",              "module": "credit",       "freq": "daily",   "transform": "level",     "timing": "LEADING",    "use": "Cleanest credit-to-economy signal (2m lead, r=-0.64, +5.2% R2). Watch IG OVER HY for early warning. IG widening = corporate investment/hiring impact 2 months out"},
    "TEDRATE":       {"name": "TED Spread",          "module": "credit",       "freq": "daily",   "transform": "level",     "timing": "LEADING",    "use": "DISCONTINUED — ignore any readings. Do not flag as missing data."},
    "BAMLH0A0HYM2EY":{"name": "HY Effective Yield",  "module": "credit",       "freq": "daily",   "transform": "level",     "timing": "COINCIDENT", "use": "Current cost of junk debt"},
    "VIXCLS":     {"name": "VIX",                    "module": "volatility",   "freq": "daily",   "transform": "level",     "timing": "NOWCAST",    "use": "Real-time fear gauge, spikes in stress"},
    "CRDQCNAPABIS": {"name": "China Credit to GDP (BIS)",  "module": "new_indicators", "freq": "quarterly", "transform": "level", "timing": "LEADING",
        "use": "First non-US leading signal. 6M growth rate leads AUD/USD at 1Q (r=0.23, +7.8% R2 over NFCI) and US IP at 2Q (r=0.23, +6.8% R2 over NFCI). Quarterly context overlay — Tier 2."},
    "DRTSCILM":     {"name": "SLOOS C&I Loan Tightening", "module": "new_indicators", "freq": "quarterly", "transform": "level", "timing": "LEADING",
        "use": "Strongest of all 7 tested candidates. Leads IP by 1Q, employment by 2Q, credit spreads by 3Q. Different dimension from NFCI (r=0.11). Tier 2 regime confirmation for UD2 Brick 3."},
    "CEFDFSA066MSFRBPHI": {"name": "PhillyFed Future CapEx", "module": "new_indicators", "freq": "monthly", "transform": "level", "timing": "LEADING",
        "use": "Leads private fixed investment by 1 month (r=0.32, t=4.58). Also leads IP at 1mo (r=0.17, +3.1% R2 over NFCI). Tier 2 growth regime context."},
}

YFINANCE_SERIES = {
    "MOVE": {"ticker": "^MOVE", "name": "MOVE Index", "module": "volatility"},
}

# =================================================================
# PRIOR SNAPSHOT LOADER
# =================================================================

def load_prior_snapshot(current_ts: str) -> Optional[Dict]:
    pattern = os.path.join(OUTPUT_DIR, "ud_fundamental_*.json")
    files = sorted(glob.glob(pattern))
    candidates = [f for f in files if current_ts not in f]
    if not candidates:
        pattern2 = "ud_fundamental_*.json"
        candidates = sorted([f for f in glob.glob(pattern2) if current_ts not in f])
    if not candidates:
        return None
    prior_file = candidates[-1]
    try:
        with open(prior_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"  [OK] Prior snapshot: {os.path.basename(prior_file)}")
        return data
    except Exception as e:
        print(f"  [!] Could not load prior snapshot: {e}")
        return None


def _extract_yield_level(snapshot: Dict, series_id: str) -> Optional[float]:
    fred = snapshot.get("fred_data", {})
    entry = fred.get(series_id, {})
    ctx = entry.get("context", {})
    val = ctx.get("current", {}).get("value")
    if val is not None:
        try:
            return float(val)
        except (TypeError, ValueError):
            return None
    return None


def _extract_diff_bp(snapshot: Dict, diff_key: str, sub_key: str) -> Optional[float]:
    diffs = snapshot.get("differentials", {})
    entry = diffs.get(diff_key, {})
    val = entry.get(sub_key)
    if val is not None and isinstance(val, (int, float)):
        return float(val)
    return None


# =================================================================
# YIELD DELTA CALCULATOR
# =================================================================

def calculate_yield_deltas(fred_data: Dict, manual_data: Dict,
                           diffs: Dict, prior_snapshot: Optional[Dict]) -> Dict:
    deltas = {
        "available": prior_snapshot is not None,
        "prior_date": None,
        "yields": {}, "curve": {}, "differentials": {}, "credit": {},
        "impulse": "UNKNOWN", "impulse_detail": "",
    }
    if prior_snapshot is None:
        deltas["note"] = "No prior snapshot found — WoW deltas unavailable on first run"
        return deltas

    deltas["prior_date"] = prior_snapshot.get("run_date", "unknown")

    yield_series = {"US_3M": "TB3MS", "US_2Y": "DGS2", "US_5Y": "DGS5",
                    "US_10Y": "DGS10", "US_30Y": "DGS30", "TIPS_10Y": "DFII10"}
    for label, sid in yield_series.items():
        cur_val = fred_data.get(sid, {}).get("context", {}).get("current", {}).get("value")
        prior_val = _extract_yield_level(prior_snapshot, sid)
        if cur_val is not None and prior_val is not None:
            delta_bp = round((float(cur_val) - float(prior_val)) * 100, 1)
            signal = "RISING_WOW" if delta_bp > 3 else ("FALLING_WOW" if delta_bp < -3 else "FLAT_WOW")
            deltas["yields"][label] = {"current": round(float(cur_val), 3), "prior": round(float(prior_val), 3), "delta_bp": delta_bp, "signal": signal}
        else:
            deltas["yields"][label] = {"current": cur_val, "prior": prior_val, "delta_bp": None, "signal": "NO_PRIOR"}

    for label, sid in {"2s10s": "T10Y2Y", "10y3m": "T10Y3M"}.items():
        cur_val = fred_data.get(sid, {}).get("context", {}).get("current", {}).get("value")
        prior_val = _extract_yield_level(prior_snapshot, sid)
        if cur_val is not None and prior_val is not None:
            delta_bp = round((float(cur_val) - float(prior_val)) * 100, 1)
            deltas["curve"][label] = {"current": round(float(cur_val), 3), "prior": round(float(prior_val), 3),
                "delta_bp": delta_bp, "signal": "STEEPENING" if delta_bp > 2 else ("FLATTENING" if delta_bp < -2 else "FLAT")}

    y2 = deltas["yields"].get("US_2Y", {}).get("delta_bp")
    y10 = deltas["yields"].get("US_10Y", {}).get("delta_bp")
    if y2 is not None and y10 is not None:
        if y2 > 0 and y10 > 0:
            if y10 > y2 + 2:
                deltas["impulse"] = "BEAR_STEEPENING"; deltas["impulse_detail"] = f"Both ends rising; 10Y (+{y10}bp) > 2Y (+{y2}bp) — term premium / inflation risk"
            elif y2 > y10 + 2:
                deltas["impulse"] = "BEAR_FLATTENING"; deltas["impulse_detail"] = f"Both ends rising; 2Y (+{y2}bp) > 10Y (+{y10}bp) — hawkish repricing"
            else:
                deltas["impulse"] = "PARALLEL_UP"; deltas["impulse_detail"] = f"Parallel shift higher (2Y +{y2}bp, 10Y +{y10}bp)"
        elif y2 < 0 and y10 < 0:
            if y10 < y2 - 2:
                deltas["impulse"] = "BULL_STEEPENING"; deltas["impulse_detail"] = f"Both ends falling; 10Y ({y10}bp) > 2Y ({y2}bp) — growth concerns / easing expectations"
            elif y2 < y10 - 2:
                deltas["impulse"] = "BULL_FLATTENING"; deltas["impulse_detail"] = f"Both ends falling; 2Y ({y2}bp) > 10Y ({y10}bp) — dovish repricing"
            else:
                deltas["impulse"] = "PARALLEL_DOWN"; deltas["impulse_detail"] = f"Parallel shift lower (2Y {y2}bp, 10Y {y10}bp)"
        elif y2 < -2 and y10 > 2:
            deltas["impulse"] = "BULL_STEEPENING"; deltas["impulse_detail"] = f"Front-end rallying ({y2}bp), long-end selling off (+{y10}bp)"
        elif y2 > 2 and y10 < -2:
            deltas["impulse"] = "BEAR_FLATTENING"; deltas["impulse_detail"] = f"Front-end selling off (+{y2}bp), long-end rallying ({y10}bp)"
        else:
            deltas["impulse"] = "MIXED"; deltas["impulse_detail"] = f"2Y {y2:+.1f}bp, 10Y {y10:+.1f}bp — no dominant impulse"

    for peer_code in ["DE", "JP", "UK"]:
        cur_2y = diffs.get(peer_code, {}).get("diff_2y_bp")
        cur_10y = diffs.get(peer_code, {}).get("diff_10y_bp")
        prior_2y = _extract_diff_bp(prior_snapshot, peer_code, "diff_2y_bp")
        prior_10y = _extract_diff_bp(prior_snapshot, peer_code, "diff_10y_bp")
        entry = {}
        if isinstance(cur_2y, (int, float)) and prior_2y is not None:
            chg = round(float(cur_2y) - float(prior_2y), 1)
            entry["2y_delta_bp"] = chg
            entry["2y_direction"] = "WIDENING_USD" if chg > 1 else ("COMPRESSING" if chg < -1 else "STABLE")
        else:
            entry["2y_delta_bp"] = None; entry["2y_direction"] = "NO_PRIOR"
        if isinstance(cur_10y, (int, float)) and prior_10y is not None:
            chg = round(float(cur_10y) - float(prior_10y), 1)
            entry["10y_delta_bp"] = chg
            entry["10y_direction"] = "WIDENING_USD" if chg > 1 else ("COMPRESSING" if chg < -1 else "STABLE")
        else:
            entry["10y_delta_bp"] = None; entry["10y_direction"] = "NO_PRIOR"
        if entry.get("2y_delta_bp") is not None:
            us_2y_d = deltas["yields"].get("US_2Y", {}).get("delta_bp", 0) or 0
            entry["driver"] = "US_LED" if abs(us_2y_d) > abs(entry["2y_delta_bp"] - us_2y_d) else "FOREIGN_LED"
        deltas["differentials"][peer_code] = entry

    for label, sid in {"IG_OAS": "BAMLC0A0CM", "HY_OAS": "BAMLH0A0HYM2"}.items():
        cur_val = fred_data.get(sid, {}).get("context", {}).get("current", {}).get("value")
        prior_val = _extract_yield_level(prior_snapshot, sid)
        if cur_val is not None and prior_val is not None:
            delta_bp = round((float(cur_val) - float(prior_val)) * 100, 1)
            deltas["credit"][label] = {"current": round(float(cur_val), 3), "prior": round(float(prior_val), 3),
                "delta_bp": delta_bp, "signal": "WIDENING" if delta_bp > 2 else ("TIGHTENING" if delta_bp < -2 else "STABLE")}

    return deltas


# =================================================================
# GDP CONFIRMATION FLAG
# =================================================================

def calculate_gdp_flag(fred_data: Dict) -> Dict:
    gdp = fred_data.get("GDPC1", {})
    if "context" not in gdp:
        return {"error": "no data"}
    ctx = gdp["context"]
    current_val = ctx.get("current", {}).get("value")
    current_date = ctx.get("current", {}).get("date")
    yoy = ctx.get("yoy_pct")
    days_since = ctx.get("days_since_update", 0)
    flag = {"latest_value_yoy": yoy, "current_level": current_val, "date": current_date,
            "days_since_update": days_since, "trend_3m": ctx.get("trend_3m", "n/a"),
            "timing": "LAGGING — confirms past, not future"}
    if yoy is not None:
        if yoy >= 3.0:
            flag["reading"] = "STRONG"; flag["confirms_expansion"] = True
        elif yoy >= 1.5:
            flag["reading"] = "MODERATE"; flag["confirms_expansion"] = True
        elif yoy >= 0:
            flag["reading"] = "WEAK"; flag["confirms_expansion"] = False
            flag["note"] = "Sub-1.5% growth — growth classification should be CAUTIOUS even if coincident indicators are steady"
        else:
            flag["reading"] = "CONTRACTION"; flag["confirms_expansion"] = False
            flag["note"] = "Negative YoY GDP — recession in prior data regardless of coincident indicator reads"
    if days_since > 90:
        flag["stale_warning"] = f"GDP last updated {days_since} days ago — advance/revised estimates may have since moved"
    q1 = ctx.get("3m_ago"); q2 = ctx.get("6m_ago")
    if q1: flag["prior_q"] = {"value": q1["value"], "date": q1["date"]}
    if q2: flag["two_q_ago"] = {"value": q2["value"], "date": q2["date"]}
    return flag


# =================================================================
# PMI DIVERGENCE (v2.8)
# =================================================================

def calculate_pmi_divergence(us_ism: Optional[float], ez_pmi: Optional[float],
                              uk_pmi: Optional[float], jp_pmi: Optional[float]) -> Optional[Dict]:
    """
    Compute PMI divergence between US ISM and foreign composite PMIs.
    Expansion threshold: 50.0
    Classification:
      US_OUTPERFORMING  — US > foreign by >2pts = USD supportive
      US_UNDERPERFORMING — foreign > US by >2pts = USD headwind
      ALIGNED           — within 2pts, neutral modifier
    """
    if us_ism is None:
        return None

    results = {}
    pairs = {
        "EUR_USD": ("Eurozone", ez_pmi),
        "GBP_USD": ("UK",       uk_pmi),
        "USD_JPY": ("Japan",    jp_pmi),
    }

    for pair, (name, foreign_pmi) in pairs.items():
        if foreign_pmi is None:
            results[pair] = {"available": False, "name": name}
            continue

        diff = round(us_ism - foreign_pmi, 1)  # positive = US stronger
        us_regime      = "EXPANDING"    if us_ism      >= 50 else "CONTRACTING"
        foreign_regime = "EXPANDING"    if foreign_pmi >= 50 else "CONTRACTING"

        if abs(diff) <= 2.0:
            classification = "ALIGNED"
            signal = "NEUTRAL — no growth differential signal (within 2pt threshold)"
        elif diff > 2.0:
            classification = "US_OUTPERFORMING"
            signal = "USD supportive vs foreign — US growth premium" if pair != "USD_JPY" else "USD supportive — US growth premium over Japan"
        else:
            classification = "US_UNDERPERFORMING"
            signal = "Foreign growth outperformance — USD headwind" if pair != "USD_JPY" else "Japan closing growth gap — JPY supportive"

        if us_regime == "EXPANDING" and foreign_regime == "CONTRACTING":
            regime_flag = "DIVERGENT — US expanding, foreign contracting"
        elif us_regime == "CONTRACTING" and foreign_regime == "EXPANDING":
            regime_flag = "DIVERGENT — foreign expanding, US contracting"
        elif us_regime == "EXPANDING" and foreign_regime == "EXPANDING":
            regime_flag = "BOTH_EXPANDING"
        else:
            regime_flag = "BOTH_CONTRACTING"

        results[pair] = {
            "available": True, "name": name,
            "us_ism": us_ism, "foreign_pmi": foreign_pmi,
            "diff": diff, "us_regime": us_regime,
            "foreign_regime": foreign_regime,
            "classification": classification,
            "regime_flag": regime_flag,
            "signal": signal,
        }

    return results


# =================================================================
# FRED DATA COLLECTION
# =================================================================

class FREDCollector:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = FRED_BASE

    def fetch_series(self, series_id: str, lookback_days: int = LOOKBACK_DAYS) -> Optional[pd.DataFrame]:
        end = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        params = {"series_id": series_id, "api_key": self.api_key, "file_type": "json",
                  "observation_start": start, "observation_end": end, "sort_order": "desc"}
        try:
            r = requests.get(self.base_url, params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
            if "observations" not in data or len(data["observations"]) == 0:
                return None
            df = pd.DataFrame(data["observations"])
            df["date"] = pd.to_datetime(df["date"])
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df = df.dropna(subset=["value"])
            return df.sort_values("date", ascending=True).reset_index(drop=True)
        except Exception as e:
            print(f"  [!] Error fetching {series_id}: {e}")
            return None

    def collect_all(self) -> Dict:
        results = {}
        total = len(FRED_SERIES)
        for i, (sid, meta) in enumerate(FRED_SERIES.items(), 1):
            if sid == "TEDRATE":
                results[sid] = {"meta": meta, "error": "discontinued"}
                print(f"  [{i}/{total}] {meta['name']:<40} [SKIP — discontinued]")
                continue
            print(f"  [{i}/{total}] {meta['name']:<40}", end="", flush=True)
            lb = LOOKBACK_DAYS
            if meta.get("freq") == "quarterly":
                lb = max(LOOKBACK_DAYS, 1825)
            df = self.fetch_series(sid, lookback_days=lb)
            if df is None or len(df) == 0:
                print(" [X] NO DATA")
                results[sid] = {"meta": meta, "error": "no data"}
                continue
            ctx = self._build_context(df, meta)
            results[sid] = {"meta": meta, "context": ctx}
            v = ctx["current"]["value"]; t = ctx.get("trend_3m", "n/a")
            print(f" [OK] {v} | 3m: {t}")
            time.sleep(0.12)
        return results

    def _build_context(self, df: pd.DataFrame, meta: Dict) -> Dict:
        ctx = {}
        latest = df.iloc[-1]
        ctx["current"] = {"value": round(latest["value"], 4), "date": latest["date"].strftime("%Y-%m-%d")}
        now = df["date"].max()
        days_since = (datetime.now() - now).days
        ctx["days_since_update"] = days_since
        if meta.get("freq") == "quarterly" and days_since > 120:
            ctx["stale_warning"] = f"Last update {days_since} days ago — quarterly series, may be stale"
        for label, days in [("1m_ago", 30), ("3m_ago", 90), ("6m_ago", 180), ("12m_ago", 365)]:
            target = now - timedelta(days=days)
            mask = df[df["date"] <= target]
            if len(mask) > 0:
                row = mask.iloc[-1]
                ctx[label] = {"value": round(row["value"], 4), "date": row["date"].strftime("%Y-%m-%d")}
            else:
                ctx[label] = None
        transform = meta.get("transform", "level")
        if transform == "yoy" and len(df) > 12:
            current_val = df.iloc[-1]["value"]
            yr_ago_df = df[df["date"] <= now - timedelta(days=365)]
            if len(yr_ago_df) > 0:
                yr_ago_val = yr_ago_df.iloc[-1]["value"]
                if yr_ago_val != 0:
                    ctx["yoy_pct"] = round(((current_val - yr_ago_val) / yr_ago_val) * 100, 2)
            if len(df) >= 2:
                prev_val = df.iloc[-2]["value"]
                if prev_val != 0:
                    ctx["mom_annualized"] = round(((current_val - prev_val) / prev_val) * 12 * 100, 2)
            if len(df) >= 4:
                val_3m_df = df[df["date"] <= now - timedelta(days=90)]
                if len(val_3m_df) > 0:
                    val_3m = val_3m_df.iloc[-1]["value"]
                    if val_3m != 0:
                        months_diff = max(1, (now - val_3m_df.iloc[-1]["date"]).days / 30.44)
                        ctx["ann_3m"] = round(((current_val / val_3m) ** (12 / months_diff) - 1) * 100, 2)
        if transform == "mom_change" and len(df) >= 2:
            ctx["mom_change"] = round(df.iloc[-1]["value"] - df.iloc[-2]["value"], 1)
        ctx["trend_3m"] = self._calc_trend(df, 90)
        ctx["trend_6m"] = self._calc_trend(df, 180)
        ctx["momentum"] = self._calc_momentum(df)
        vals = df["value"].values
        if len(vals) > 10:
            ctx["percentile_2y"] = round((vals < vals[-1]).sum() / len(vals) * 100, 1)
        return ctx

    def _calc_trend(self, df, days):
        if len(df) < 5: return "insufficient_data"
        now = df["date"].max()
        period = df[df["date"] >= now - timedelta(days=days)]
        if len(period) < 2: return "insufficient_data"
        start_val = period.iloc[0]["value"]; end_val = period.iloc[-1]["value"]
        if start_val == 0: return "n/a"
        chg = ((end_val - start_val) / abs(start_val)) * 100
        return "RISING" if chg > 2 else ("FALLING" if chg < -2 else "FLAT")

    def _calc_momentum(self, df):
        if len(df) < 6: return "insufficient_data"
        recent = df.iloc[-3:]["value"].values; prior = df.iloc[-6:-3]["value"].values
        if len(recent) < 3 or len(prior) < 3: return "insufficient_data"
        rc = recent[-1] - recent[0]; pc = prior[-1] - prior[0]
        if abs(pc) < 0.0001: return "STEADY"
        if abs(rc) > abs(pc) * 1.2: return "ACCELERATING_UP" if rc > 0 else "ACCELERATING_DOWN"
        elif abs(rc) < abs(pc) * 0.8: return "DECELERATING"
        return "STEADY"


# =================================================================
# NEW INDICATOR DERIVED METRICS
# =================================================================

def calculate_new_indicators(fred_data: Dict) -> Dict:
    ni = {}
    china = fred_data.get("CRDQCNAPABIS", {})
    if "context" in china:
        ctx = china["context"]
        current = ctx["current"]["value"]
        ni["china_credit"] = {"current_level": current, "date": ctx["current"]["date"],
            "days_since_update": ctx.get("days_since_update", "?"), "stale_warning": ctx.get("stale_warning")}
        ago_6m = ctx.get("6m_ago")
        if ago_6m and ago_6m["value"] and ago_6m["value"] != 0:
            g6 = round(((current - ago_6m["value"]) / ago_6m["value"]) * 100, 2)
            ni["china_credit"]["growth_6m"] = g6
            ni["china_credit"]["signal"] = "ACCELERATING" if g6 > 2 else ("DECELERATING" if g6 < -2 else "FLAT")
            ni["china_credit"]["implication"] = {"ACCELERATING": "Bullish AUD/USD at 1Q horizon, bullish US IP at 2Q",
                "DECELERATING": "Bearish AUD/USD at 1Q horizon, bearish US IP at 2Q",
                "FLAT": "Neutral — no directional credit impulse"}[ni["china_credit"]["signal"]]
        else:
            ni["china_credit"].update({"growth_6m": None, "signal": "INSUFFICIENT_DATA", "implication": "Cannot compute 6M growth"})
        ago_12m = ctx.get("12m_ago")
        if ago_12m and ago_12m["value"] and ago_12m["value"] != 0:
            ni["china_credit"]["growth_12m"] = round(((current - ago_12m["value"]) / ago_12m["value"]) * 100, 2)
    else:
        ni["china_credit"] = {"error": "no data from FRED"}

    sloos = fred_data.get("DRTSCILM", {})
    if "context" in sloos:
        ctx = sloos["context"]
        current = ctx["current"]["value"]
        ni["sloos"] = {"current_level": current, "date": ctx["current"]["date"],
            "days_since_update": ctx.get("days_since_update", "?"), "stale_warning": ctx.get("stale_warning"),
            "trend_3m": ctx.get("trend_3m", "n/a")}
        ni["sloos"]["signal"] = ("TIGHT" if current > 20 else "MILD_TIGHTENING" if current > 0 else "NEUTRAL" if current > -20 else "EASING")
        ni["sloos"]["implication"] = {"TIGHT": "Credit supply contracting sharply. Leads IP weakness 1Q, employment 2Q, credit widening 3Q.",
            "MILD_TIGHTENING": "Mild credit tightening. Watch for escalation.",
            "NEUTRAL": "Credit conditions roughly balanced.",
            "EASING": "Credit supply expanding. Supportive for growth/employment at 1-2Q horizon."}[ni["sloos"]["signal"]]
        ago = ctx.get("3m_ago")
        if ago and ago["value"] is not None:
            chg = round(current - ago["value"], 2)
            ni["sloos"]["quarterly_change"] = chg
            ni["sloos"]["direction"] = ("TIGHTENING_SHARPLY" if chg > 10 else "TIGHTENING" if chg > 2 else
                                        "EASING_SHARPLY" if chg < -10 else "EASING" if chg < -2 else "STABLE")
    else:
        ni["sloos"] = {"error": "no data from FRED"}

    capex = fred_data.get("CEFDFSA066MSFRBPHI", {})
    if "context" in capex:
        ctx = capex["context"]
        current = ctx["current"]["value"]
        ni["philly_capex"] = {"current_level": current, "date": ctx["current"]["date"],
            "days_since_update": ctx.get("days_since_update", "?"),
            "trend_3m": ctx.get("trend_3m", "n/a"), "momentum": ctx.get("momentum", "n/a")}
        ni["philly_capex"]["signal"] = "STRONG" if current > 30 else "POSITIVE" if current > 10 else "NEUTRAL" if current > -10 else "WEAK"
        ni["philly_capex"]["implication"] = {"STRONG": "Investment intentions strong — bullish growth context at 1mo",
            "POSITIVE": "Moderate investment intentions — mildly supportive",
            "NEUTRAL": "Investment intentions flat",
            "WEAK": "Investment pullback planned — growth headwind at 1mo"}[ni["philly_capex"]["signal"]]
    else:
        ni["philly_capex"] = {"error": "no data from FRED"}

    return ni


# =================================================================
# YFINANCE
# =================================================================

class YFinanceCollector:
    def collect_all(self) -> Dict:
        results = {}
        print()
        print(f"  {'MOVE Index':<30}", end="", flush=True)
        move = self._try_fetch("^MOVE", "MOVE Index", "volatility")
        if move:
            results["MOVE"] = move
            print(f" [OK] {move['context']['current']['value']}")
        else:
            print(" [X] NO DATA")
            results["MOVE"] = {"meta": {"name": "MOVE Index", "module": "volatility"}, "error": "no data"}
        return results

    def _try_fetch(self, ticker, name, module):
        try:
            tkr = yf.Ticker(ticker)
            hist = tkr.history(period="2y")
            if hist.empty: return None
            current = round(hist["Close"].iloc[-1], 4)
            date = hist.index[-1].strftime("%Y-%m-%d")
            ctx = {"current": {"value": current, "date": date}}
            for label, days in [("1m_ago", 30), ("3m_ago", 90), ("6m_ago", 180)]:
                target = hist.index[-1] - timedelta(days=days)
                mask = hist[hist.index <= target]
                ctx[label] = {"value": round(mask["Close"].iloc[-1], 4)} if len(mask) > 0 else None
            if len(hist) >= 5:
                ctx["weekly_change"] = round(current - hist["Close"].iloc[-5], 4)
            vals = hist["Close"].values
            if len(vals) > 10:
                ctx["percentile_2y"] = round((vals < current).sum() / len(vals) * 100, 1)
            return {"meta": {"name": name, "module": module}, "context": ctx}
        except Exception:
            return None


# =================================================================
# CFTC COT
# =================================================================

class COTCollector:
    URL = "https://www.cftc.gov/dea/newcot/deafut.txt"
    CONTRACTS = {
        "EUR": {"search": "EURO FX",       "name": "Euro FX"},
        "JPY": {"search": "JAPANESE YEN",  "name": "Japanese Yen"},
        "GBP": {"search": "BRITISH POUND", "name": "British Pound"},
        "USD": {"search": "U.S. DOLLAR INDEX", "name": "USD Index"},
    }

    def collect_all(self) -> Dict:
        results = {}
        print(); print("  Fetching CFTC COT data...", end="", flush=True)
        try:
            r = requests.get(self.URL, timeout=30); r.raise_for_status()
            lines = r.text.strip().split("\n")
            if len(lines) < 2: print(" [X] Empty response"); return {"error": "empty"}
            header = [h.strip().strip('"') for h in lines[0].split(",")]
            col_map = {}
            for i, h in enumerate(header):
                hl = h.lower().strip()
                if "market_and_exchange" in hl: col_map["name"] = i
                elif "as_of_date" in hl and "yyyy" in hl: col_map["date"] = i
                elif "open_interest_all" in hl: col_map["oi"] = i
                elif "noncomm_positions_long_all" == hl: col_map["nc_long"] = i
                elif "noncomm_positions_short_all" == hl: col_map["nc_short"] = i
                elif "comm_positions_long_all" == hl: col_map["comm_long"] = i
                elif "comm_positions_short_all" == hl: col_map["comm_short"] = i
            if len(col_map) < 5:
                col_map = {"name": 0, "date": 2, "oi": 7, "nc_long": 8, "nc_short": 9, "comm_long": 11, "comm_short": 12}
            for line in lines[1:]:
                fields = [f.strip().strip('"') for f in line.split(",")]
                if len(fields) < 13: continue
                market_name = fields[col_map["name"]].upper()
                for key, contract in self.CONTRACTS.items():
                    if key in results: continue
                    if contract["search"] in market_name:
                        try:
                            nc_l = self._int(fields[col_map["nc_long"]]); nc_s = self._int(fields[col_map["nc_short"]])
                            cm_l = self._int(fields[col_map["comm_long"]]); cm_s = self._int(fields[col_map["comm_short"]])
                            oi = self._int(fields[col_map["oi"]]); rd = fields[col_map["date"]].strip()
                            results[key] = {"name": contract["name"], "report_date": rd, "nc_long": nc_l,
                                "nc_short": nc_s, "net_specs": nc_l - nc_s, "comm_long": cm_l,
                                "comm_short": cm_s, "net_comms": cm_l - cm_s, "open_interest": oi}
                        except (IndexError, ValueError) as e:
                            results[key] = {"name": contract["name"], "error": f"parse: {e}"}
            found = len([v for v in results.values() if isinstance(v, dict) and "error" not in v])
            print(f" [OK] {found}/{len(self.CONTRACTS)} contracts parsed")
            for k, v in results.items():
                if isinstance(v, dict) and "error" not in v:
                    print(f"    {v['name']}: Net Specs = {v['net_specs']:+,} | NC Long = {v['nc_long']:,} | "
                          f"NC Short = {v['nc_short']:,} | OI = {v['open_interest']:,} | Date: {v.get('report_date', '?')}")
            for key in self.CONTRACTS:
                if key not in results:
                    results[key] = {"name": self.CONTRACTS[key]["name"], "error": "not found in report"}
        except Exception as e:
            print(f" [X] Error: {e}"); results["error"] = str(e)
        return results

    def _int(self, s):
        s = s.strip().strip('"').replace(",", "")
        if not s: return 0
        return int(float(s))


# =================================================================
# COT HISTORY MANAGER
# =================================================================

class COTHistoryManager:
    HISTORY_FILE = os.path.join(OUTPUT_DIR, "cot_history.json")
    CONTRACTS = {
        "EUR": {"codes": ["099741"], "name": "EURO FX",       "search": "EURO FX"},
        "JPY": {"codes": ["097741"], "name": "JAPANESE YEN",  "search": "JAPANESE YEN"},
        "GBP": {"codes": ["096742"], "name": "BRITISH POUND", "search": "BRITISH POUND"},
    }
    CFTC_URL_PATTERNS = [
        "https://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip",
        "https://www.cftc.gov/files/dea/history/deahistfo{year}.zip",
        "https://www.cftc.gov/files/dea/history/fut_disagg_txt_{year}.zip",
    ]

    def __init__(self):
        self.history = self._load_history()

    def _load_history(self):
        if os.path.exists(self.HISTORY_FILE):
            try:
                with open(self.HISTORY_FILE, "r") as f:
                    data = json.load(f)
                total = 0
                for ccy, records in data.items():
                    if isinstance(records, list):
                        print(f"    {ccy}: {len(records)} weekly records")
                        total += len(records)
                if total == 0:
                    print("  WARNING: COT history file exists but has no records — will rebuild")
                    return {}
                return data
            except Exception as e:
                print(f"  WARNING: Could not load COT history: {e}"); return {}
        else:
            print("  No COT history found — will build on first run"); return {}

    def ensure_history(self):
        needs = any(ccy not in self.history or len(self.history.get(ccy, [])) < 52 for ccy in self.CONTRACTS)
        if not needs: return
        print("  Building COT history (3-year backfill from CFTC)...")
        success = False
        for url_pattern in self.CFTC_URL_PATTERNS:
            try:
                self._backfill_from_cftc(url_pattern)
                total = sum(len(v) for v in self.history.values() if isinstance(v, list))
                if total > 100: success = True; break
            except Exception as e:
                print(f"  URL pattern failed: {e}")
        if not success:
            try:
                self._backfill_from_cot_reports()
                success = True
            except Exception as e:
                print(f"  cot_reports also failed: {e}")
        if not success:
            print("  !! COT percentile history unavailable — re-run next week")

    def _backfill_from_cftc(self, url_pattern):
        import io, zipfile
        years = range(datetime.now().year - 3, datetime.now().year + 1)
        all_records = {ccy: [] for ccy in self.CONTRACTS}
        for year in years:
            url = url_pattern.format(year=year)
            print(f"    Fetching {year}...", end=" ", flush=True)
            try:
                resp = requests.get(url, timeout=90, headers={"User-Agent": "Mozilla/5.0"})
                if resp.status_code != 200: print(f"HTTP {resp.status_code} — skipping"); continue
                if len(resp.content) < 1000: print(f"Empty — skipping"); continue
                try:
                    z = zipfile.ZipFile(io.BytesIO(resp.content))
                except zipfile.BadZipFile:
                    print("Not a zip — skipping"); continue
                parsed = 0
                for fname in z.namelist():
                    if fname.lower().endswith((".txt", ".csv")):
                        try:
                            raw = z.read(fname).decode("utf-8", errors="ignore")
                            before = sum(len(v) for v in all_records.values())
                            self._parse_cftc_text(raw, all_records)
                            parsed += sum(len(v) for v in all_records.values()) - before
                        except Exception as ex:
                            print(f"  parse error: {ex}")
                print(f"OK (+{parsed} records)")
            except Exception as e:
                print(f"FAIL ({e})")
        for ccy in all_records:
            seen = set(); deduped = []
            for rec in sorted(all_records[ccy], key=lambda x: x["date"]):
                if rec["date"] not in seen: seen.add(rec["date"]); deduped.append(rec)
            all_records[ccy] = deduped
        total = sum(len(v) for v in all_records.values())
        if total == 0: raise RuntimeError("No records parsed from CFTC archive")
        for ccy, new_recs in all_records.items():
            if ccy not in self.history: self.history[ccy] = []
            existing = {r["date"] for r in self.history[ccy]}
            for rec in new_recs:
                if rec["date"] not in existing: self.history[ccy].append(rec)
            self.history[ccy].sort(key=lambda x: x["date"])
        self._save_history()

    def _parse_cftc_text(self, raw_text, all_records):
        lines = raw_text.strip().split("\n")
        if len(lines) < 2: return
        header = lines[0]
        sep = "," if header.count(",") > header.count("\t") else "\t"
        cols = [c.strip().strip('"') for c in header.split(sep)]
        col_map = {}; is_fin = False
        for j, c in enumerate(cols):
            cl = c.lower().replace(" ", "_").strip().strip('"')
            if any(x in cl for x in ["market_and_exchange", "market_name"]): col_map.setdefault("name", j)
            if "cftc_contract_market_code" in cl and "quotes" not in cl: col_map.setdefault("code", j)
            if cl == "report_date_as_yyyy-mm-dd": col_map["date"] = j
            elif "as_of_date" in cl and "form" in cl and "date" not in col_map: col_map["date"] = j
            if cl == "open_interest_all": col_map.setdefault("oi", j)
            if cl == "noncomm_positions_long_all": col_map["nc_long"] = j
            if cl == "noncomm_positions_short_all": col_map["nc_short"] = j
            if cl == "lev_money_positions_long_all": col_map["lev_long"] = j; is_fin = True
            if cl == "lev_money_positions_short_all": col_map["lev_short"] = j
        if "nc_long" not in col_map and "lev_long" in col_map:
            col_map["nc_long"] = col_map["lev_long"]; col_map["nc_short"] = col_map["lev_short"]
        if "nc_long" not in col_map and len(cols) >= 16 and (is_fin or "lev_money" in header.lower()):
            col_map.setdefault("name", 0); col_map.setdefault("code", 3)
            col_map.setdefault("date", 2); col_map.setdefault("oi", 7)
            col_map["nc_long"] = 14; col_map["nc_short"] = 15
        if "date" not in col_map or "nc_long" not in col_map: return
        max_col = max(col_map.values())
        for line in lines[1:]:
            fields = [f.strip().strip('"') for f in line.split(sep)]
            if len(fields) <= max_col: continue
            row_name = fields[col_map.get("name", 0)].upper() if "name" in col_map else ""
            row_code = fields[col_map.get("code", 0)].strip() if "code" in col_map else ""
            matched = None
            for ccy, info in self.CONTRACTS.items():
                if row_code in info.get("codes", []) or info["search"] in row_name: matched = ccy; break
            if not matched: continue
            try:
                date_raw = fields[col_map["date"]].strip(); date_str = None
                for fmt in ("%Y-%m-%d", "%y%m%d", "%m/%d/%Y", "%Y%m%d", "%d/%m/%Y"):
                    try: date_str = datetime.strptime(date_raw, fmt).strftime("%Y-%m-%d"); break
                    except ValueError: continue
                if not date_str: continue
                def _si(key):
                    idx = col_map.get(key)
                    if idx is None or idx >= len(fields): return 0
                    try: return int(float(fields[idx].replace(",", "").strip()))
                    except: return 0
                nc_l = _si("nc_long"); nc_s = _si("nc_short"); oi = _si("oi")
                all_records[matched].append({"date": date_str, "net_specs": nc_l - nc_s,
                    "nc_long": nc_l, "nc_short": nc_s, "open_interest": oi})
            except (ValueError, IndexError):
                continue

    def _backfill_from_cot_reports(self):
        try: import cot_reports as cot
        except ImportError: raise RuntimeError("cot_reports not installed. Run: pip install cot_reports")
        all_records = {ccy: [] for ccy in self.CONTRACTS}
        for year in range(datetime.now().year - 3, datetime.now().year + 1):
            try:
                print(f"    cot_reports: {year}...", end=" ", flush=True)
                df = cot.cot_year(year=year, cot_report_type="traders_in_financial_futures_fut")
                for ccy, info in self.CONTRACTS.items():
                    for code in info.get("codes", []):
                        mask = df["CFTC_Contract_Market_Code"].astype(str).str.strip() == code
                        for _, row in df[mask].iterrows():
                            try:
                                date_raw = str(row.get("As_of_Date_In_Form_YYMMDD", row.get("Report_Date_as_YYYY-MM-DD", "")))
                                date_str = None
                                for fmt in ("%Y-%m-%d", "%y%m%d"):
                                    try: date_str = datetime.strptime(date_raw.strip(), fmt).strftime("%Y-%m-%d"); break
                                    except ValueError: continue
                                if not date_str: continue
                                nc_l = int(row.get("NonComm_Positions_Long_All", 0)); nc_s = int(row.get("NonComm_Positions_Short_All", 0))
                                all_records[ccy].append({"date": date_str, "net_specs": nc_l - nc_s,
                                    "nc_long": nc_l, "nc_short": nc_s, "open_interest": int(row.get("Open_Interest_All", 0))})
                            except: continue
                print("OK")
            except Exception as e: print(f"FAIL ({e})")
        for ccy in all_records:
            seen = set()
            all_records[ccy] = [r for r in sorted(all_records[ccy], key=lambda x: x["date"])
                                 if r["date"] not in seen and not seen.add(r["date"])]
        total = sum(len(v) for v in all_records.values())
        if total == 0: raise RuntimeError("No records from cot_reports")
        self.history = all_records; self._save_history()

    def _save_history(self):
        os.makedirs(os.path.dirname(self.HISTORY_FILE) if os.path.dirname(self.HISTORY_FILE) else ".", exist_ok=True)
        with open(self.HISTORY_FILE, "w") as f:
            json.dump(self.history, f, indent=2)

    def compute_percentiles(self, current_cot: dict) -> dict:
        results = {}
        for ccy in ["EUR", "JPY", "GBP"]:
            entry = current_cot.get(ccy, {})
            if not isinstance(entry, dict) or "error" in entry:
                results[ccy] = {"error": "no current COT data"}; continue
            current_net = entry.get("net_specs", 0)
            history = self.history.get(ccy, [])
            if len(history) < 26:
                results[ccy] = {"net_specs": current_net, "error": f"insufficient history ({len(history)} weeks, need 26+)"}; continue
            all_nets = [r["net_specs"] for r in history]
            nets_1y = all_nets[-52:] if len(all_nets) >= 52 else all_nets
            nets_3y = all_nets[-156:] if len(all_nets) >= 156 else all_nets
            pct_1y = round(sum(1 for x in nets_1y if x < current_net) / len(nets_1y) * 100, 1)
            pct_3y = round(sum(1 for x in nets_3y if x < current_net) / len(nets_3y) * 100, 1)
            ref = pct_3y if len(all_nets) >= 156 else pct_1y
            if ref >= 90: cls = "CROWDED_LONG"; note = f"Specs {ref:.0f}th percentile LONG — crowded. Reduce size on longs, watch for squeeze on shorts."
            elif ref <= 10: cls = "CROWDED_SHORT"; note = f"Specs {ref:.0f}th percentile SHORT — crowded. Reduce size on shorts, watch for squeeze on longs."
            else: cls = "NEUTRAL"; note = f"Specs {ref:.0f}th percentile — balanced. No execution adjustment needed."
            results[ccy] = {"net_specs": current_net, "pct_1y": pct_1y, "pct_3y": pct_3y,
                "classification": cls, "execution_note": note,
                "range_1y": {"min": min(nets_1y), "max": max(nets_1y)},
                "range_3y": {"min": min(nets_3y), "max": max(nets_3y)},
                "history_weeks": len(all_nets)}
            rd = entry.get("report_date", datetime.now().strftime("%Y-%m-%d"))
            existing = {r["date"] for r in history}
            if rd not in existing:
                history.append({"date": rd, "net_specs": current_net, "nc_long": entry.get("nc_long", 0),
                    "nc_short": entry.get("nc_short", 0), "open_interest": entry.get("open_interest", 0)})
                self.history[ccy] = history
        self._save_history()
        return results


# =================================================================
# FOMC FETCHER
# =================================================================

class FOMCFetcher:
    FED_BASE = "https://www.federalreserve.gov"

    def fetch_latest_statement(self) -> Dict:
        print(); print("  Fetching latest FOMC statement...", end="", flush=True)
        try:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            r = requests.get(f"{self.FED_BASE}/newsevents/pressreleases.htm", timeout=15, headers=headers)
            r.raise_for_status()
            patterns = [r'href="(/newsevents/pressreleases/monetary\d+a?\.htm)"',
                        r'href="(/newsevents/pressreleases/monetary\d+\.htm)"']
            links = []
            for pat in patterns: links.extend(re.findall(pat, r.text))
            if not links:
                r2 = requests.get(f"{self.FED_BASE}/monetarypolicy/fomccalendars.htm", timeout=15, headers=headers)
                for pat in patterns: links.extend(re.findall(pat, r2.text))
            if not links:
                print(" [X] No links found")
                return {"status": "not_found", "note": "Could not locate FOMC statement URL. LLM should use its knowledge of the most recent FOMC decision."}
            links = list(dict.fromkeys(links))
            stmt_url = self.FED_BASE + links[0]
            r3 = requests.get(stmt_url, timeout=15, headers=headers); r3.raise_for_status()
            text = re.sub(r'<script[^>]*>.*?</script>', ' ', r3.text, flags=re.DOTALL)
            text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.DOTALL)
            text = re.sub(r'<[^>]+>', ' ', text); text = re.sub(r'\s+', ' ', text).strip()
            start_idx = 0
            for marker in ["For release at", "For immediate release", "Recent indicators suggest",
                           "The Federal Open Market Committee", "Information received since", "Economic activity"]:
                idx = text.find(marker)
                if idx != -1: start_idx = idx; break
            end_idx = len(text)
            for marker in ["Voting for", "Implementation Note", "Last Update:"]:
                idx = text.find(marker, start_idx)
                if idx != -1 and idx < end_idx: end_idx = idx + 200
            stmt_text = text[start_idx:min(end_idx, start_idx + 4000)].strip()
            date_match = re.search(r'monetary(\d{8})', links[0])
            stmt_date = date_match.group(1) if date_match else "unknown"
            stmt_date_fmt = f"{stmt_date[:4]}-{stmt_date[4:6]}-{stmt_date[6:]}" if stmt_date != "unknown" and len(stmt_date) == 8 else stmt_date
            print(f" [OK] Statement dated {stmt_date_fmt}")
            return {"date": stmt_date_fmt, "url": stmt_url, "text": stmt_text, "status": "fetched"}
        except Exception as e:
            print(f" [X] Error: {e}")
            return {"status": "error", "error": str(e), "note": "LLM should use its knowledge of the most recent FOMC decision."}


# =================================================================
# MANUAL INPUTS — v2.8 (adds foreign PMIs)
# =================================================================

def collect_manual_inputs() -> Dict:
    print()
    print("=" * 60)
    print("MANUAL DATA INPUTS")
    print("These are the only data points without a free API.")
    print("Press Enter to skip any you don't have.")
    print("=" * 60)
    print()
    manual = {}

    # Core 3
    for key, label in [("truflation", "1. Truflation CPI reading (e.g. 2.8)"),
                        ("ism_pmi",    "2. ISM Manufacturing PMI (e.g. 49.3)"),
                        ("lei",        "3. Conference Board LEI MoM % (e.g. -0.4)")]:
        val = input(f"  {label}: ").strip()
        if val:
            try: manual[key] = {"value": float(val), "name": label.split(". ")[1]}
            except ValueError: manual[key] = {"value": None, "note": "invalid input"}
        else:
            manual[key] = {"value": None, "note": "skipped"}

    # Foreign yields
    print("\n  Foreign yields for rate differentials (from TradingView):")
    print("  Press Enter to skip any you don't have.\n")
    yield_inputs = {}
    for label, desc in [("de_2y", "Germany 2Y yield"), ("de_10y", "Germany 10Y yield"),
                         ("jp_2y", "Japan 2Y yield"),   ("jp_10y", "Japan 10Y yield"),
                         ("uk_2y", "UK 2Y yield"),      ("uk_10y", "UK 10Y yield")]:
        val = input(f"  {desc} (e.g. 2.15): ").strip()
        try: yield_inputs[label] = float(val) if val else None
        except ValueError: yield_inputs[label] = None
    manual["foreign_yields"] = yield_inputs

    # Foreign PMIs (v2.8)
    print("\n  Foreign Composite PMIs for growth differential (from TradingView / investing.com):")
    print("  Press Enter to skip any you don't have.\n")
    pmi_inputs = {}
    for label, desc in [("ez_pmi", "Eurozone Composite PMI (e.g. 50.2)"),
                         ("uk_pmi", "UK Composite PMI (e.g. 51.4)"),
                         ("jp_pmi", "Japan Composite PMI (e.g. 49.8)")]:
        val = input(f"  {desc}: ").strip()
        try: pmi_inputs[label] = float(val) if val else None
        except ValueError: pmi_inputs[label] = None
    manual["foreign_pmis"] = pmi_inputs

    return manual


# =================================================================
# CALCULATIONS
# =================================================================

def calculate_differentials(fred_data: Dict, manual_data: Dict) -> Dict:
    diffs = {}
    yields = manual_data.get("foreign_yields", {})
    us_2y = fred_data.get("DGS2", {}).get("context", {}).get("current", {}).get("value")
    us_10y = fred_data.get("DGS10", {}).get("context", {}).get("current", {}).get("value")
    for code, peer in [("DE", {"name": "Germany", "key_2y": "de_2y", "key_10y": "de_10y"}),
                        ("JP", {"name": "Japan",   "key_2y": "jp_2y", "key_10y": "jp_10y"}),
                        ("UK", {"name": "UK",      "key_2y": "uk_2y", "key_10y": "uk_10y"})]:
        entry = {"peer": peer["name"]}
        p2 = yields.get(peer["key_2y"]); p10 = yields.get(peer["key_10y"])
        entry["diff_2y_bp"] = round((us_2y - p2) * 100, 1) if us_2y and p2 else "n/a (foreign yield not provided)"
        entry["diff_2y_pct"] = round(us_2y - p2, 4) if us_2y and p2 else None
        entry["diff_10y_bp"] = round((us_10y - p10) * 100, 1) if us_10y and p10 else "n/a (foreign yield not provided)"
        entry["diff_10y_pct"] = round(us_10y - p10, 4) if us_10y and p10 else None
        diffs[code] = entry
    return diffs


def calculate_derived(fred_data: Dict) -> Dict:
    d = {}
    openings = fred_data.get("JTSJOL", {}).get("context", {}).get("current", {}).get("value")
    unemployed = fred_data.get("UNEMPLOY", {}).get("context", {}).get("current", {}).get("value")
    if openings and unemployed and unemployed > 0:
        d["openings_to_unemployed"] = round(openings / unemployed, 2)
    hy = fred_data.get("BAMLH0A0HYM2", {}).get("context", {}).get("current", {}).get("value")
    ig = fred_data.get("BAMLC0A0CM", {}).get("context", {}).get("current", {}).get("value")
    if hy and ig: d["hy_minus_ig"] = round(hy - ig, 2)
    effr = fred_data.get("EFFR", {}).get("context", {}).get("current", {}).get("value")
    core_pce_yoy = fred_data.get("PCEPILFE", {}).get("context", {}).get("yoy_pct")
    if effr and core_pce_yoy: d["real_fed_funds"] = round(effr - core_pce_yoy, 2)
    walcl = fred_data.get("WALCL", {})
    if "context" in walcl: d["balance_sheet_3m_trend"] = walcl["context"].get("trend_3m", "n/a")
    t10y2y = fred_data.get("T10Y2Y", {}).get("context", {}).get("current", {}).get("value")
    t10y3m = fred_data.get("T10Y3M", {}).get("context", {}).get("current", {}).get("value")
    if t10y2y: d["curve_2s10s_state"] = "INVERTED" if t10y2y < 0 else ("FLAT" if t10y2y < 0.25 else "NORMAL")
    if t10y3m: d["curve_10y3m_state"] = "INVERTED" if t10y3m < 0 else ("FLAT" if t10y3m < 0.25 else "NORMAL")
    return d


# =================================================================
# INFLATION BRICK
# =================================================================

def calculate_inflation_brick(fred_data: Dict, manual_data: Dict) -> Dict:
    brick = {}
    components = {"shelter": {"sid": "CUSR0000SAH1", "weight": 0.36},
        "energy": {"sid": "CPIENGSL", "weight": 0.07},
        "goods": {"sid": "CUSR0000SACL1E", "weight": 0.21},
        "supercore": {"sid": "CUSR0000SASLE", "weight": 0.25},
        "food": {"sid": "CPIUFDSL", "weight": 0.14}}
    for name, info in components.items():
        entry = fred_data.get(info["sid"], {})
        if "context" in entry:
            ctx = entry["context"]
            brick[name] = {"weight": info["weight"], "yoy": ctx.get("yoy_pct"),
                "mom_ann": ctx.get("mom_annualized"), "ann_3m": ctx.get("ann_3m"),
                "trend_3m": ctx.get("trend_3m"), "trend_6m": ctx.get("trend_6m"), "momentum": ctx.get("momentum")}
        else:
            brick[name] = {"weight": info["weight"], "error": "no data"}
    uc = fred_data.get("CUSR0000SETA02", {})
    if "context" in uc:
        ctx = uc["context"]
        brick["used_cars"] = {"yoy": ctx.get("yoy_pct"), "mom_ann": ctx.get("mom_annualized"),
            "trend_3m": ctx.get("trend_3m"), "momentum": ctx.get("momentum")}
    leaders = {}
    ppi = fred_data.get("PPIFIS", {})
    if "context" in ppi:
        ctx = ppi["context"]
        leaders["ppi_pipeline"] = {"grade": "A", "yoy": ctx.get("yoy_pct"), "mom_ann": ctx.get("mom_annualized"),
            "trend_3m": ctx.get("trend_3m"),
            "signal": "RISING" if (ctx.get("mom_annualized") or 0) > 3 else ("FALLING" if (ctx.get("mom_annualized") or 0) < -1 else "NEUTRAL")}
    gas = fred_data.get("GASREGW", {})
    if "context" in gas:
        ctx = gas["context"]
        leaders["gasoline"] = {"grade": "A", "current": ctx["current"]["value"], "trend_3m": ctx.get("trend_3m"), "pct_chg_3m": None}
        ago = ctx.get("3m_ago")
        if ago and ago["value"] != 0:
            leaders["gasoline"]["pct_chg_3m"] = round(((ctx["current"]["value"] - ago["value"]) / ago["value"]) * 100, 1)
    ahe = fred_data.get("CES0500000003", {})
    if "context" in ahe:
        ctx = ahe["context"]
        yoy = ctx.get("yoy_pct")
        leaders["wages"] = {"grade": "B", "yoy": yoy, "above_threshold": (yoy or 0) > 3.5,
            "signal": "FLOOR_ACTIVE" if (yoy or 0) > 3.5 else "FLOOR_INACTIVE",
            "note": "Directional only — does NOT predict magnitude"}
    truf = manual_data.get("truflation", {})
    if truf.get("value") is not None:
        leaders["truflation"] = {"grade": "NOWCAST", "value": truf["value"]}
    brick["leaders"] = leaders
    buckets = {}
    for bname, comps in [("energy", ["energy"]), ("goods", ["goods"]), ("services_shelter", ["shelter", "supercore"])]:
        vals = [brick[c]["ann_3m"] for c in comps if c in brick and isinstance(brick[c], dict) and brick[c].get("ann_3m") is not None]
        if vals: buckets[bname] = {"avg_ann_3m": round(sum(vals) / len(vals), 2), "components": comps}
    brick["buckets_3m"] = buckets
    ppi_signal = leaders.get("ppi_pipeline", {}).get("signal", "NEUTRAL")
    driver_bucket = max(buckets.items(), key=lambda x: x[1].get("avg_ann_3m", -999))[0] if buckets else None
    brick["ppi_passthrough"] = {"ppi_direction": ppi_signal, "driver": driver_bucket,
        "gate": "ON" if driver_bucket in ["energy", "goods"] and ppi_signal == "RISING" else "OFF",
        "reason": f"Driver is {driver_bucket}, PPI is {ppi_signal}" if driver_bucket else "insufficient data"}
    return brick


# =================================================================
# LABOUR BRICK
# =================================================================

def calculate_labour_brick(fred_data: Dict) -> Dict:
    brick = {}
    def _ctx(sid): return fred_data.get(sid, {}).get("context", {})
    def _val(sid): return _ctx(sid).get("current", {}).get("value")
    def _trend(sid, p="trend_3m"): return _ctx(sid).get(p, "n/a")
    def _ago(sid, lbl="3m_ago"):
        h = _ctx(sid).get(lbl); return h.get("value") if h else None

    claims = _val("ICSA"); claims_trend = _trend("ICSA")
    cont_claims = _val("CCSA"); cont_trend = _trend("CCSA")
    claims_signal = "n/a"
    if claims is not None:
        c3m = _ago("ICSA"); c6m = _ago("ICSA", "6m_ago")
        if c3m and c6m:
            if claims > c3m * 1.10 and claims > c6m * 1.10: claims_signal = "HIGH"
            elif claims > c3m * 1.03 or claims_trend == "RISING": claims_signal = "RISING"
            else: claims_signal = "LOW"
        elif claims_trend == "RISING": claims_signal = "RISING"
        else: claims_signal = "LOW"
    brick["layoffs"] = {"signal": claims_signal, "initial_claims": claims,
        "claims_trend": claims_trend, "continuing_claims": cont_claims, "cont_trend": cont_trend}

    openings = _val("JTSJOL"); hires = _val("JTSHIL"); quits = _val("JTSQUL")
    ot = _trend("JTSJOL"); ht = _trend("JTSHIL"); qt = _trend("JTSQUL")
    fc = sum(1 for t in [ot, ht, qt] if t == "FALLING"); rc = sum(1 for t in [ot, ht, qt] if t == "RISING")
    hiring_signal = "DETERIORATING" if fc >= 2 and rc == 0 else "COOLING" if fc >= 1 else "STRONG" if rc >= 2 else "MIXED"
    brick["hiring_engine"] = {"signal": hiring_signal, "openings": openings, "openings_trend": ot,
        "hires": hires, "hires_trend": ht, "quits": quits, "quits_trend": qt}

    layoffs_disc = _val("JTSLDL"); lt = _trend("JTSLDL")
    stress_signal = "CONFIRMED" if lt == "RISING" and claims_signal in ["RISING", "HIGH"] else "CONTAINED"
    brick["stress"] = {"signal": stress_signal, "layoffs_discharges": layoffs_disc,
        "layoffs_trend": lt, "claims_confirming": claims_signal in ["RISING", "HIGH"]}

    ratio_current = ratio_3m = None
    if openings and _val("UNEMPLOY"):
        unemp = _val("UNEMPLOY")
        if unemp and unemp > 0: ratio_current = round(openings / unemp, 3)
    o3m = _ago("JTSJOL"); u3m = _ago("UNEMPLOY")
    if o3m and u3m and u3m > 0: ratio_3m = round(o3m / u3m, 3)
    td = "STABLE"
    if ratio_current and ratio_3m:
        chg = ratio_current - ratio_3m
        td = "TIGHTENING" if chg > 0.05 else ("LOOSENING" if chg < -0.05 else "STABLE")
    brick["tightness"] = {"direction": td, "ratio_current": ratio_current, "ratio_3m_ago": ratio_3m,
        "change": round(ratio_current - ratio_3m, 3) if ratio_current and ratio_3m else None}

    hours = _val("AWHMAN"); hours_trend = _trend("AWHMAN")
    h1m = _ago("AWHMAN", "1m_ago"); h3m = _ago("AWHMAN")
    hours_signal = "STABLE"
    if hours and h1m:
        if ((hours - h1m) / h1m * 100) < -1.0: hours_signal = "SOFTENING"
        elif h3m and ((hours - h3m) / h3m * 100) < -1.0: hours_signal = "EARLY_SOFTENING"
    nfp_chg = fred_data.get("PAYEMS", {}).get("context", {}).get("mom_change")
    brick["payroll_quality"] = {"signal": hours_signal, "hours": hours, "hours_trend": hours_trend,
        "nfp_change": nfp_chg, "note": "Low fire rate — use as manual confirmation only"}

    ur = _val("UNRATE"); ur_trend = _trend("UNRATE")
    sahm = _val("SAHMREALTIME"); sahm_triggered = sahm is not None and sahm >= 0.5
    brick["headline_gate"] = {"unemployment_rate": ur, "ur_trend": ur_trend,
        "sahm_value": sahm, "sahm_triggered": sahm_triggered}

    decomp = "NOT_APPLICABLE"
    if ur_trend == "RISING" or (ur and _ago("UNRATE") and ur > _ago("UNRATE")):
        if claims_signal in ["LOW"] and lt != "RISING": decomp = "ENTRANT_DRIVEN"
        elif claims_signal in ["RISING", "HIGH"] or lt == "RISING": decomp = "JOB_LOSS_DRIVEN"
        else: decomp = "MIXED"
    brick["decomposition"] = {"type": decomp}

    score = 0; checklist = []
    for q, pts, cond, ans in [
        ("Q1: Initial claims trending up?", 10, claims_signal in ["RISING", "HIGH"], claims_signal),
        ("Q2: Continuing claims trending up?", 5, cont_trend == "RISING", cont_trend),
        ("Q3: JOLTS openings declining?", 8, ot == "FALLING", ot),
        ("Q4: Hires declining?", 8, ht == "FALLING", ht),
        ("Q5: Quits declining?", 5, qt == "FALLING", qt),
        ("Q6: Layoffs/discharges rising?", 10, lt == "RISING", lt),
        ("Q7: Tightness loosening?", 10, td == "LOOSENING", td),
        ("Q8: Hours declining?", 5, hours_signal in ["SOFTENING", "EARLY_SOFTENING"], hours_signal),
        ("Q10: Unemployment rate rising?", 10, ur_trend == "RISING", ur_trend),
        ("Q11: Sahm rule triggered?", 15, sahm_triggered, f"{sahm} (>= 0.5 = trigger)"),
        ("Q12: Stress confirmed?", 14, stress_signal == "CONFIRMED", stress_signal),
    ]:
        v = 1 if cond else 0; score += v * pts
        checklist.append({"q": q, "answer": ans, "points": v * pts})
    checklist.insert(8, {"q": "Q9: Diffusion/breadth narrowing? [MANUAL]", "answer": "MANUAL", "points": "manual"})
    decomp_adj = -15 if decomp == "ENTRANT_DRIVEN" else 0
    score = max(0, score + decomp_adj)
    state = "STABLE" if score <= 25 else "COOLING" if score <= 50 else "DETERIORATING" if score <= 75 else "ACCELERATION_RISK"
    brick["checklist"] = checklist; brick["score"] = score
    brick["decomp_adjustment"] = decomp_adj; brick["state"] = state
    return brick


# =================================================================
# CONFLICT DETECTOR
# =================================================================

def detect_conflicts(fred_data, inflation_brick, labour_brick):
    conflicts = []
    def _ctx(sid): return fred_data.get(sid, {}).get("context", {})
    def _trend(sid, p="trend_3m"): return _ctx(sid).get(p, "n/a")
    def _val(sid): return _ctx(sid).get("current", {}).get("value")
    def _yoy(sid): return _ctx(sid).get("yoy_pct")
    def _mom_ann(sid): return _ctx(sid).get("mom_annualized")

    ppi_t = _trend("PPIFIS"); ccpi_t = _trend("CPILFESL")
    if ppi_t != "n/a" and ccpi_t != "n/a":
        if ppi_t == "RISING" and ccpi_t in ["FLAT", "FALLING"]:
            conflicts.append({"module": "INFLATION", "type": "PPI vs Core CPI",
                "leading": f"PPI [LEADING] {ppi_t} (MoM-ann: {_mom_ann('PPIFIS')}%)",
                "lagging": f"Core CPI [LAGGING] {ccpi_t} (YoY: {_yoy('CPILFESL')}%)",
                "weight": "LEADING", "implication": "Inflation risk RISING — pipeline pressure not yet in CPI"})
        elif ppi_t == "FALLING" and ccpi_t in ["FLAT", "RISING"]:
            conflicts.append({"module": "INFLATION", "type": "PPI vs Core CPI",
                "leading": f"PPI [LEADING] {ppi_t} (MoM-ann: {_mom_ann('PPIFIS')}%)",
                "lagging": f"Core CPI [LAGGING] {ccpi_t} (YoY: {_yoy('CPILFESL')}%)",
                "weight": "LEADING", "implication": "Inflation risk FALLING — pipeline cooling, CPI will follow"})

    gas_t = _trend("GASREGW"); en_t = _trend("CPIENGSL")
    if gas_t != "n/a" and en_t != "n/a" and gas_t != en_t and gas_t in ["RISING", "FALLING"]:
        gas_l = inflation_brick.get("leaders", {}).get("gasoline", {})
        conflicts.append({"module": "INFLATION", "type": "Gasoline vs Energy CPI",
            "leading": f"Gasoline [LEADING] {gas_t} (3M chg: {gas_l.get('pct_chg_3m', 'n/a')}%)",
            "lagging": f"Energy CPI [LAGGING] {en_t}",
            "weight": "LEADING", "implication": f"Energy CPI will follow gasoline {gas_t} — near-deterministic (A-tier)"})

    wage_t = _trend("CES0500000003"); sc_t = _trend("CUSR0000SASLE"); wage_yoy = _yoy("CES0500000003")
    if wage_t != "n/a" and sc_t != "n/a" and wage_t == "RISING" and sc_t in ["FLAT", "FALLING"]:
        conflicts.append({"module": "INFLATION", "type": "Wages vs Supercore",
            "leading": f"AHE [LEADING] {wage_t} (YoY: {wage_yoy}%)", "lagging": f"Supercore [LAGGING] {sc_t}",
            "weight": "LEADING (directional only — B-tier)",
            "implication": f"Supercore has floor support — wages {'above' if (wage_yoy or 0) > 3.5 else 'below'} 3.5% threshold"})

    claims_sig = labour_brick.get("layoffs", {}).get("signal", "n/a"); ur_t = _trend("UNRATE")
    if claims_sig in ["RISING", "HIGH"] and ur_t in ["FLAT", "FALLING"]:
        conflicts.append({"module": "LABOUR", "type": "Claims vs Unemployment",
            "leading": f"Initial Claims [LEADING] {claims_sig} ({_val('ICSA')})",
            "lagging": f"Unemployment [LAGGING] {ur_t} ({_val('UNRATE')}%)",
            "weight": "LEADING", "implication": "Unemployment likely to RISE — claims leading by 1 month (r=0.85, +39% R2)"})
    elif claims_sig == "LOW" and ur_t == "RISING":
        conflicts.append({"module": "LABOUR", "type": "Claims vs Unemployment",
            "leading": f"Initial Claims [LEADING] {claims_sig} ({_val('ICSA')})",
            "lagging": f"Unemployment [LAGGING] {ur_t} ({_val('UNRATE')}%)",
            "weight": "LEADING", "implication": "UR rise likely ENTRANT-DRIVEN, not job-loss — claims not confirming stress"})

    ot = _trend("JTSJOL"); nfp_t = _trend("PAYEMS")
    if ot == "FALLING" and nfp_t in ["FLAT", "RISING"]:
        conflicts.append({"module": "LABOUR", "type": "JOLTS Openings vs Payrolls",
            "leading": f"Openings [COINCIDENT] {ot} ({_val('JTSJOL')}K)",
            "lagging": f"Payrolls [COINCIDENT] {nfp_t}",
            "weight": "EQUAL (both coincident — investigate divergence)",
            "implication": "Openings falling while payrolls hold — likely normalisation, not deterioration"})

    hours_sig = labour_brick.get("payroll_quality", {}).get("signal", "n/a")
    if hours_sig in ["SOFTENING", "EARLY_SOFTENING"] and nfp_t in ["FLAT", "RISING"]:
        conflicts.append({"module": "LABOUR", "type": "Hours vs Payrolls",
            "leading": f"Hours [COINCIDENT] {hours_sig} ({_val('AWHMAN')}h)",
            "lagging": f"Payrolls [COINCIDENT] {nfp_t}",
            "weight": "EQUAL (both coincident — low confidence, hours rarely fire)",
            "implication": "Hours cut before headcount — monitor for payroll weakness"})

    sent_t = _trend("UMCSENT"); retail_t = _trend("RSAFS")
    if sent_t != "n/a" and retail_t != "n/a" and sent_t != retail_t and sent_t in ["FALLING", "RISING"]:
        conflicts.append({"module": "GROWTH", "type": "Sentiment vs Retail Sales",
            "leading": f"Sentiment [SOFT DATA — UNRELIABLE] {sent_t} ({_val('UMCSENT')})",
            "lagging": f"Retail Sales [COINCIDENT] {retail_t}",
            "weight": "RETAIL SALES (hard data always wins — sentiment has r=0.11)",
            "implication": "NOT a real conflict. Trust hard data entirely. Ignore sentiment."})

    curve = _val("T10Y3M"); gdp_t = _trend("GDPC1")
    if curve is not None and gdp_t != "n/a":
        if curve < 0 and gdp_t in ["FLAT", "RISING"]:
            conflicts.append({"module": "GROWTH/YIELDS", "type": "Curve Inversion vs GDP",
                "leading": f"10Y-3M Curve [LEADING] INVERTED ({curve}%)",
                "lagging": f"GDP [LAGGING] {gdp_t}",
                "weight": "LEADING", "implication": "Recession signal active — GDP has not caught up yet. Avg 14-month lead."})

    return conflicts


# =================================================================
# LLM SNAPSHOT FORMATTER — v2.8
# =================================================================

def format_snapshot(fred_data, yf_data, cot_data, fomc_data, manual_data, diffs, derived,
                    inflation_brick, labour_brick, conflicts, cot_percentiles,
                    new_indicators, yield_deltas, gdp_flag, pmi_divergence) -> str:
    L = []; a = L.append

    a("=" * 70)
    a("UD FRAMEWORK -- FUNDAMENTAL DATA SNAPSHOT")
    a(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    a(f"Script Version: {SCRIPT_VERSION} (+ PMI divergence + WoW yield deltas + GDP flag + COT fix)")
    a("=" * 70)
    a("")
    a("LLM INSTRUCTIONS:")
    a("You have the complete fundamental data below. Your task:")
    a("1. Run UD1 analysis across all modules (Inflation, Growth, Labour,")
    a("   Monetary+Fiscal, Liquidity+Rates, Correlation, Geoeconomics, Trade)")
    a("2. Run UD2 transmission check (Yields+Curve, Rate Differentials,")
    a("   Credit+Funding, Volatility+Correlation)")
    a("3. Produce output cards for each module per the step-by-step docs")
    a("4. For FOMC/policy: interpret the statement text provided below")
    a("5. For geoeconomics/trade/correlation: use your current knowledge")
    a("   combined with the data to assess regime state")
    a("6. Flag any data gaps that limit your analysis")
    a("7. Review NEW INDICATOR CONTEXT section for Tier 2 regime overlays")
    a("8. Use WEEKLY YIELD DELTA section to assess intraweek impulse direction")
    a("   alongside the 3-month regime trend tags for dual-timeframe yield analysis")
    a("9. GDP CONFIRMATION FLAG is in the Growth module — surface it in your")
    a("   output card as confirming or contradicting the expansion classification")
    a("10. PMI GROWTH DIFFERENTIAL section after Brick 2 — use as quality modifier")
    a("    for rate differential confidence scoring. CONFIRMED = higher confidence,")
    a("    UNCONFIRMED = flag divergence, ALIGNED = neutral, no adjustment.")
    a("")
    a("TIMING CLASSIFICATION KEY (EMPIRICALLY VALIDATED):")
    a("  [LEADING]              - Validated forward signal. Use for DIRECTION prediction")
    a("  [LEADING/NOWCAST]      - Sub-monthly lead. Turns before target but within same month")
    a("  [COINCIDENT]           - Moves WITH economy. Use for CONFIRMATION and regime classification")
    a("  [COINCIDENT/NOWCAST]   - Real-time confirmation signal")
    a("  [LAGGING]              - Turns AFTER economy. Use for VALIDATION only")
    a("  [NOWCAST]              - Real-time signal. Use for MONITORING")
    a("  [SOFT DATA (UNRELIABLE)] - No validated predictive power. Context/colour ONLY, never for decisions")
    a("")
    a("WEIGHTING RULES:")
    a("  1. LEADING vs LAGGING conflict: weight LEADING")
    a("  2. LEADING vs COINCIDENT conflict: weight LEADING for direction, COINCIDENT for confirmation")
    a("  3. SOFT DATA vs HARD DATA: ALWAYS trust hard data (sentiment r=0.11)")
    a("  4. Rate differential alone explains only 6-13% of EUR/USD — needs multi-brick confirmation")
    a("  5. SLOOS, China Credit, PhillyFed CapEx are Tier 2 overlays — never override Tier 1 signals")
    a("  6. TED Spread is DISCONTINUED — do not flag as missing data, do not reference")
    a("  7. PMI divergence is a Tier 2 growth differential modifier — never standalone signal")
    a("")
    a("KEY VALIDATED LEADING SIGNALS:")
    a("  - Initial Claims: 1-month lead (r=0.85, +39% R2) — fastest labour warning")
    a("  - NFCI: 10-11 month lead (+8-13% R2) — only strategic growth forecaster")
    a("  - PPI: 2-3 month lead (r=0.73) — strongest CPI pipeline signal")
    a("  - 2Y Treasury: 3 month lead over Fed Funds (r=0.97) — rate path expectations")
    a("  - IG OAS: 2 month lead (r=-0.64, +5.2% R2) — cleanest credit signal (watch over HY)")
    a("  - 10Y-3M Curve: 9-23 month recession lead (3/4 predicted)")
    a("")

    # -- INFLATION --
    a("-" * 70); a("UD1 MODULE 1: INFLATION"); a("-" * 70)
    a("  --- HEADLINE ANCHORS ---")
    for sid in ["CPIAUCSL", "CPILFESL", "PCEPI", "PCEPILFE", "PPIFIS"]:
        a(_fmt(sid, fred_data))
    truf = manual_data.get("truflation", {})
    a(f"  Truflation CPI (nowcast): {truf['value']}" if truf.get("value") is not None else "  Truflation CPI: not provided")
    a(""); a("  --- CPI COMPONENTS (3-Bucket Breakdown) ---")
    a("  Weights: Shelter 36% | Supercore 25% | Core Goods 21% | Food 14% | Energy 7%"); a("")
    for sid in ["CUSR0000SAH1", "CPIENGSL", "CUSR0000SACL1E", "CUSR0000SASLE", "CPIUFDSL", "CUSR0000SETA02"]:
        a(_fmt(sid, fred_data))
    a(""); a("  --- VALIDATED LEADING INDICATORS ---")
    for sid in ["GASREGW", "CES0500000003"]: a(_fmt(sid, fred_data))
    a(""); a("  --- INFLATION BRICK PRE-COMPUTED ---")
    buckets = inflation_brick.get("buckets_3m", {})
    for bname in ["energy", "goods", "services_shelter"]:
        b = buckets.get(bname, {})
        a(f"    Bucket [{bname.upper()}]: 3M annualized = {b.get('avg_ann_3m', 'n/a')}%")
    pt = inflation_brick.get("ppi_passthrough", {})
    a(f"    PPI Pipeline: {pt.get('ppi_direction', 'n/a')} | Pass-through Gate: {pt.get('gate', 'n/a')}")
    leaders = inflation_brick.get("leaders", {})
    ppi_l = leaders.get("ppi_pipeline", {}); gas_l = leaders.get("gasoline", {}); wage_l = leaders.get("wages", {})
    a(f"    PPI Signal [A]: {ppi_l.get('signal', 'n/a')} (MoM-ann: {ppi_l.get('mom_ann', 'n/a')}%)")
    a(f"    Gasoline [A]: ${gas_l.get('current', 'n/a')}/gal | 3M chg: {gas_l.get('pct_chg_3m', 'n/a')}%")
    a(f"    Wages [B]: {wage_l.get('yoy', 'n/a')}% YoY | Floor: {wage_l.get('signal', 'n/a')}")
    a(""); a("  LLM INFLATION CHECKLIST (score each 0 or 1):")
    a("    Q1:  Core CPI trend rising? | Q2: Core PCE trend rising?")
    a("    Q3:  MoM annualized accelerating? | Q4: MoM risen 2+ consecutive? [MANUAL]")
    a("    Q5:  Services+Shelter main driver? | Q6: Services broadening? [MANUAL]")
    a("    Q7:  2+ of 3 buckets accelerating? | Q8: PPI rising?")
    a("    Q9:  PPI pass-through ON? | Q10: Truflation rising? [MANUAL]")
    a("  Score 0-10. Output: State / Driver / Persistence / Forecast cone / Flip triggers"); a("")

    # -- GROWTH --
    a("-" * 70); a("UD1 MODULE 2: GROWTH"); a("-" * 70)
    for sid in ["HOUST", "PERMIT", "DGORDER", "RSAFS", "PCE", "INDPRO", "UMCSENT", "GDPC1", "NFCI"]:
        a(_fmt(sid, fred_data))
    pmi = manual_data.get("ism_pmi", {}); lei = manual_data.get("lei", {})
    a(f"  ISM Manufacturing PMI: {pmi['value']}" if pmi.get("value") is not None else "  ISM PMI: not provided")
    a(f"  Conference Board LEI MoM: {lei['value']}%" if lei.get("value") is not None else "  LEI: not provided")
    a("")
    a("  --- GDP CONFIRMATION FLAG (v2.7) ---")
    a("  [LAGGING — use to confirm or contradict expansion classification, not predict]")
    if "error" not in gdp_flag:
        a(f"    Latest GDP YoY: {gdp_flag.get('latest_value_yoy', 'n/a')}% ({gdp_flag.get('date', 'n/a')}) — {gdp_flag.get('days_since_update', '?')} days ago")
        a(f"    Reading: {gdp_flag.get('reading', 'n/a')} | Confirms Expansion: {gdp_flag.get('confirms_expansion', 'n/a')}")
        if gdp_flag.get("stale_warning"): a(f"    !! {gdp_flag['stale_warning']}")
        if gdp_flag.get("note"): a(f"    >> {gdp_flag['note']}")
        pq = gdp_flag.get("prior_q"); tq = gdp_flag.get("two_q_ago")
        if pq: a(f"    Prior quarter: {pq['value']} ({pq['date']})")
        if tq: a(f"    Two quarters ago: {tq['value']} ({tq['date']})")
        a(f"    LLM: If GDP reading contradicts coincident indicators, flag the gap in your output card.")
    else:
        a(f"    GDP data unavailable: {gdp_flag.get('error', 'no data')}")
    a("")

    # -- LABOUR --
    a("-" * 70); a("UD1 MODULE 3: LABOUR"); a("-" * 70)
    for sid in ["ICSA", "CCSA", "JTSJOL", "JTSHIL", "JTSQUL", "JTSLDL",
                "PAYEMS", "UNRATE", "AWHMAN", "SAHMREALTIME", "UNEMPLOY"]:
        a(_fmt(sid, fred_data))
    if "openings_to_unemployed" in derived: a(f"  >> Openings/Unemployed Ratio: {derived['openings_to_unemployed']}")
    a("")
    a("  --- LABOUR BRICK PRE-COMPUTED ---")
    a(f"    STATE: {labour_brick.get('state', 'n/a')} | SCORE: {labour_brick.get('score', 'n/a')}/100")
    if labour_brick.get("decomp_adjustment", 0) != 0: a(f"    Decomp Adj: {labour_brick['decomp_adjustment']}pts (entrant-driven = less severe)")
    lay = labour_brick.get("layoffs", {}); hire = labour_brick.get("hiring_engine", {})
    stress = labour_brick.get("stress", {}); tight = labour_brick.get("tightness", {})
    hg = labour_brick.get("headline_gate", {}); dec = labour_brick.get("decomposition", {})
    a(f"    Layoffs: {lay.get('signal', 'n/a')} | Claims: {lay.get('initial_claims', 'n/a')} ({lay.get('claims_trend', 'n/a')})")
    a(f"    Hiring Engine: {hire.get('signal', 'n/a')} | Open: {hire.get('openings_trend', 'n/a')} Hires: {hire.get('hires_trend', 'n/a')} Quits: {hire.get('quits_trend', 'n/a')}")
    a(f"    Stress: {stress.get('signal', 'n/a')} | Layoffs trend: {stress.get('layoffs_trend', 'n/a')}")
    a(f"    Tightness: {tight.get('direction', 'n/a')} | Ratio: {tight.get('ratio_current', 'n/a')} (3m ago: {tight.get('ratio_3m_ago', 'n/a')})")
    a(f"    Headline Gate: UR={hg.get('unemployment_rate', 'n/a')}% ({hg.get('ur_trend', 'n/a')}) | Sahm: {hg.get('sahm_value', 'n/a')} (triggered: {hg.get('sahm_triggered', 'n/a')})")
    a(f"    Decomposition: {dec.get('type', 'n/a')}")
    a(""); a("  LLM LABOUR CHECKLIST:")
    for item in labour_brick.get("checklist", []):
        a(f"    {item['q']} -> {item['answer']} [{item.get('points', '?')}pts]")
    a(f"  TOTAL: {labour_brick.get('score', 'n/a')}/100 -> STATE: {labour_brick.get('state', 'n/a')}"); a("")

    # -- MONETARY --
    a("-" * 70); a("UD1 MODULE 4: MONETARY + FISCAL"); a("-" * 70)
    for sid in ["FEDFUNDS", "WALCL", "DFEDTARU"]: a(_fmt(sid, fred_data))
    if "real_fed_funds" in derived: a(f"  >> Real Fed Funds (EFFR - Core PCE YoY): {derived['real_fed_funds']}%")
    if "balance_sheet_3m_trend" in derived: a(f"  >> Balance Sheet 3m Trend: {derived['balance_sheet_3m_trend']}")
    a("")
    a("  FOMC STATEMENT:")
    if fomc_data.get("status") == "fetched":
        a(f"  Date: {fomc_data['date']} | URL: {fomc_data['url']}")
        a("  ---BEGIN STATEMENT---")
        stmt = fomc_data.get("text", "")[:3000]
        words = stmt.split(); line = "  "
        for w in words:
            if len(line) + len(w) + 1 > 95: a(line); line = "  " + w
            else: line += " " + w if line.strip() else "  " + w
        if line.strip(): a(line)
        a("  ---END STATEMENT---")
    else:
        a(f"  {fomc_data.get('note', 'Not available')}")
    a("")

    # -- LIQUIDITY --
    a("-" * 70); a("UD1 MODULE 5: LIQUIDITY & RATES"); a("-" * 70)
    for sid in ["EFFR", "IORB", "DPCREDIT", "SOFR", "RRPONTSYD", "WRESBAL", "M2SL"]:
        a(_fmt(sid, fred_data))
    a("")

    # -- 6-8 --
    a("-" * 70); a("UD1 MODULES 6-8: CORRELATION / GEOECONOMICS / TRADE"); a("-" * 70)
    a("  LLM: Assess using quantitative data above combined with current knowledge.")
    a("  Geoeconomics toggle: ON if active shock affecting FX transmission."); a("")

    # -- UD2 BRICK 1 --
    a("-" * 70); a("UD2 BRICK 1: YIELDS & CURVE"); a("-" * 70)
    for sid in ["TB3MS", "DGS2", "DGS5", "DGS10", "DGS30", "T10Y2Y", "T10Y3M", "DFII10"]:
        a(_fmt(sid, fred_data))
    if "curve_2s10s_state" in derived: a(f"  >> 2s10s State: {derived['curve_2s10s_state']}")
    if "curve_10y3m_state" in derived: a(f"  >> 10y-3m State: {derived['curve_10y3m_state']}")
    a("")

    # -- WEEKLY YIELD DELTA --
    a("-" * 70); a("WEEKLY YIELD DELTA (v2.7) — Intraweek impulse vs 3M regime")
    if yield_deltas.get("available"):
        a(f"  vs prior snapshot: {yield_deltas.get('prior_date', 'unknown')}")
        a(f"  CURVE IMPULSE: {yield_deltas.get('impulse', 'UNKNOWN')}")
        a(f"  >> {yield_deltas.get('impulse_detail', '')}")
        a(""); a("  US YIELDS (WoW basis point change):")
        for label, entry in yield_deltas.get("yields", {}).items():
            delta = entry.get("delta_bp"); sig = entry.get("signal", ""); cur = entry.get("current", "n/a")
            a(f"    {label:<12}: {cur}% | WoW: {delta:+.1f}bp [{sig}]" if delta is not None else f"    {label:<12}: {cur}% | WoW: no prior")
        a(""); a("  CURVE SPREADS (WoW):")
        for label, entry in yield_deltas.get("curve", {}).items():
            delta = entry.get("delta_bp")
            if delta is not None: a(f"    {label:<12}: {entry.get('current', 'n/a')} | WoW: {delta:+.1f}bp [{entry.get('signal', '')}]")
        a(""); a("  RATE DIFFERENTIAL CHANGES (WoW):")
        for peer, entry in yield_deltas.get("differentials", {}).items():
            d2 = entry.get("2y_delta_bp"); d10 = entry.get("10y_delta_bp"); drv = entry.get("driver", "")
            parts = []
            if d2 is not None: parts.append(f"2Y: {d2:+.1f}bp [{entry.get('2y_direction', '')}]")
            if d10 is not None: parts.append(f"10Y: {d10:+.1f}bp [{entry.get('10y_direction', '')}]")
            if drv: parts.append(f"Driver: {drv}")
            a(f"    US-{peer}: {' | '.join(parts)}")
        a(""); a("  CREDIT SPREADS (WoW):")
        for label, entry in yield_deltas.get("credit", {}).items():
            delta = entry.get("delta_bp")
            if delta is not None: a(f"    {label:<12}: {entry.get('current', 'n/a')} | WoW: {delta:+.1f}bp [{entry.get('signal', '')}]")
        a("")
        a("  LLM DUAL-TIMEFRAME RULE:")
        a("  Use the 3M regime trend tags (from FRED data above) as the structural anchor.")
        a("  Use the WoW delta as the intraweek impulse signal on top of the regime.")
        a("  When WoW direction CONTRADICTS the 3M trend: note as potential inflection point.")
        a("  When WoW direction CONFIRMS the 3M trend: reinforces the regime score.")
        a("  Curve impulse type affects USD tailwind score in Brick 1:")
        a("    BEAR_FLATTENING / BEAR_STEEPENING = front-end repricing = USD tailwind consideration")
        a("    BULL_STEEPENING / BULL_FLATTENING = easing expectations = USD headwind consideration")
    else:
        a("  No prior snapshot available — WoW deltas will be computed from next run.")
        a(f"  Note: {yield_deltas.get('note', '')}")
    a("")

    # -- UD2 BRICK 2 --
    a("-" * 70); a("UD2 BRICK 2: RATE DIFFERENTIALS")
    a("  (Rate differential alone explains 6-13% of EUR/USD. Needs multi-brick confirmation.)")
    a("-" * 70)
    yields_man = manual_data.get("foreign_yields", {})
    for label, desc in [("de_2y", "Germany 2Y"), ("de_10y", "Germany 10Y"),
                         ("jp_2y", "Japan 2Y"),   ("jp_10y", "Japan 10Y"),
                         ("uk_2y", "UK 2Y"),      ("uk_10y", "UK 10Y")]:
        v = yields_man.get(label)
        a(f"  {desc}: {v if v is not None else 'not provided'}")
    a(""); a("  CALCULATED DIFFERENTIALS:")
    for code, diff in diffs.items():
        a(f"    US-{diff.get('peer', code)} 2Y: {diff.get('diff_2y_bp', 'n/a')}bp | US-{diff.get('peer', code)} 10Y: {diff.get('diff_10y_bp', 'n/a')}bp")
    a("")

    # ══════════════════════════════════════════════════════════
    # PMI GROWTH DIFFERENTIAL (v2.8) — inserted after Brick 2
    # ══════════════════════════════════════════════════════════
    a("-" * 70)
    a("PMI GROWTH DIFFERENTIAL (v2.8)")
    a("  (Use as quality modifier for UD2 Brick 2 rate differential confidence scoring)")
    a("  Expansion threshold: 50 | Divergence threshold: >2pts gap")
    a("-" * 70)
    pmi_man = manual_data.get("foreign_pmis", {})
    us_ism_val = manual_data.get("ism_pmi", {}).get("value")
    a(f"  US ISM Manufacturing: {us_ism_val if us_ism_val else 'not provided'}")
    a(f"  Eurozone Composite PMI: {pmi_man.get('ez_pmi') if pmi_man.get('ez_pmi') else 'not provided'}")
    a(f"  UK Composite PMI:      {pmi_man.get('uk_pmi') if pmi_man.get('uk_pmi') else 'not provided'}")
    a(f"  Japan Composite PMI:   {pmi_man.get('jp_pmi') if pmi_man.get('jp_pmi') else 'not provided'}")
    a("")
    if pmi_divergence is None:
        a("  PMI divergence: US ISM not provided — skip growth differential analysis")
    else:
        for pair, data in pmi_divergence.items():
            if not data.get("available"):
                a(f"  {pair} ({data['name']}): foreign PMI not provided")
                continue
            diff_sign = "+" if data["diff"] >= 0 else ""
            a(f"  {pair}: US {data['us_ism']} vs {data['name']} {data['foreign_pmi']} | "
              f"Diff: {diff_sign}{data['diff']}pts | {data['classification']}")
            a(f"    Regime: {data['regime_flag']}")
            a(f"    Signal: {data['signal']}")
            a("")
    a("  LLM RULE — PMI DIVERGENCE QUALITY MODIFIER:")
    a("  Apply to UD2 Brick 2 rate differential confidence only.")
    a("  Rate diff direction + PMI diff direction AGREE   -> CONFIRMED differential, higher confidence")
    a("  Rate diff direction + PMI diff direction DISAGREE -> UNCONFIRMED, flag divergence, reduce confidence")
    a("  ALIGNED (within 2pts)                            -> Neutral, no adjustment to Brick 2")
    a("  DIVERGENT regime (one expanding, one contracting) -> Amplify the rate differential signal")
    a("  NEVER use PMI divergence alone as a trade signal — Tier 2 overlay only.")
    a("")

    # -- UD2 BRICK 3 --
    a("-" * 70); a("UD2 BRICK 3: CREDIT & FUNDING")
    a("  (HY OAS is COINCIDENT — real-time. IG OAS is LEADING — 2m lead, watch IG over HY.)")
    a("-" * 70)
    for sid in ["BAMLH0A0HYM2", "BAMLC0A0CM", "BAMLH0A0HYM2EY"]:
        a(_fmt(sid, fred_data))
    if "hy_minus_ig" in derived: a(f"  >> HY-IG Spread: {derived['hy_minus_ig']}")
    a("  [TED Spread: discontinued — not tracked]"); a("")

    # -- UD2 BRICK 4 --
    a("-" * 70); a("UD2 BRICK 4: VOLATILITY & CORRELATION"); a("-" * 70)
    a(_fmt("VIXCLS", fred_data))
    move = yf_data.get("MOVE", {})
    if "context" in move:
        mc = move["context"]; mv = mc["current"]["value"]; mp = mc.get("percentile_2y", "n/a"); wk = mc.get("weekly_change", "n/a")
        a(f"  MOVE Index: {mv} | Pctile: {mp}% | WkChg: {wk:+.1f}" if isinstance(wk, (int, float)) else f"  MOVE Index: {mv} | Pctile: {mp}%")
    else:
        a("  MOVE Index: no data")
    a("")

    # -- NEW INDICATORS --
    a("-" * 70); a("VALIDATED NEW INDICATOR CONTEXT (Tier 2 — regime overlays only)")
    a("  RULE #14: Never override Tier 1 signals or generate standalone trade calls."); a("-" * 70); a("")
    cc = new_indicators.get("china_credit", {})
    if "error" not in cc:
        a(f"  CHINA CREDIT IMPULSE (BIS Quarterly) [LEADING 1-2Q]")
        a(f"    Level: {cc.get('current_level', 'n/a')} | Date: {cc.get('date', 'n/a')} ({cc.get('days_since_update', '?')}d ago)")
        if cc.get("stale_warning"): a(f"    !! {cc['stale_warning']}")
        a(f"    6M Growth: {cc.get('growth_6m', 'n/a')}% | Signal: {cc.get('signal', 'n/a')}")
        a(f"    >> {cc.get('implication', 'n/a')}")
    else:
        a(f"  CHINA CREDIT IMPULSE: {cc.get('error', 'no data')}")
    a("")
    sl = new_indicators.get("sloos", {})
    if "error" not in sl:
        a(f"  SLOOS C&I LOAN TIGHTENING (Fed Quarterly) [LEADING 1-3Q]")
        a(f"    Level: {sl.get('current_level', 'n/a')}% net tightening | Date: {sl.get('date', 'n/a')} ({sl.get('days_since_update', '?')}d ago)")
        if sl.get("stale_warning"): a(f"    !! {sl['stale_warning']}")
        a(f"    Trend: {sl.get('trend_3m', 'n/a')} | Direction: {sl.get('direction', 'n/a')} | Signal: {sl.get('signal', 'n/a')}")
        a(f"    >> {sl.get('implication', 'n/a')}")
    else:
        a(f"  SLOOS: {sl.get('error', 'no data')}")
    a("")
    pc = new_indicators.get("philly_capex", {})
    if "error" not in pc:
        a(f"  PHILLYFED FUTURE CAPEX (Monthly) [LEADING 1mo]")
        a(f"    Level: {pc.get('current_level', 'n/a')} | Date: {pc.get('date', 'n/a')} ({pc.get('days_since_update', '?')}d ago)")
        a(f"    Trend: {pc.get('trend_3m', 'n/a')} | Momentum: {pc.get('momentum', 'n/a')} | Signal: {pc.get('signal', 'n/a')}")
        a(f"    >> {pc.get('implication', 'n/a')}")
    else:
        a(f"  PHILLYFED FUTURE CAPEX: {pc.get('error', 'no data')}")
    a("")

    # -- COT --
    a("-" * 70); a("NERVOUS SYSTEM: COT POSITIONING (with percentile context)"); a("-" * 70)
    if isinstance(cot_data, dict):
        for key in ["EUR", "JPY", "GBP", "USD"]:
            v = cot_data.get(key, {})
            if isinstance(v, dict) and "error" not in v:
                a(f"  {v['name']}: Net Specs = {v.get('net_specs', 0):+,} | NC Long = {v.get('nc_long', 0):,} | "
                  f"NC Short = {v.get('nc_short', 0):,} | OI = {v.get('open_interest', 0):,} | Date: {v.get('report_date', '?')}")
                pct = cot_percentiles.get(key, {})
                if pct and "error" not in pct:
                    a(f"    1Y Pctile: {pct['pct_1y']}th | 3Y Pctile: {pct['pct_3y']}th | {pct['classification']}")
                    a(f"    1Y Range: [{pct['range_1y']['min']:+,} to {pct['range_1y']['max']:+,}]")
                    a(f"    3Y Range: [{pct['range_3y']['min']:+,} to {pct['range_3y']['max']:+,}]")
                    a(f"    >> {pct['execution_note']}")
                elif pct and "error" in pct:
                    a(f"    Percentile: {pct['error']}")
            elif isinstance(v, dict):
                a(f"  {v.get('name', key)}: {v.get('error', 'error')}")
    else:
        a("  COT data unavailable")
    a(""); a("  POSITIONING RULES: Execution modifier only — never directional bias.")
    a("  CROWDED_LONG (>=90th): reduce longs, watch for short squeeze.")
    a("  CROWDED_SHORT (<=10th): reduce shorts, watch for long squeeze."); a("")

    # -- CONFLICTS --
    a("-" * 70); a("LEADING vs LAGGING CONFLICT DETECTOR (v2.4)"); a("-" * 70)
    if conflicts:
        a(f"  {len(conflicts)} CONFLICT(S) DETECTED:"); a("")
        for i, c in enumerate(conflicts, 1):
            a(f"  [{i}] {c['module']} — {c['type']}")
            a(f"      LEADING:     {c['leading']}")
            a(f"      LAGGING:     {c['lagging']}")
            a(f"      WEIGHT:      {c['weight']}")
            a(f"      IMPLICATION: {c['implication']}"); a("")
    else:
        a("  No LEADING vs LAGGING conflicts detected."); a("")

    a("=" * 70)
    a(f"END OF FUNDAMENTAL SNAPSHOT {SCRIPT_VERSION}")
    a("Paste this entire output into your Operations Project for UD1 + UD2 analysis.")
    a("=" * 70)

    return "\n".join(L)


def _fmt(sid: str, fred_data: Dict) -> str:
    entry = fred_data.get(sid, {})
    meta = entry.get("meta", {}); name = meta.get("name", sid)
    timing = meta.get("timing", ""); use = meta.get("use", "")
    if "error" in entry:
        if "discontinued" in entry.get("error", ""): return ""
        return f"  {name}: NO DATA"
    ctx = entry.get("context", {}); cur = ctx.get("current", {})
    val = cur.get("value", "n/a"); date = cur.get("date", "")
    timing_tag = f"[{timing}]" if timing else ""
    parts = [f"  {name}: {val} ({date}) {timing_tag}"]
    hp = []
    for lbl in ["1m_ago", "3m_ago", "6m_ago", "12m_ago"]:
        h = ctx.get(lbl)
        if h: hp.append(f"{lbl.replace('_',' ')}={h['value']}")
    if hp: parts.append(f"[{', '.join(hp)}]")
    for k, label in [("yoy_pct", "YoY"), ("mom_annualized", "MoM-ann"), ("ann_3m", "3M-ann"), ("mom_change", "MoM-chg")]:
        v = ctx.get(k)
        if v is not None: parts.append(f"{label}:{v}{'%' if k != 'mom_change' else ''}")
    for k, label in [("trend_3m", "3mTrend"), ("trend_6m", "6mTrend"), ("momentum", "Mom"), ("percentile_2y", "Pct")]:
        v = ctx.get(k)
        if v is not None and v != "": parts.append(f"{label}:{v}{'%' if k == 'percentile_2y' else ''}")
    if ctx.get("stale_warning"): parts.append(f"!! STALE: {ctx['stale_warning']}")
    result = " | ".join(parts)
    if use: result += f"\n    -> Use: {use}"
    return result


# =================================================================
# MAIN
# =================================================================

def main():
    print()
    print("=" * 70)
    print(f"UD FRAMEWORK -- FUNDAMENTAL COLLECTOR {SCRIPT_VERSION}")
    print(f"Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("Changes: PMI divergence | COT fix | WoW yield deltas | .txt fix | GDP flag")
    print("=" * 70)
    print()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M")

    print("-" * 60); print("LOADING PRIOR SNAPSHOT (for WoW yield deltas)"); print("-" * 60)
    prior_snapshot = load_prior_snapshot(ts)
    if prior_snapshot is None:
        print("  No prior snapshot found — WoW deltas will not be available this run")
    print()

    print("-" * 60); print("FRED DATA COLLECTION (59 series)"); print("-" * 60)
    fred = FREDCollector(FRED_API_KEY)
    fred_data = fred.collect_all()

    print(); print("-" * 60); print("MARKET DATA (yfinance)"); print("-" * 60)
    yf_data = YFinanceCollector().collect_all()

    print(); print("-" * 60); print("CFTC COT POSITIONING"); print("-" * 60)
    cot_data = COTCollector().collect_all()

    print(); print("  COT PERCENTILE CONTEXT")
    cot_history = COTHistoryManager()
    cot_history.ensure_history()
    cot_percentiles = cot_history.compute_percentiles(cot_data)
    for ccy, pct in cot_percentiles.items():
        if "error" not in pct:
            print(f"    {ccy}: {pct['net_specs']:+,} | 1Y: {pct['pct_1y']}th | 3Y: {pct['pct_3y']}th | {pct['classification']}")
        else:
            print(f"    {ccy}: {pct.get('error', 'no data')}")

    print(); print("-" * 60); print("FOMC STATEMENT"); print("-" * 60)
    fomc_data = FOMCFetcher().fetch_latest_statement()

    manual_data = collect_manual_inputs()

    print(); print("-" * 60); print("DERIVED METRICS"); print("-" * 60)
    diffs = calculate_differentials(fred_data, manual_data)
    derived = calculate_derived(fred_data)
    inflation_brick = calculate_inflation_brick(fred_data, manual_data)
    labour_brick = calculate_labour_brick(fred_data)
    conflicts = detect_conflicts(fred_data, inflation_brick, labour_brick)
    new_indicators = calculate_new_indicators(fred_data)

    # PMI divergence (v2.8)
    pmi_man = manual_data.get("foreign_pmis", {})
    us_ism = manual_data.get("ism_pmi", {}).get("value")
    ez_pmi = pmi_man.get("ez_pmi"); uk_pmi = pmi_man.get("uk_pmi"); jp_pmi = pmi_man.get("jp_pmi")
    pmi_divergence = calculate_pmi_divergence(us_ism, ez_pmi, uk_pmi, jp_pmi)

    print(); print("-" * 60); print("PMI GROWTH DIFFERENTIAL (v2.8)"); print("-" * 60)
    if pmi_divergence:
        print(f"  US ISM: {us_ism}")
        for pair, data in pmi_divergence.items():
            if data.get("available"):
                sign = "+" if data["diff"] >= 0 else ""
                print(f"  {pair}: US {data['us_ism']} vs {data['name']} {data['foreign_pmi']} | {sign}{data['diff']}pts | {data['classification']}")
            else:
                print(f"  {pair} ({data['name']}): foreign PMI not provided")
    else:
        print("  US ISM not provided — PMI divergence skipped")

    print(); print("-" * 60); print("WEEKLY YIELD DELTAS (v2.7)"); print("-" * 60)
    yield_deltas = calculate_yield_deltas(fred_data, manual_data, diffs, prior_snapshot)
    if yield_deltas.get("available"):
        print(f"  Curve Impulse: {yield_deltas.get('impulse', 'UNKNOWN')}")
        print(f"  >> {yield_deltas.get('impulse_detail', '')}")
        for label, entry in yield_deltas.get("yields", {}).items():
            delta = entry.get("delta_bp")
            if delta is not None:
                print(f"    {label}: {entry.get('current', 'n/a')}% | WoW: {delta:+.1f}bp [{entry.get('signal', '')}]")
    else:
        print(f"  {yield_deltas.get('note', 'No prior snapshot')}")

    print(); print("-" * 60); print("GDP CONFIRMATION FLAG (v2.7)"); print("-" * 60)
    gdp_flag = calculate_gdp_flag(fred_data)
    if "error" not in gdp_flag:
        print(f"  Latest GDP YoY: {gdp_flag.get('latest_value_yoy', 'n/a')}% ({gdp_flag.get('date', 'n/a')})")
        print(f"  Reading: {gdp_flag.get('reading', 'n/a')} | Confirms Expansion: {gdp_flag.get('confirms_expansion', 'n/a')}")
        if gdp_flag.get("stale_warning"): print(f"  !! {gdp_flag['stale_warning']}")

    print(); print("-" * 60); print("INFLATION & LABOUR SUMMARY"); print("-" * 60)
    buckets = inflation_brick.get("buckets_3m", {})
    for bname in ["energy", "goods", "services_shelter"]:
        b = buckets.get(bname, {})
        print(f"  {bname.upper()}: 3M-ann = {b.get('avg_ann_3m', 'n/a')}%")
    pt = inflation_brick.get("ppi_passthrough", {})
    print(f"  PPI Gate: {pt.get('gate', 'n/a')} ({pt.get('reason', '')})")
    print(f"  Labour: STATE={labour_brick.get('state', 'n/a')} | SCORE={labour_brick.get('score', 'n/a')}/100")
    print(f"  New Indicators: China Credit={new_indicators.get('china_credit', {}).get('signal', 'n/a')} | "
          f"SLOOS={new_indicators.get('sloos', {}).get('signal', 'n/a')} | "
          f"CapEx={new_indicators.get('philly_capex', {}).get('signal', 'n/a')}")
    print()
    if conflicts:
        print(f"  {len(conflicts)} conflict(s): " + " | ".join(f"[{c['module']}] {c['type']}" for c in conflicts))
    else:
        print("  No LEADING vs LAGGING conflicts detected.")

    # Save JSON
    json_path = os.path.join(OUTPUT_DIR, f"ud_fundamental_{ts}.json")
    output = {
        "run_date": datetime.now().isoformat(), "script_version": SCRIPT_VERSION,
        "fred_data": _serializable(fred_data), "yfinance_data": _serializable(yf_data),
        "cot_data": cot_data, "cot_percentiles": cot_percentiles, "fomc_data": fomc_data,
        "manual_inputs": manual_data, "differentials": diffs, "derived": derived,
        "inflation_brick": inflation_brick, "labour_brick": labour_brick, "conflicts": conflicts,
        "new_indicators": new_indicators, "yield_deltas": yield_deltas, "gdp_flag": gdp_flag,
        "pmi_divergence": pmi_divergence,
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, default=str); f.flush()
    print(f"\n  [OK] Raw JSON saved: {json_path}")

    # Save .txt snapshot
    snapshot = format_snapshot(
        fred_data, yf_data, cot_data, fomc_data, manual_data,
        diffs, derived, inflation_brick, labour_brick, conflicts,
        cot_percentiles, new_indicators, yield_deltas, gdp_flag, pmi_divergence
    )
    txt_path = os.path.join(OUTPUT_DIR, f"ud_snapshot_{ts}.txt")
    saved = False
    try:
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(snapshot); f.flush()
        print(f"  [OK] LLM snapshot saved: {txt_path}"); saved = True
    except Exception as e:
        print(f"  [!] Could not write to {txt_path}: {e}")
    if not saved:
        fallback = f"ud_snapshot_{ts}.txt"
        try:
            with open(fallback, "w", encoding="utf-8") as f:
                f.write(snapshot); f.flush()
            print(f"  [OK] LLM snapshot saved (fallback): {os.path.abspath(fallback)}")
        except Exception as e2:
            print(f"  [!] Fallback write also failed: {e2}")

    print(); print(snapshot)
    print(); print("=" * 70)
    print(f"DONE — {SCRIPT_VERSION}")
    print(f"JSON:     {json_path}")
    print(f"Snapshot: {txt_path if saved else 'console only'}")
    print("Paste snapshot into Operations Project for UD1 + UD2 analysis.")
    print("=" * 70)


def _serializable(data: Dict) -> Dict:
    out = {}
    for k, v in data.items():
        if isinstance(v, dict): out[k] = _serializable(v)
        elif isinstance(v, (pd.Timestamp, datetime)): out[k] = str(v)
        elif isinstance(v, (int, float, str, bool, type(None))): out[k] = v
        elif isinstance(v, list): out[k] = v
        else: out[k] = str(v)
    return out


if __name__ == "__main__":
    main()