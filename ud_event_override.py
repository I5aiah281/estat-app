"""
UD Framework — Same-Day Event Override Generator v1.0
======================================================
Run this on any high-impact release day (NFP, CPI, FOMC, etc.)
Generates a .txt override file to paste into Operations Project
alongside your weekly snapshot.

Usage:
    python3 ud_event_override.py

Output:
    ud_data/ud_override_YYYYMMDD_HHMM.txt
"""

from datetime import datetime
import os

OUTPUT_DIR = "ud_data"

# ── Event type definitions ──────────────────────────────────────────────────
EVENT_TYPES = {
    "1":  "CPI",
    "2":  "NFP (Nonfarm Payrolls)",
    "3":  "FOMC Decision",
    "4":  "FOMC Minutes",
    "5":  "PCE / Core PCE",
    "6":  "PPI",
    "7":  "GDP (Advance / Revised / Final)",
    "8":  "JOLTS",
    "9":  "Retail Sales",
    "10": "ISM Manufacturing PMI",
    "11": "ISM Services PMI",
    "12": "Initial Claims",
    "13": "Fed Speaker (High Impact)",
    "14": "Other",
}

# ── Market reaction classifications ────────────────────────────────────────
REACTION_TYPES = {
    "1": "USD_BULLISH    — Dollar ripped, yields up",
    "2": "USD_BEARISH    — Dollar sold, yields down",
    "3": "RISK_ON        — Equities up, spreads tight, USD soft",
    "4": "RISK_OFF       — Equities down, spreads wide, USD bid",
    "5": "CONFUSED       — Initial spike reversed, no clean direction",
    "6": "MUTED          — Print in-line, minimal reaction",
    "7": "CURVE_FLATTER  — Front end sold, long end bid",
    "8": "CURVE_STEEPER  — Front end bid, long end sold",
}

# ── Surprise direction ──────────────────────────────────────────────────────
SURPRISE_TYPES = {
    "1": "BEAT           — Actual > Forecast (hawkish/positive surprise)",
    "2": "MISS           — Actual < Forecast (dovish/negative surprise)",
    "3": "IN_LINE        — Actual = Forecast (no surprise)",
    "4": "MIXED          — Headline beat, internals miss (or vice versa)",
}

# ── Policy implication ──────────────────────────────────────────────────────
POLICY_TYPES = {
    "1": "HAWKISH        — Reduces cut probability / adds hike risk",
    "2": "DOVISH         — Increases cut probability / removes hike risk",
    "3": "NEUTRAL        — No change to policy path expectations",
    "4": "AMBIGUOUS      — Market repricing unclear",
}

# ── Framework module impact ─────────────────────────────────────────────────
MODULE_LIST = [
    "UD1 Inflation",
    "UD1 Growth",
    "UD1 Labour",
    "UD1 Monetary/Fiscal",
    "UD2 Brick 1 (Yields/Curve)",
    "UD2 Brick 2 (Rate Differentials)",
    "UD2 Brick 3 (Credit/Funding)",
    "UD2 Brick 4 (Volatility/Correlation)",
]


def print_menu(title, options):
    print(f"\n  {title}")
    for k, v in options.items():
        print(f"    [{k}] {v}")


def get_choice(options, allow_skip=False):
    while True:
        val = input("  > ").strip()
        if allow_skip and val == "":
            return None
        if val in options:
            return val
        print(f"  Invalid — enter one of: {', '.join(options.keys())}")


def get_float(prompt, allow_skip=False):
    while True:
        val = input(f"  {prompt}: ").strip()
        if allow_skip and val == "":
            return None
        try:
            return float(val)
        except ValueError:
            print("  Enter a number (e.g. 3.2 or -0.1) or press Enter to skip.")


def get_text(prompt, allow_skip=False):
    val = input(f"  {prompt}: ").strip()
    if allow_skip and val == "":
        return None
    return val if val else None


def build_override():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d %H:%M")
    file_ts   = now.strftime("%Y%m%d_%H%M")

    print("\n" + "="*70)
    print("  UD FRAMEWORK — SAME-DAY EVENT OVERRIDE GENERATOR v1.0")
    print(f"  Run time: {timestamp}")
    print("="*70)
    print("\n  This generates an override .txt for your Operations Project.")
    print("  Paste it AFTER your weekly snapshot when running analysis.\n")

    # ── Step 1: Event type ──────────────────────────────────────────────────
    print_menu("STEP 1 — Select event type:", EVENT_TYPES)
    event_key  = get_choice(EVENT_TYPES)
    event_name = EVENT_TYPES[event_key]
    if event_key == "14":
        custom = get_text("Enter event name")
        event_name = custom if custom else "Other"

    # ── Step 2: Actuals vs Forecast ─────────────────────────────────────────
    print("\n" + "-"*50)
    print("  STEP 2 — Actual vs Forecast (press Enter to skip any field)\n")

    actual_headline   = get_text("Actual headline print (e.g. 151k, 3.2%, 25bps)", allow_skip=True)
    forecast_headline = get_text("Consensus forecast (e.g. 170k, 3.0%, 25bps)",    allow_skip=True)
    prior_headline    = get_text("Prior print / revision (e.g. 125k revised 110k)", allow_skip=True)

    # Secondary prints (optional)
    print("\n  Secondary prints (optional — press Enter to skip):")
    secondary = {}
    if event_key == "1":   # CPI
        secondary["Core CPI MoM"]   = get_text("Core CPI MoM actual vs forecast", allow_skip=True)
        secondary["Core CPI YoY"]   = get_text("Core CPI YoY actual vs forecast", allow_skip=True)
        secondary["Shelter MoM"]    = get_text("Shelter MoM actual vs forecast",  allow_skip=True)
        secondary["Supercore MoM"]  = get_text("Supercore MoM actual vs forecast",allow_skip=True)
    elif event_key == "2":  # NFP
        secondary["Unemployment Rate"] = get_text("Unemployment rate actual vs forecast", allow_skip=True)
        secondary["Avg Hourly Earnings MoM"] = get_text("AHE MoM actual vs forecast", allow_skip=True)
        secondary["Private Payrolls"]  = get_text("Private payrolls actual vs forecast", allow_skip=True)
        secondary["Prior revision"]    = get_text("Prior month revision (e.g. -22k)", allow_skip=True)
    elif event_key == "3":  # FOMC
        secondary["Dot Plot shift"]    = get_text("Dot plot change (e.g. median 2026 cut removed)", allow_skip=True)
        secondary["Statement changes"] = get_text("Key statement language changes", allow_skip=True)
        secondary["Press conf tone"]   = get_text("Powell press conference tone (hawkish/dovish/balanced)", allow_skip=True)
    elif event_key == "5":  # PCE
        secondary["Core PCE MoM"]  = get_text("Core PCE MoM actual vs forecast", allow_skip=True)
        secondary["Core PCE YoY"]  = get_text("Core PCE YoY actual vs forecast", allow_skip=True)
    elif event_key == "7":  # GDP
        secondary["GDP type"]      = get_text("Advance / Revised / Final?", allow_skip=True)
        secondary["PCE component"] = get_text("PCE contribution (e.g. PCE +2.1%)", allow_skip=True)
        secondary["Deflator"]      = get_text("GDP deflator actual vs forecast",  allow_skip=True)

    # ── Step 3: Surprise direction ──────────────────────────────────────────
    print_menu("\n  STEP 3 — Surprise direction:", SURPRISE_TYPES)
    surprise_key = get_choice(SURPRISE_TYPES)
    surprise     = SURPRISE_TYPES[surprise_key]

    # ── Step 4: Market reaction ─────────────────────────────────────────────
    print_menu("\n  STEP 4 — Initial market reaction (first 30-60 min):", REACTION_TYPES)
    reaction_key = get_choice(REACTION_TYPES)
    reaction     = REACTION_TYPES[reaction_key]

    # Market moves
    print("\n  Key market moves at time of override (press Enter to skip):")
    eurusd_move  = get_text("EUR/USD move (e.g. -45 pips, +0.35%)", allow_skip=True)
    usdjpy_move  = get_text("USD/JPY move (e.g. +80 pips)",         allow_skip=True)
    gbpusd_move  = get_text("GBP/USD move",                         allow_skip=True)
    us10y_move   = get_text("US 10Y yield move (e.g. +8bp)",        allow_skip=True)
    us2y_move    = get_text("US 2Y yield move (e.g. +12bp)",        allow_skip=True)
    dxy_move     = get_text("DXY move (e.g. +0.4%)",                allow_skip=True)
    spx_move     = get_text("SPX / equity reaction (e.g. -0.8%)",   allow_skip=True)

    # ── Step 5: Policy implication ──────────────────────────────────────────
    print_menu("\n  STEP 5 — Policy implication:", POLICY_TYPES)
    policy_key = get_choice(POLICY_TYPES)
    policy     = POLICY_TYPES[policy_key]

    cuts_before = get_text("Implied cuts priced BEFORE print (e.g. 1.8 cuts in 2026)", allow_skip=True)
    cuts_after  = get_text("Implied cuts priced AFTER print  (e.g. 1.2 cuts in 2026)", allow_skip=True)

    # ── Step 6: Framework module impact ────────────────────────────────────
    print("\n  STEP 6 — Which framework modules does this update?\n")
    for i, m in enumerate(MODULE_LIST, 1):
        print(f"    [{i}] {m}")
    print("  Enter module numbers affected (comma-separated, e.g. 1,3,5):")
    raw = input("  > ").strip()
    affected_modules = []
    for x in raw.split(","):
        x = x.strip()
        if x.isdigit() and 1 <= int(x) <= len(MODULE_LIST):
            affected_modules.append(MODULE_LIST[int(x) - 1])

    # ── Step 7: Analyst notes ───────────────────────────────────────────────
    print("\n  STEP 7 — Your analytical notes (press Enter to skip)\n")
    thesis_impact    = get_text("Impact on current thesis / open position", allow_skip=True)
    weekly_override  = get_text("Does this override any weekly snapshot reading? (Y/N + detail)", allow_skip=True)
    action_required  = get_text("Action required? (e.g. invalidate long, reduce size, monitor)", allow_skip=True)
    additional_notes = get_text("Additional notes", allow_skip=True)

    # ── Build output ────────────────────────────────────────────────────────
    lines = []
    lines.append("=" * 70)
    lines.append("  UD FRAMEWORK — SAME-DAY EVENT OVERRIDE")
    lines.append(f"  Generated: {timestamp}")
    lines.append(f"  Event: {event_name}")
    lines.append("=" * 70)

    lines.append("\nLLM INSTRUCTIONS:")
    lines.append("This override supplements the weekly fundamental snapshot.")
    lines.append("Priority rules:")
    lines.append("  1. Same-day actuals SUPERSEDE stale FRED readings for affected modules.")
    lines.append("  2. Re-score affected modules using override data + existing snapshot context.")
    lines.append("  3. If surprise direction CONTRADICTS weekly regime: flag as INFLECTION RISK.")
    lines.append("  4. If surprise CONFIRMS weekly regime: note as REGIME REINFORCEMENT.")
    lines.append("  5. Update rate differential assessment if yields moved significantly (>5bp).")
    lines.append("  6. Do NOT change modules not listed in AFFECTED MODULES section.")
    lines.append("  7. COT positioning context from weekly snapshot remains valid — do not override.")

    lines.append("\n" + "-"*50)
    lines.append("  RELEASE DATA")
    lines.append("-"*50)
    if actual_headline:
        lines.append(f"  Actual:   {actual_headline}")
    if forecast_headline:
        lines.append(f"  Forecast: {forecast_headline}")
    if prior_headline:
        lines.append(f"  Prior:    {prior_headline}")
    lines.append(f"  Surprise: {surprise}")

    if any(v for v in secondary.values()):
        lines.append("\n  SECONDARY PRINTS:")
        for k, v in secondary.items():
            if v:
                lines.append(f"    {k}: {v}")

    lines.append("\n" + "-"*50)
    lines.append("  MARKET REACTION (first 30-60 min)")
    lines.append("-"*50)
    lines.append(f"  Reaction type: {reaction}")
    if eurusd_move:  lines.append(f"  EUR/USD:  {eurusd_move}")
    if usdjpy_move:  lines.append(f"  USD/JPY:  {usdjpy_move}")
    if gbpusd_move:  lines.append(f"  GBP/USD:  {gbpusd_move}")
    if us10y_move:   lines.append(f"  US 10Y:   {us10y_move}")
    if us2y_move:    lines.append(f"  US 2Y:    {us2y_move}")
    if dxy_move:     lines.append(f"  DXY:      {dxy_move}")
    if spx_move:     lines.append(f"  SPX:      {spx_move}")

    lines.append("\n" + "-"*50)
    lines.append("  POLICY IMPLICATION")
    lines.append("-"*50)
    lines.append(f"  Direction: {policy}")
    if cuts_before: lines.append(f"  Cuts priced before: {cuts_before}")
    if cuts_after:  lines.append(f"  Cuts priced after:  {cuts_after}")

    lines.append("\n" + "-"*50)
    lines.append("  AFFECTED FRAMEWORK MODULES")
    lines.append("-"*50)
    if affected_modules:
        for m in affected_modules:
            lines.append(f"  >> {m} — re-score using override data above")
    else:
        lines.append("  (none specified)")

    lines.append("\n" + "-"*50)
    lines.append("  ANALYST NOTES")
    lines.append("-"*50)
    if thesis_impact:
        lines.append(f"  Thesis impact:    {thesis_impact}")
    if weekly_override:
        lines.append(f"  Weekly override:  {weekly_override}")
    if action_required:
        lines.append(f"  Action required:  {action_required}")
    if additional_notes:
        lines.append(f"  Notes:            {additional_notes}")

    lines.append("\n" + "="*70)
    lines.append("  END OF EVENT OVERRIDE")
    lines.append(f"  Paste AFTER weekly snapshot in Operations Project.")
    lines.append("="*70)

    output = "\n".join(lines)

    # ── Save file ───────────────────────────────────────────────────────────
    fname = os.path.join(OUTPUT_DIR, f"ud_override_{file_ts}.txt")
    try:
        with open(fname, "w", encoding="utf-8") as f:
            f.write(output)
            f.flush()
        print(f"\n  [OK] Override saved: {fname}")
    except Exception as e:
        fallback = f"ud_override_{file_ts}.txt"
        with open(fallback, "w", encoding="utf-8") as f:
            f.write(output)
        print(f"\n  [WARN] Saved to current dir instead: {fallback}")
        print(f"  Error was: {e}")

    print("\n  Paste the .txt file into your Operations Project alongside")
    print("  the weekly snapshot. The LLM will prioritise override data")
    print("  for affected modules automatically.\n")

    return fname


if __name__ == "__main__":
    build_override()
