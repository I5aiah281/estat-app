#!/usr/bin/env python3
"""
UD Framework -- Foreign Economy Module v2.2
Standalone. No Script 1 dependency.

Sources:
  FRED     -> US baseline (EFFR, Core PCE, UR), ECB rate, interbank rates, JP UR, EZ GDP
  ECB SDW  -> EZ HICP headline + core
  Eurostat -> EZ unemployment
  ONS      -> UK CPI, UK core CPI, UK unemployment
  e-Stat   -> Japan CPI, Japan core CPI

Analytics:
  Differential trends (rate, inflation) -- 3m direction (WIDENING/COMPRESSING/STABLE)

Setup:
    pip3 install fredapi requests
    export FRED_API_KEY="your_key"
    export ESTAT_API_KEY="your_key"
    python3 foreign_economy.py
"""

import os
import json
import csv
import io
import warnings
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import requests

warnings.filterwarnings("ignore")

STALENESS_DAYS = 120

# ============================================================
# FRED
# ============================================================

FRED_SERIES = {
    "US_EFFR":     {"id": "EFFR",               "name": "US EFFR",           "calc_yoy": False},
    "US_CORE_PCE": {"id": "PCEPILFE",           "name": "US Core PCE",       "calc_yoy": True},
    "US_UR":       {"id": "UNRATE",             "name": "US Unemployment",   "calc_yoy": False},
    "ECB_RATE":    {"id": "ECBDFR",             "name": "ECB Deposit Rate",  "calc_yoy": False},
    "EZ_3M":       {"id": "IR3TIB01EZM156N",   "name": "EZ 3M Interbank",   "calc_yoy": False},
    "EZ_GDP":      {"id": "CLVMNACSCAB1GQEA19", "name": "EZ Real GDP",      "calc_yoy": True},
    "JP_UR":       {"id": "LRUNTTTTJPM156S",    "name": "Japan Unemployment","calc_yoy": False},
    "JP_3M":       {"id": "IR3TIB01JPM156N",   "name": "Japan 3M Interbank","calc_yoy": False},
    "UK_3M":       {"id": "IR3TIB01GBM156N",   "name": "UK 3M Interbank",   "calc_yoy": False},
}


def fetch_fred_all():
    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        print("  [!] FRED_API_KEY not set")
        return {}
    from fredapi import Fred
    fred = Fred(api_key=api_key)
    print("  FRED API connected.\n  --- FRED ---")

    results = {}
    start = datetime.now() - timedelta(days=60 * 31)
    for key, cfg in FRED_SERIES.items():
        print(f"    {cfg['name']}...", end=" ")
        try:
            data = fred.get_series(cfg["id"], observation_start=start)
            if data is None or data.empty:
                data = fred.get_series(cfg["id"])
            data = data.dropna()
            if data.empty:
                print("FAILED"); results[key] = None; continue

            val = float(data.iloc[-1])
            dt = data.index[-1].to_pydatetime().replace(tzinfo=None)
            age = (datetime.now() - dt).days

            yoy = val
            if cfg["calc_yoy"] and len(data) >= 13:
                yoy = round(((float(data.iloc[-1]) / float(data.iloc[-13])) - 1) * 100, 2)

            trend = "N/A"
            if len(data) >= 4:
                r3 = data.iloc[-3:]
                if r3.iloc[-1] > r3.iloc[0] * 1.005: trend = "RISING"
                elif r3.iloc[-1] < r3.iloc[0] * 0.995: trend = "FALLING"
                else: trend = "FLAT"

            stale = age > STALENESS_DAYS
            s = " STALE" if stale else ""
            print(f"{round(yoy,2)}% ({trend}){s}")
            results[key] = {"value": val, "yoy": yoy, "date": str(dt.date()),
                           "age": age, "stale": stale, "trend": trend,
                           "series_last6": list(data.iloc[-6:].values)}
        except Exception as e:
            print(f"FAILED: {e}"); results[key] = None
    return results


# ============================================================
# ECB Statistical Data Warehouse
# ============================================================

def parse_ecb_csv(text):
    """Parse ECB SDW CSV with proper CSV handling."""
    reader = csv.reader(io.StringIO(text))
    header = next(reader)
    ti = next(i for i, h in enumerate(header) if "TIME_PERIOD" in h)
    vi = next(i for i, h in enumerate(header) if "OBS_VALUE" in h)
    readings = []
    for row in reader:
        if len(row) > max(ti, vi) and row[vi]:
            try:
                readings.append((row[ti], float(row[vi])))
            except ValueError:
                pass
    readings.sort(key=lambda x: x[0])
    return readings


def fetch_ecb():
    print("\n  --- ECB SDW ---")
    results = {}
    series = {
        "EZ_HICP": {
            "url": "https://data-api.ecb.europa.eu/service/data/ICP/M.U2.N.000000.4.ANR?lastNObservations=12&format=csvdata",
            "name": "EZ HICP Headline",
        },
        "EZ_CORE": {
            "url": "https://data-api.ecb.europa.eu/service/data/ICP/M.U2.N.XEF000.4.ANR?lastNObservations=12&format=csvdata",
            "name": "EZ Core HICP (ex energy/food)",
        },
    }

    for key, cfg in series.items():
        print(f"    {cfg['name']}...", end=" ")
        try:
            r = requests.get(cfg["url"], timeout=15)
            if not r.ok:
                print(f"FAILED HTTP {r.status_code}"); results[key] = None; continue

            readings = parse_ecb_csv(r.text)
            if not readings:
                print("FAILED (no data)"); results[key] = None; continue

            last_period, last_val = readings[-1]
            yr, mn = last_period.split("-")
            dt = datetime(int(yr), int(mn), 1)
            age = (datetime.now() - dt).days

            trend = "N/A"
            if len(readings) >= 3:
                r3 = [x[1] for x in readings[-3:]]
                if r3[-1] > r3[0] + 0.1: trend = "RISING"
                elif r3[-1] < r3[0] - 0.1: trend = "FALLING"
                else: trend = "FLAT"

            stale = age > STALENESS_DAYS
            vals = [x[1] for x in readings]
            s = " STALE" if stale else ""
            print(f"{last_val}% ({trend}) [{last_period}]{s}")
            results[key] = {"value": last_val, "yoy": last_val, "date": last_period,
                           "age": age, "stale": stale, "trend": trend,
                           "series_vals": vals}
        except Exception as e:
            print(f"FAILED: {e}"); results[key] = None
    return results


# ============================================================
# Eurostat Direct API
# ============================================================

def fetch_eurostat():
    print("\n  --- Eurostat ---")
    results = {}
    print("    EZ Unemployment...", end=" ")
    try:
        url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/une_rt_m?geo=EA20&s_adj=SA&age=TOTAL&unit=PC_ACT&sex=T&lastTimePeriod=12"
        r = requests.get(url, timeout=15)
        if not r.ok:
            print(f"FAILED HTTP {r.status_code}"); results["EZ_UR"] = None
            return results

        d = r.json()
        vals = d.get("value", {})
        dims = d.get("dimension", {}).get("time", {}).get("category", {}).get("index", {})
        readings = []
        for period, idx in sorted(dims.items(), key=lambda x: x[1]):
            v = vals.get(str(idx))
            if v is not None:
                readings.append((period, float(v)))

        if readings:
            last_period, last_val = readings[-1]
            yr, mn = last_period.split("-")
            dt = datetime(int(yr), int(mn), 1)
            age = (datetime.now() - dt).days
            trend = "N/A"
            if len(readings) >= 3:
                r3 = [x[1] for x in readings[-3:]]
                if r3[-1] > r3[0] + 0.1: trend = "RISING"
                elif r3[-1] < r3[0] - 0.1: trend = "FALLING"
                else: trend = "FLAT"
            stale = age > STALENESS_DAYS
            s = " STALE" if stale else ""
            print(f"{last_val}% ({trend}) [{last_period}]{s}")
            results["EZ_UR"] = {"value": last_val, "yoy": last_val, "date": last_period,
                                "age": age, "stale": stale, "trend": trend}
        else:
            print("FAILED (no values)"); results["EZ_UR"] = None
    except Exception as e:
        print(f"FAILED: {e}"); results["EZ_UR"] = None
    return results


# ============================================================
# ONS (UK Office for National Statistics)
# ============================================================

MONTH_MAP = {"January": 1, "February": 2, "March": 3, "April": 4,
             "May": 5, "June": 6, "July": 7, "August": 8,
             "September": 9, "October": 10, "November": 11, "December": 12}


def parse_ons_months(months_data):
    readings = []
    for m in months_data:
        yr = m.get("year", "")
        mn = m.get("month", m.get("quarter", ""))
        val = m.get("value", "")
        if yr and val and val != "":
            try:
                fval = float(val)
                mn_num = MONTH_MAP.get(mn, 0)
                if mn_num > 0:
                    readings.append((f"{yr}-{mn_num:02d}", int(yr), mn_num, fval))
            except ValueError:
                pass
    readings.sort(key=lambda x: (x[1], x[2]))
    return readings


def fetch_ons():
    print("\n  --- ONS (UK) ---")
    results = {}
    series = {
        "UK_CPI":  {"url": "https://www.ons.gov.uk/economy/inflationandpriceindices/timeseries/d7g7/mm23/data",
                    "name": "UK CPI Headline"},
        "UK_CORE": {"url": "https://www.ons.gov.uk/economy/inflationandpriceindices/timeseries/dko8/mm23/data",
                    "name": "UK Core CPI"},
        "UK_UR":   {"url": "https://www.ons.gov.uk/employmentandlabourmarket/peoplenotinwork/unemployment/timeseries/mgsx/lms/data",
                    "name": "UK Unemployment"},
    }

    for key, cfg in series.items():
        print(f"    {cfg['name']}...", end=" ")
        try:
            r = requests.get(cfg["url"], timeout=15)
            if not r.ok:
                print(f"FAILED HTTP {r.status_code}"); results[key] = None; continue

            d = r.json()
            months = d.get("months", [])
            if not months: months = d.get("quarters", [])
            if not months:
                print("FAILED (no data)"); results[key] = None; continue

            readings = parse_ons_months(months)
            if not readings:
                print("FAILED (no valid readings)"); results[key] = None; continue

            last = readings[-1]
            period, yr, mn, val = last
            dt = datetime(yr, mn, 1)
            age = (datetime.now() - dt).days

            trend = "N/A"
            if len(readings) >= 3:
                r3 = [x[3] for x in readings[-3:]]
                if r3[-1] > r3[0] + 0.1: trend = "RISING"
                elif r3[-1] < r3[0] - 0.1: trend = "FALLING"
                else: trend = "FLAT"

            stale = age > STALENESS_DAYS
            all_vals = [x[3] for x in readings[-12:]]
            s = " STALE" if stale else ""
            print(f"{val}% ({trend}) [{period}]{s}")
            results[key] = {"value": val, "yoy": val, "date": period,
                           "age": age, "stale": stale, "trend": trend,
                           "series_vals": all_vals}
        except Exception as e:
            print(f"FAILED: {e}"); results[key] = None
    return results


# ============================================================
# Japan e-Stat API
# ============================================================

def fetch_estat():
    print("\n  --- e-Stat (Japan) ---")
    results = {}
    api_key = os.environ.get("ESTAT_API_KEY")
    if not api_key:
        print("  [!] ESTAT_API_KEY not set. Run: export ESTAT_API_KEY='your_key'")
        return results

    base = f"https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData?appId={api_key}&statsDataId=0003427113&cdArea=00000&lang=E&cdTab=3&cdTimeFrom=2024000707"

    series = {
        "JP_CPI":  {"cat": "0001", "name": "Japan CPI (All Items)"},
        "JP_CORE": {"cat": "0060", "name": "Japan Core CPI (ex fresh food + energy)"},
    }

    for key, cfg in series.items():
        print(f"    {cfg['name']}...", end=" ")
        try:
            url = f"{base}&cdCat01={cfg['cat']}"
            r = requests.get(url, timeout=15)
            if not r.ok:
                print(f"FAILED HTTP {r.status_code}"); results[key] = None; continue

            d = r.json()
            status = d.get("GET_STATS_DATA", {}).get("RESULT", {}).get("STATUS", -1)
            if status != 0:
                print("FAILED (API error)"); results[key] = None; continue

            values = d.get("GET_STATS_DATA", {}).get("STATISTICAL_DATA", {}).get("DATA_INF", {}).get("VALUE", [])
            readings = []
            for v in values:
                tc = v.get("@time", "")
                val = v.get("$", "")
                if tc and val and val != "-":
                    yr = int(tc[:4])
                    mn = int(tc[6:8])
                    readings.append((f"{yr}-{mn:02d}", yr, mn, float(val)))

            readings.sort(key=lambda x: (x[1], x[2]))
            if not readings:
                print("FAILED (no data)"); results[key] = None; continue

            last = readings[-1]
            period, yr, mn, val = last
            dt = datetime(yr, mn, 1)
            age = (datetime.now() - dt).days

            trend = "N/A"
            if len(readings) >= 3:
                r3 = [x[3] for x in readings[-3:]]
                if r3[-1] > r3[0] + 0.1: trend = "RISING"
                elif r3[-1] < r3[0] - 0.1: trend = "FALLING"
                else: trend = "FLAT"

            stale = age > STALENESS_DAYS
            all_vals = [x[3] for x in readings[-12:]]
            s = " STALE" if stale else ""
            print(f"{val}% ({trend}) [{period}]{s}")
            results[key] = {"value": val, "yoy": val, "date": period,
                           "age": age, "stale": stale, "trend": trend,
                           "series_vals": all_vals}
        except Exception as e:
            print(f"FAILED: {e}"); results[key] = None
    return results


# ============================================================
# BUILD SUMMARIES + ANALYTICS
# ============================================================

def g(d, k, f="yoy"):
    if k in d and d[k] is not None: return d[k].get(f)
    return None


def infl_class(yoy, target=2.0):
    if yoy is None: return "N/A"
    if yoy > target + 1.5: return "HOT"
    if yoy > target + 0.5: return "ABOVE_TARGET"
    if yoy >= target - 0.5: return "AT_TARGET"
    if yoy >= target - 1.5: return "BELOW_TARGET"
    return "DEFLATIONARY_RISK"


def diff_trend(current, prev):
    if current is None or prev is None: return "N/A"
    delta = current - prev
    if abs(delta) < 3: return "STABLE"
    if delta > 0: return "WIDENING"
    return "COMPRESSING"


def build_summaries(fred, ecb, eurostat, ons, estat):
    us_effr = g(fred, "US_EFFR", "value")
    us_pce = g(fred, "US_CORE_PCE")
    us_ur = g(fred, "US_UR", "value")
    us = {"effr": us_effr, "core_pce": us_pce, "unemployment": us_ur}

    econs = {}

    # --- EUROZONE ---
    ez_cpi = g(ecb, "EZ_HICP"); ez_core = g(ecb, "EZ_CORE")
    ez_rate = g(fred, "ECB_RATE", "value"); ez_3m = g(fred, "EZ_3M", "value")
    ez_ur = g(eurostat, "EZ_UR"); ez_gdp = g(fred, "EZ_GDP")

    rdiff = round((us_effr - ez_rate) * 100) if us_effr and ez_rate else None
    idiff = round(us_pce - ez_core, 2) if us_pce and ez_core else None
    udiff = round(us_ur - ez_ur, 1) if us_ur and ez_ur else None

    # Rate diff trend
    ez_3m_hist = g(fred, "EZ_3M", "series_last6")
    rdiff_3m_ago = round((us_effr - ez_3m_hist[-3]) * 100) if ez_3m_hist and len(ez_3m_hist) >= 4 and us_effr else None
    rdiff_t = diff_trend(rdiff, rdiff_3m_ago)

    # Inflation diff trend
    ez_core_hist = g(ecb, "EZ_CORE", "series_vals")
    idiff_3m_ago = round(us_pce - ez_core_hist[-4], 2) if ez_core_hist and len(ez_core_hist) >= 4 and us_pce else None
    idiff_t = diff_trend(int(idiff * 100) if idiff else None, int(idiff_3m_ago * 100) if idiff_3m_ago else None)

    econs["EZ"] = {
        "label": "Eurozone (ECB)", "pair": "EUR/USD",
        "headline_cpi": ez_cpi, "core_cpi": ez_core,
        "inflation_class": infl_class(ez_cpi),
        "policy_rate": ez_rate, "interbank_3m": ez_3m,
        "unemployment": ez_ur, "gdp_yoy": ez_gdp,
        "rate_diff_bp": rdiff, "inflation_diff_pp": idiff, "ur_diff_pp": udiff,
        "rate_diff_trend": rdiff_t, "inflation_diff_trend": idiff_t,
        "cpi_date": g(ecb, "EZ_HICP", "date"), "core_date": g(ecb, "EZ_CORE", "date"),
        "ur_date": g(eurostat, "EZ_UR", "date"),
    }

    # --- JAPAN ---
    jp_cpi = g(estat, "JP_CPI"); jp_core = g(estat, "JP_CORE")
    jp_ur = g(fred, "JP_UR", "value"); jp_3m = g(fred, "JP_3M", "value")

    rdiff_jp = round((us_effr - jp_3m) * 100) if us_effr and jp_3m else None
    idiff_jp = round(us_pce - jp_cpi, 2) if us_pce and jp_cpi else None

    jp_3m_hist = g(fred, "JP_3M", "series_last6")
    rdiff_jp_3m = round((us_effr - jp_3m_hist[-3]) * 100) if jp_3m_hist and len(jp_3m_hist) >= 4 and us_effr else None
    rdiff_jp_t = diff_trend(rdiff_jp, rdiff_jp_3m)

    jp_cpi_hist = g(estat, "JP_CPI", "series_vals")
    idiff_jp_3m = round(us_pce - jp_cpi_hist[-4], 2) if jp_cpi_hist and len(jp_cpi_hist) >= 4 and us_pce else None
    idiff_jp_t = diff_trend(int(idiff_jp * 100) if idiff_jp else None, int(idiff_jp_3m * 100) if idiff_jp_3m else None)

    econs["JP"] = {
        "label": "Japan (BOJ)", "pair": "USD/JPY",
        "headline_cpi": jp_cpi, "core_cpi": jp_core,
        "inflation_class": infl_class(jp_cpi),
        "policy_rate": jp_3m,
        "policy_rate_note": "3M interbank proxy (BOJ official ~0.50%, 3M includes term premium)",
        "unemployment": jp_ur, "gdp_yoy": None,
        "rate_diff_bp": rdiff_jp, "inflation_diff_pp": idiff_jp,
        "rate_diff_trend": rdiff_jp_t, "inflation_diff_trend": idiff_jp_t,
        "cpi_date": g(estat, "JP_CPI", "date"), "core_date": g(estat, "JP_CORE", "date"),
        "ur_date": g(fred, "JP_UR", "date"),
    }

    # --- UK ---
    uk_cpi = g(ons, "UK_CPI"); uk_core = g(ons, "UK_CORE")
    uk_ur = g(ons, "UK_UR"); uk_3m = g(fred, "UK_3M", "value")

    rdiff_uk = round((us_effr - uk_3m) * 100) if us_effr and uk_3m else None
    idiff_uk = round(us_pce - uk_core, 2) if us_pce and uk_core else None
    udiff_uk = round(us_ur - uk_ur, 1) if us_ur and uk_ur else None

    uk_3m_hist = g(fred, "UK_3M", "series_last6")
    rdiff_uk_3m = round((us_effr - uk_3m_hist[-3]) * 100) if uk_3m_hist and len(uk_3m_hist) >= 4 and us_effr else None
    rdiff_uk_t = diff_trend(rdiff_uk, rdiff_uk_3m)

    uk_core_hist = g(ons, "UK_CORE", "series_vals")
    idiff_uk_3m = round(us_pce - uk_core_hist[-4], 2) if uk_core_hist and len(uk_core_hist) >= 4 and us_pce else None
    idiff_uk_t = diff_trend(int(idiff_uk * 100) if idiff_uk else None, int(idiff_uk_3m * 100) if idiff_uk_3m else None)

    econs["UK"] = {
        "label": "United Kingdom (BOE)", "pair": "GBP/USD",
        "headline_cpi": uk_cpi, "core_cpi": uk_core,
        "inflation_class": infl_class(uk_cpi),
        "policy_rate": uk_3m,
        "policy_rate_note": "3M interbank proxy (BOE bank rate ~4.5%, 3M reflects market)",
        "unemployment": uk_ur, "gdp_yoy": None,
        "rate_diff_bp": rdiff_uk, "inflation_diff_pp": idiff_uk, "ur_diff_pp": udiff_uk,
        "rate_diff_trend": rdiff_uk_t, "inflation_diff_trend": idiff_uk_t,
        "cpi_date": g(ons, "UK_CPI", "date"), "core_date": g(ons, "UK_CORE", "date"),
        "ur_date": g(ons, "UK_UR", "date"),
    }

    return us, econs


# ============================================================
# CONSOLE OUTPUT
# ============================================================

def fmt_console(us, econs, warns):
    print("\n" + "=" * 60)
    print("  FOREIGN ECONOMY SUMMARY")
    print("=" * 60)
    print(f"\n  US Baseline: EFFR {us['effr']}%  |  Core PCE {us['core_pce']}%  |  UR {us['unemployment']}%")

    for k, e in econs.items():
        print(f"\n  {e['label']} ({e['pair']})")
        if e.get("headline_cpi") is not None:
            print(f"    CPI: {e['headline_cpi']}% [{e['inflation_class']}] ({e.get('cpi_date','')})")
        if e.get("core_cpi") is not None:
            print(f"    Core: {e['core_cpi']}% ({e.get('core_date','')})")
        if e.get("unemployment") is not None:
            print(f"    Unemployment: {e['unemployment']}% ({e.get('ur_date','')})")
        if e.get("policy_rate") is not None:
            print(f"    Rate (proxy): {e['policy_rate']:.2f}%")
        if e.get("gdp_yoy") is not None:
            print(f"    GDP YoY: {e['gdp_yoy']}%")
        print(f"    --- Differentials vs US ---")
        if e.get("rate_diff_bp") is not None:
            d = "US higher" if e["rate_diff_bp"] > 0 else "foreign higher"
            t = e.get("rate_diff_trend", "")
            print(f"    Rate: {e['rate_diff_bp']}bp ({d}) [{t}]")
        if e.get("inflation_diff_pp") is not None:
            d = "US hotter" if e["inflation_diff_pp"] > 0 else "foreign hotter"
            t = e.get("inflation_diff_trend", "")
            print(f"    Inflation: {e['inflation_diff_pp']}pp ({d}) [{t}]")
        if e.get("ur_diff_pp") is not None:
            d = "US higher UR" if e["ur_diff_pp"] > 0 else "foreign higher UR"
            print(f"    Unemployment: {e['ur_diff_pp']}pp ({d})")

    if warns:
        print(f"\n  WARNINGS ({len(warns)}):")
        for w in warns: print(f"    [!] {w}")
    print("=" * 60)


# ============================================================
# LLM CONTEXT BLOCK
# ============================================================

def fmt_llm(us, econs, warns):
    L = []
    L.append("=" * 60)
    L.append("  UD FRAMEWORK -- FOREIGN ECONOMY CONTEXT")
    L.append(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    L.append("  Sources: FRED | ECB SDW | Eurostat | ONS | Japan e-Stat")
    L.append("=" * 60)
    L.append(f"\n  US BASELINE")
    L.append(f"    EFFR: {us['effr']}%  |  Core PCE: {us['core_pce']}%  |  UR: {us['unemployment']}%")

    for k, e in econs.items():
        L.append("")
        L.append("-" * 60)
        L.append(f"  {e['label']}  |  {e['pair']}")
        L.append("-" * 60)
        if e.get("headline_cpi") is not None:
            L.append(f"    Headline CPI: {e['headline_cpi']}% YoY  [{e['inflation_class']}]  ({e.get('cpi_date','')})")
        if e.get("core_cpi") is not None:
            L.append(f"    Core CPI: {e['core_cpi']}% YoY  ({e.get('core_date','')})")
        if e.get("unemployment") is not None:
            L.append(f"    Unemployment: {e['unemployment']}%  ({e.get('ur_date','')})")
        if e.get("policy_rate") is not None:
            note = f"  [{e['policy_rate_note']}]" if e.get("policy_rate_note") else ""
            L.append(f"    Policy Rate: {e['policy_rate']:.2f}%{note}")
        if e.get("gdp_yoy") is not None:
            L.append(f"    GDP YoY: {e['gdp_yoy']}%")

        L.append("    --- Differentials vs US ---")
        if e.get("rate_diff_bp") is not None:
            d = "US higher" if e["rate_diff_bp"] > 0 else "foreign higher"
            t = e.get("rate_diff_trend", "N/A")
            L.append(f"    Rate: {e['rate_diff_bp']}bp ({d}) -- 3m trend: {t}")
        if e.get("inflation_diff_pp") is not None:
            d = "US hotter" if e["inflation_diff_pp"] > 0 else "foreign hotter"
            t = e.get("inflation_diff_trend", "N/A")
            L.append(f"    Inflation: {e['inflation_diff_pp']}pp ({d}) -- 3m trend: {t}")
        if e.get("ur_diff_pp") is not None:
            d = "US higher UR" if e["ur_diff_pp"] > 0 else "foreign higher UR"
            L.append(f"    Unemployment: {e['ur_diff_pp']}pp ({d})")

    L.append("")
    L.append("=" * 60)
    L.append("  LLM USAGE GUIDE")
    L.append("=" * 60)
    L.append("  1. Rate diff = carry direction. + = USD higher-yielding.")
    L.append("     WIDENING = USD carry strengthening. COMPRESSING = convergence risk.")
    L.append("  2. Inflation diff -> relative policy paths:")
    L.append("     US sticky + foreign falling -> foreign CB cuts faster -> USD bid")
    L.append("     US falling + foreign sticky -> Fed cuts faster -> USD offered")
    L.append("  3. Differential trends show 3-month direction of the GAP itself:")
    L.append("     WIDENING = divergence increasing (supports current carry direction)")
    L.append("     COMPRESSING = convergence (carry unwind risk)")
    L.append("     STABLE = gap unchanged (status quo)")
    L.append("  4. 3M interbank = market proxy with term premium (10-40bp over official rate).")
    L.append("  5. Japan 'core' = ex fresh food + energy (BOJ preferred measure).")
    L.append("     UK 'core' = ex energy, food, alcohol, tobacco (ONS definition).")
    L.append("=" * 60)

    if warns:
        L.append(f"\n  WARNINGS ({len(warns)}):")
        for w in warns: L.append(f"    [!] {w}")

    return "\n".join(L)


# ============================================================
# MAIN
# ============================================================

def run():
    print("=" * 60)
    print("  UD Framework -- Foreign Economy Module v2.2")
    print("  Sources: FRED | ECB SDW | Eurostat | ONS | e-Stat")
    print("=" * 60)

    fred = fetch_fred_all()
    ecb = fetch_ecb()
    eurostat = fetch_eurostat()
    ons = fetch_ons()
    estat = fetch_estat()

    warns = []
    for src in [fred, ecb, eurostat, ons, estat]:
        for k, v in src.items():
            if v and v.get("stale"):
                warns.append(f"{k}: STALE ({v['age']}d, last: {v['date']})")

    us, econs = build_summaries(fred, ecb, eurostat, ons, estat)
    fmt_console(us, econs, warns)
    llm = fmt_llm(us, econs, warns)

    ts = datetime.now().strftime("%Y%m%d_%H%M")
    txt = f"ud_foreign_{ts}.txt"
    jsn = f"ud_foreign_{ts}.json"

    with open(txt, "w", encoding="utf-8") as f:
        f.write(llm)

    jout = {
        "metadata": {"module": "foreign_economy", "version": "2.2",
                     "generated": datetime.now().isoformat(),
                     "sources": ["FRED", "ECB SDW", "Eurostat", "ONS", "Japan e-Stat"]},
        "us_baseline": us,
        "economies": econs,
        "warnings": warns,
    }
    with open(jsn, "w", encoding="utf-8") as f:
        json.dump(jout, f, indent=2, default=str)

    print(f"\n  Saved: {txt}")
    print(f"  Saved: {jsn}")
    print("=" * 60)


if __name__ == "__main__":
    run()
