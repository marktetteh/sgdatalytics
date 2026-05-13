"""
SG Datalytics — Ghana Statistical Service (GSS) Data Scraper
=============================================================
Source: GSS StatsBank PX-Web API (official, free, no restrictions)
URL:    https://statsbank.statsghana.gov.gh/api/v1/en/

Run:  python gss_scraper.py
Output: CSV files in ./output/gss/ folder, ready for SG Datalytics.
"""

import requests
import json
import pandas as pd
from io import StringIO
from datetime import datetime
import os
import time

OUTPUT_DIR = "output/gss"
os.makedirs(OUTPUT_DIR, exist_ok=True)

TODAY = datetime.today().strftime("%Y-%m-%d")
BASE = "https://statsbank.statsghana.gov.gh/api/v1/en/Macroeconomic%20Indicators"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://statsbank.statsghana.gov.gh/",
    "Content-Type": "application/json",
}


# ─────────────────────────────────────────────────────────────
# CORE FETCH FUNCTION
# ─────────────────────────────────────────────────────────────

def fetch_dataset(category_path, table_file, dataset_name):
    """Fetch a full dataset from GSS StatsBank PX-Web API."""
    url = f"{BASE}/{category_path}/{table_file}"

    # Step 1: Get metadata
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            print(f"    ✗ Metadata error {r.status_code} for {dataset_name}")
            return None
        meta = r.json()
    except Exception as e:
        print(f"    ✗ Failed to get metadata: {e}")
        return None

    # Step 2: Build query — empty query selects all values (smaller POST body,
    # avoids 403 when variable value lists are very long)
    query = {"query": [], "response": {"format": "csv"}}

    # Step 3: POST to fetch data
    try:
        r2 = requests.post(url, headers=HEADERS, data=json.dumps(query), timeout=30)
        if r2.status_code != 200:
            print(f"    ✗ Data fetch error {r2.status_code} for {dataset_name}")
            return None
        df = pd.read_csv(StringIO(r2.text))
    except Exception as e:
        print(f"    ✗ Failed to parse data: {e}")
        return None

    # Step 4: Add metadata columns
    df.insert(0, "Dataset", dataset_name)
    df.insert(1, "Source", "Ghana Statistical Service (GSS StatsBank)")
    df.insert(2, "Scraped Date", TODAY)

    return df


def save(df, filename, dataset_name):
    """Save dataframe to CSV."""
    filepath = f"{OUTPUT_DIR}/{filename}"
    df.to_csv(filepath, index=False)
    print(f"    ✓ {len(df):>4} rows × {len(df.columns)} cols → {filepath}")
    return filepath


# ─────────────────────────────────────────────────────────────
# DATASETS TO SCRAPE
# ─────────────────────────────────────────────────────────────

DATASETS = [
    # Prices & Inflation
    {
        "name": "Ghana CPI & Inflation",
        "category": "Prices%20and%20Inflation",
        "table": "cpi.px",
        "file": f"gss_cpi_inflation_{TODAY}.csv",
    },
    {
        "name": "Ghana Producer Price Index (PPI)",
        "category": "Prices%20and%20Inflation",
        "table": "ppi.px",
        "file": f"gss_ppi_{TODAY}.csv",
    },
    {
        "name": "Ghana Commodity Prices",
        "category": "Prices%20and%20Inflation",
        "table": "commodity_price.px",
        "file": f"gss_commodity_prices_{TODAY}.csv",
    },
    {
        "name": "Ghana Index of Industrial Production (IIP)",
        "category": "Prices%20and%20Inflation",
        "table": "iip.px",
        "file": f"gss_industrial_production_{TODAY}.csv",
    },
    {
        "name": "Ghana Export & Import Price Indices (XMPI)",
        "category": "Prices%20and%20Inflation",
        "table": "macro_xmpi.px",
        "file": f"gss_xmpi_{TODAY}.csv",
    },
    # External Sector
    {
        "name": "Ghana Exchange Rates",
        "category": "External%20Sector",
        "table": "exchange_rates.px",
        "file": f"gss_exchange_rates_{TODAY}.csv",
    },
    {
        "name": "Ghana International Finance",
        "category": "External%20Sector",
        "table": "int_fin.px",
        "file": f"gss_international_finance_{TODAY}.csv",
    },
    {
        "name": "Ghana International Merchandise Trade",
        "category": "External%20Sector",
        "table": "macro_trade.px",
        "file": f"gss_merchandise_trade_{TODAY}.csv",
    },
    # Fiscal Sector
    {
        "name": "Ghana Government Debt Data",
        "category": "Fiscal%20Sector",
        "table": "debt_data.px",
        "file": f"gss_debt_{TODAY}.csv",
    },
    {
        "name": "Ghana Government Fiscal Data",
        "category": "Fiscal%20Sector",
        "table": "fiscal_data.px",
        "file": f"gss_fiscal_{TODAY}.csv",
    },
    # Monetary & Financial
    {
        "name": "Ghana Financial Soundness Indicators",
        "category": "Monetary%20and%20Financial%20Sector",
        "table": "fin_sound.px",
        "file": f"gss_financial_soundness_{TODAY}.csv",
    },
    {
        "name": "Ghana Interest Rates",
        "category": "Monetary%20and%20Financial%20Sector",
        "table": "interest.px",
        "file": f"gss_interest_rates_{TODAY}.csv",
    },
    {
        "name": "Ghana Monetary Data",
        "category": "Monetary%20and%20Financial%20Sector",
        "table": "monetary.px",
        "file": f"gss_monetary_{TODAY}.csv",
    },
]


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 62)
    print("  SG DATALYTICS — Ghana Statistical Service (GSS) Scraper")
    print(f"  Source: GSS StatsBank PX-Web API (official, free)")
    print(f"  Date:   {datetime.today().strftime('%d %B %Y')}")
    print("=" * 62)

    results = {}

    for ds in DATASETS:
        print(f"\n📊 {ds['name']}")
        df = fetch_dataset(ds["category"], ds["table"], ds["name"])
        if df is not None and not df.empty:
            save(df, ds["file"], ds["name"])
            results[ds["name"]] = df
        else:
            print(f"    ✗ Skipped — no data returned")
            results[ds["name"]] = None
        time.sleep(1)  # Polite delay

    # Summary
    print("\n" + "=" * 62)
    print("  ✅ SUMMARY")
    print("=" * 62)
    total_rows = 0
    success = 0
    for name, df in results.items():
        if df is not None and not df.empty:
            print(f"  ✓ {name:<45} {len(df):>5} rows")
            total_rows += len(df)
            success += 1
        else:
            print(f"  ✗ {name:<45} Failed")

    print(f"\n  Datasets collected:  {success}/{len(DATASETS)}")
    print(f"  Total records:       {total_rows:,}")
    print(f"  Output folder:       ./{OUTPUT_DIR}/")
    print("=" * 62)

    # Preview exchange rates
    fx = results.get("Ghana Exchange Rates")
    if fx is not None:
        print("\n  📊 Exchange Rates Preview (latest 5 months):")
        cols = [c for c in fx.columns if c not in ["Dataset", "Source", "Scraped Date"]][:5]
        print(fx[cols].head(5).to_string(index=False))
