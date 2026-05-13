"""
SG Datalytics — Ghana Economic Data Scraper
Sources: World Bank Open Data API (official, free, no restrictions)
Datasets: Macroeconomic, Trade, Social, Financial indicators for Ghana

Run:  python ghana_data_scraper.py
Output: CSV files in ./output/ folder
"""

import requests
import pandas as pd
from datetime import datetime
import os
import time

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

WORLD_BANK_BASE = "https://api.worldbank.org/v2"
TODAY = datetime.today().strftime("%Y-%m-%d")


# ─────────────────────────────────────────────────────────────────
# INDICATOR DEFINITIONS
# ─────────────────────────────────────────────────────────────────
INDICATOR_GROUPS = {

    "Ghana Macroeconomic Indicators": {
        "filename": f"ghana_macroeconomic_{TODAY}.csv",
        "description": (
            "Key macroeconomic indicators for Ghana including GDP, inflation, "
            "exchange rate, and fiscal data. Source: World Bank Open Data."
        ),
        "indicators": {
            "NY.GDP.MKTP.CD":    "GDP (Current USD)",
            "NY.GDP.MKTP.KD.ZG": "GDP Growth Rate (%)",
            "NY.GDP.PCAP.CD":    "GDP Per Capita (Current USD)",
            "NY.GDP.PCAP.KD.ZG": "GDP Per Capita Growth (%)",
            "FP.CPI.TOTL.ZG":   "Inflation Rate - CPI (%)",
            "PA.NUS.FCRF":       "Official Exchange Rate (LCU per USD)",
            "GC.DOD.TOTL.GD.ZS": "Government Debt (% of GDP)",
            "GC.REV.XGRT.GD.ZS": "Government Revenue (% of GDP)",
            "GC.XPN.TOTL.GD.ZS": "Government Expenditure (% of GDP)",
            "NY.GNS.ICTR.ZS":    "Gross Savings (% of GDP)",
        }
    },

    "Ghana Trade & Investment": {
        "filename": f"ghana_trade_investment_{TODAY}.csv",
        "description": (
            "Ghana trade balance, exports, imports, and foreign direct investment data. "
            "Source: World Bank Open Data."
        ),
        "indicators": {
            "NE.EXP.GNFS.ZS":    "Exports of Goods & Services (% of GDP)",
            "NE.IMP.GNFS.ZS":    "Imports of Goods & Services (% of GDP)",
            "NE.EXP.GNFS.CD":    "Exports of Goods & Services (USD)",
            "NE.IMP.GNFS.CD":    "Imports of Goods & Services (USD)",
            "BX.KLT.DINV.WD.GD.ZS": "Foreign Direct Investment, Net Inflows (% of GDP)",
            "BX.KLT.DINV.CD.WD": "Foreign Direct Investment, Net Inflows (USD)",
            "BM.KLT.DINV.GD.ZS": "Foreign Direct Investment, Net Outflows (% of GDP)",
            "TG.VAL.TOTL.GD.ZS": "Merchandise Trade (% of GDP)",
            "TX.VAL.MRCH.CD.WT":  "Merchandise Exports (USD)",
            "TM.VAL.MRCH.CD.WT":  "Merchandise Imports (USD)",
        }
    },

    "Ghana Social & Demographic Indicators": {
        "filename": f"ghana_social_demographic_{TODAY}.csv",
        "description": (
            "Ghana population, employment, poverty, health, and education statistics. "
            "Source: World Bank Open Data."
        ),
        "indicators": {
            "SP.POP.TOTL":        "Total Population",
            "SP.POP.GROW":        "Population Growth Rate (%)",
            "SP.URB.TOTL.IN.ZS":  "Urban Population (% of Total)",
            "SL.UEM.TOTL.ZS":    "Unemployment Rate (% of Total Labor Force)",
            "SL.UEM.1524.ZS":    "Youth Unemployment Rate (% ages 15–24)",
            "SI.POV.NAHC":       "Poverty Headcount Ratio at National Lines (%)",
            "SP.DYN.LE00.IN":    "Life Expectancy at Birth (years)",
            "SH.DYN.MORT":       "Under-5 Mortality Rate (per 1,000 live births)",
            "SE.ADT.LITR.ZS":    "Adult Literacy Rate (%)",
            "SE.PRM.ENRR":       "Primary School Enrollment Rate (gross %)",
        }
    },

    "Ghana Financial Sector": {
        "filename": f"ghana_financial_sector_{TODAY}.csv",
        "description": (
            "Ghana banking sector, interest rates, credit, and financial inclusion data. "
            "Source: World Bank Open Data."
        ),
        "indicators": {
            "FR.INR.LEND":       "Lending Interest Rate (%)",
            "FR.INR.DPST":       "Deposit Interest Rate (%)",
            "FS.AST.DOMS.GD.ZS": "Domestic Credit to Private Sector (% of GDP)",
            "FD.AST.PRVT.GD.ZS": "Domestic Credit to Private Sector by Banks (% of GDP)",
            "FB.ATM.TOTL.P5":    "ATMs (per 100,000 adults)",
            "FB.CBK.BRCH.P5":    "Commercial Bank Branches (per 100,000 adults)",
            "FX.OWN.TOTL.ZS":   "Account Ownership at Financial Institution (%)",
            "FB.BNK.CAPA.ZS":    "Bank Capital to Assets Ratio (%)",
            "FS.AST.NONP.ZS":    "Bank Nonperforming Loans (% of Total Loans)",
        }
    },

    "Ghana Agriculture & Food Security": {
        "filename": f"ghana_agriculture_{TODAY}.csv",
        "description": (
            "Ghana agricultural production, land use, food security, and rural economy data. "
            "Source: World Bank Open Data."
        ),
        "indicators": {
            "NV.AGR.TOTL.ZS":    "Agriculture, Forestry & Fishing (% of GDP)",
            "NV.AGR.TOTL.KD.ZG": "Agriculture Value Added Growth (%)",
            "AG.LND.ARBL.ZS":    "Arable Land (% of Land Area)",
            "AG.LND.AGRI.ZS":    "Agricultural Land (% of Land Area)",
            "AG.YLD.CREL.KG":    "Cereal Yield (kg per hectare)",
            "SN.ITK.DEFC.ZS":    "Prevalence of Undernourishment (%)",
            "AG.PRD.FOOD.XD":    "Food Production Index",
            "SL.AGR.EMPL.ZS":    "Employment in Agriculture (% of total)",
        }
    },

    "Ghana Energy & Infrastructure": {
        "filename": f"ghana_energy_infrastructure_{TODAY}.csv",
        "description": (
            "Ghana energy access, electricity, road, internet, and infrastructure data. "
            "Source: World Bank Open Data."
        ),
        "indicators": {
            "EG.ELC.ACCS.ZS":   "Access to Electricity (% of Population)",
            "EG.ELC.ACCS.RU.ZS":"Access to Electricity, Rural (% of Rural Population)",
            "EG.ELC.ACCS.UR.ZS":"Access to Electricity, Urban (% of Urban Population)",
            "EG.USE.PCAP.KG.OE":"Energy Use (kg of oil equivalent per capita)",
            "IT.NET.USER.ZS":   "Internet Users (% of Population)",
            "IT.CEL.SETS.P2":   "Mobile Cellular Subscriptions (per 100 people)",
            "IS.ROD.PAVE.ZS":   "Roads, Paved (% of Total Roads)",
            "EG.FEC.RNEW.ZS":   "Renewable Energy Consumption (% of Total)",
        }
    },
}


# ─────────────────────────────────────────────────────────────────
# FETCH FUNCTIONS
# ─────────────────────────────────────────────────────────────────

def fetch_indicator(indicator_code, years=25):
    """Fetch a single indicator for Ghana from World Bank API."""
    url = (
        f"{WORLD_BANK_BASE}/country/GH/indicator/{indicator_code}"
        f"?format=json&mrv={years}&per_page=100"
    )
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        if len(data) < 2 or not data[1]:
            return {}
        return {entry["date"]: entry["value"] for entry in data[1] if entry["value"] is not None}
    except Exception as e:
        print(f"      ⚠ Error fetching {indicator_code}: {e}")
        return {}


def build_dataset(group_name, config):
    """Build a wide-format DataFrame for a group of indicators."""
    print(f"\n📊 Building: {group_name}")

    # Collect data for each indicator
    all_data = {}
    for code, label in config["indicators"].items():
        print(f"   ↓ {label}")
        values = fetch_indicator(code)
        all_data[label] = values
        time.sleep(0.3)  # Polite delay

    # Find all years across all indicators
    all_years = sorted(
        set(year for values in all_data.values() for year in values.keys()),
        reverse=True
    )

    # Build rows
    rows = []
    for year in all_years:
        row = {
            "Year": year,
            "Country": "Ghana",
            "Country Code": "GH",
            "Source": "World Bank Open Data",
            "Scraped Date": TODAY,
        }
        for label, values in all_data.items():
            row[label] = values.get(year, None)
        rows.append(row)

    df = pd.DataFrame(rows)

    # Save CSV
    filepath = f"{OUTPUT_DIR}/{config['filename']}"
    df.to_csv(filepath, index=False)

    # Save metadata sidecar
    meta_path = filepath.replace(".csv", "_metadata.txt")
    with open(meta_path, "w") as f:
        f.write(f"Dataset: {group_name}\n")
        f.write(f"Description: {config['description']}\n")
        f.write(f"Source: World Bank Open Data API\n")
        f.write(f"Country: Ghana (GH)\n")
        f.write(f"Scraped: {TODAY}\n")
        f.write(f"Years covered: {all_years[-1]} – {all_years[0]}\n")
        f.write(f"Rows: {len(df)}\n")
        f.write(f"Columns: {len(df.columns)}\n")
        f.write(f"\nIndicators:\n")
        for code, label in config["indicators"].items():
            f.write(f"  {code}: {label}\n")

    print(f"   ✓ {len(df)} rows × {len(df.columns)} columns → {filepath}")
    return df


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  SG DATALYTICS — Ghana Economic Data Scraper")
    print(f"  Source: World Bank Open Data API (official, free)")
    print(f"  Date:   {datetime.today().strftime('%d %B %Y')}")
    print("=" * 60)

    results = {}
    for group_name, config in INDICATOR_GROUPS.items():
        df = build_dataset(group_name, config)
        results[group_name] = df
        time.sleep(1)

    # Final summary
    print("\n" + "=" * 60)
    print("  ✅ ALL DATASETS COMPLETE")
    print("=" * 60)
    total_rows = 0
    for name, df in results.items():
        rows = len(df) if df is not None else 0
        cols = len(df.columns) if df is not None else 0
        total_rows += rows
        print(f"  ✓ {name}")
        print(f"    → {rows} rows, {cols} columns")

    print(f"\n  Total records collected: {total_rows}")
    print(f"  Files saved to: ./{OUTPUT_DIR}/")
    print("=" * 60)

    # Preview macro data
    macro = results.get("Ghana Macroeconomic Indicators")
    if macro is not None:
        print("\n  📈 Macroeconomic Preview (last 5 years):")
        preview_cols = ["Year", "GDP Growth Rate (%)", "Inflation Rate - CPI (%)",
                        "Official Exchange Rate (LCU per USD)", "Unemployment Rate (% of Total Labor Force)"]
        available = [c for c in preview_cols if c in macro.columns]
        print(macro[available].head(5).to_string(index=False))
