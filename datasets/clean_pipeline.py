"""
SG Datalytics — Data Cleaning Pipeline
=======================================
Cleans and standardises all datasets:
  - Jiji Ghana market listings
  - Tonaton Ghana market listings
  - GSS StatsBank economic data
  - Bank of Ghana bulletin data
  - World Bank Open Data

Outputs:
  - Clean CSVs in datasets/clean/{source}/
  - Data dictionary per dataset in datasets/clean/dictionaries/
  - Master catalogue: datasets/catalogue.csv

Run: python clean_pipeline.py
"""

import pandas as pd
import numpy as np
import os
import re
from datetime import datetime

# ─────────────────────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────────────────────
BASE     = "/sessions/friendly-trusting-hopper/mnt/sgdatalytics/datasets"
RAW      = BASE
CLEAN    = os.path.join(BASE, "clean")
DICT_DIR = os.path.join(CLEAN, "dictionaries")
TODAY    = datetime.today().strftime("%Y-%m-%d")
MONTH    = datetime.today().strftime("%Y-%m")

for d in [CLEAN,
          os.path.join(CLEAN, "jiji"),
          os.path.join(CLEAN, "tonaton"),
          os.path.join(CLEAN, "gss"),
          os.path.join(CLEAN, "bog"),
          os.path.join(CLEAN, "worldbank"),
          DICT_DIR]:
    os.makedirs(d, exist_ok=True)

catalogue = []   # rows for master catalogue


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def to_snake(col):
    """Convert a column name to snake_case."""
    col = col.strip()
    col = re.sub(r"[/&,()\-%]", " ", col)
    col = re.sub(r"\s+", "_", col)
    col = re.sub(r"__+", "_", col)
    col = col.strip("_").lower()
    return col


def save_clean(df, subfolder, filename, dataset_name, source, frequency,
               description, date_col=None):
    """Save a cleaned dataframe and register in catalogue."""
    path = os.path.join(CLEAN, subfolder, filename)
    df.to_csv(path, index=False)

    # Date range
    date_min = date_max = ""
    if date_col and date_col in df.columns:
        vals = df[date_col].dropna().astype(str)
        date_min = vals.min()
        date_max = vals.max()

    catalogue.append({
        "dataset_name":  dataset_name,
        "source":        source,
        "category":      subfolder,
        "frequency":     frequency,
        "date_start":    date_min,
        "date_end":      date_max,
        "rows":          len(df),
        "columns":       len(df.columns),
        "file":          f"clean/{subfolder}/{filename}",
        "description":   description,
        "last_updated":  TODAY,
    })
    print(f"   ✓ {dataset_name}: {len(df):,} rows → clean/{subfolder}/{filename}")
    return path


def make_dict(df, subfolder, stem, col_meta):
    """
    Generate a data dictionary CSV.
    col_meta: dict of {col_name: (display_name, description, unit)}
    """
    rows = []
    for col in df.columns:
        meta = col_meta.get(col, (col.replace("_", " ").title(), "", ""))
        display, desc, unit = meta
        example = df[col].dropna().iloc[0] if df[col].notna().any() else ""
        rows.append({
            "column_name":   col,
            "display_name":  display,
            "description":   desc,
            "data_type":     str(df[col].dtype),
            "unit":          unit,
            "example_value": str(example)[:80],
            "null_count":    int(df[col].isnull().sum()),
            "null_pct":      f"{df[col].isnull().mean()*100:.1f}%",
        })
    dict_df = pd.DataFrame(rows)
    path = os.path.join(DICT_DIR, f"{stem}_dictionary.csv")
    dict_df.to_csv(path, index=False)
    print(f"     📖 Dictionary → {stem}_dictionary.csv")


def remove_price_outliers(df, price_col="price_ghs", min_price=1.0):
    """
    Remove listings with price < min_price.
    Flag (but keep) statistical outliers using IQR per category.
    """
    before = len(df)
    df = df[df[price_col] >= min_price].copy()

    # Flag outliers per category using IQR
    df["price_outlier_flag"] = False
    for cat in df["category"].unique():
        mask = df["category"] == cat
        q1 = df.loc[mask, price_col].quantile(0.25)
        q3 = df.loc[mask, price_col].quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 3 * iqr
        upper = q3 + 3 * iqr
        outlier = mask & ((df[price_col] < lower) | (df[price_col] > upper))
        df.loc[outlier, "price_outlier_flag"] = True

    flagged = df["price_outlier_flag"].sum()
    removed = before - len(df)
    if removed > 0:
        print(f"     Removed {removed} rows with price < GH₵{min_price:.0f}")
    if flagged > 0:
        print(f"     Flagged {flagged} statistical outliers (kept, marked in price_outlier_flag)")
    return df


def standardise_condition(val):
    """Normalise condition strings to a standard set."""
    if pd.isna(val):
        return None
    v = str(val).lower().strip()
    if "foreign used" in v:
        return "Foreign Used"
    if "brand new" in v or v == "new":
        return "Brand New"
    if "refurb" in v:
        return "Refurbished"
    if "used" in v:
        return "Used"
    return val.strip()


def parse_gss_date(val):
    """Convert GSS date formats to ISO standard.
    2024M08 → 2024-08   |   2024Q2 → 2024-Q2   |   2023 → 2023
    """
    val = str(val).strip()
    m = re.match(r"^(\d{4})M(\d{2})$", val)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    q = re.match(r"^(\d{4})Q(\d)$", val)
    if q:
        return f"{q.group(1)}-Q{q.group(2)}"
    return val  # annual or already clean


# ─────────────────────────────────────────────────────────────
# 1. JIJI DATASETS
# ─────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("  CLEANING JIJI DATASETS")
print("="*60)

jiji_col_rename = {
    "Category":     "category",
    "Title":        "title",
    "Price GHS":    "price_ghs",
    "Condition":    "condition",
    "Region":       "region",
    "Area":         "area",
    "Location":     "location",
    "Seller Badge": "seller_badge",
    "Description":  "description",
    "URL":          "url",
    "Source":       "source",
    "Scraped Date": "scraped_date",
}

jiji_col_meta = {
    "category":            ("Category",     "Product category on Jiji Ghana",                            ""),
    "title":               ("Title",        "Listing title as entered by seller",                        ""),
    "price_ghs":           ("Price (GHS)",  "Asking price in Ghana Cedis",                               "GHS"),
    "condition":           ("Condition",    "Item condition: Brand New, Used, Foreign Used, Refurbished", ""),
    "region":              ("Region",       "Ghana region (e.g. Greater Accra)",                         ""),
    "area":                ("Area",         "Specific area within the region",                           ""),
    "location":            ("Location",     "Full location string (region + area)",                      ""),
    "seller_badge":        ("Seller Badge", "Seller trust level on Jiji (e.g. Enterprise, Diamond)",     ""),
    "description":         ("Description",  "Short listing description snippet",                         ""),
    "url":                 ("URL",          "Direct link to listing on jiji.com.gh",                     ""),
    "price_outlier_flag":  ("Outlier Flag", "True if price is a statistical outlier (IQR × 3) for its category", ""),
    "source":              ("Source",       "Data source",                                               ""),
    "scraped_date":        ("Scraped Date", "Date this record was collected",                             "YYYY-MM-DD"),
}

jiji_files = {
    "Mobile Phones & Tablets":    ("jiji_mobile_phones_2026-04-20.csv",        "jiji_mobile_phones"),
    "Vehicles":                   ("jiji_vehicles_2026-04-20.csv",             "jiji_vehicles"),
    "Electronics":                ("jiji_electronics_2026-04-20.csv",          "jiji_electronics"),
    "Home, Furniture & Appliances":("jiji_home_furniture_2026-04-20.csv",      "jiji_home_furniture"),
    "Food, Agriculture & Farming":("jiji_food_agriculture_2026-04-20.csv",     "jiji_food_agriculture"),
    "Property / Real Estate":     ("jiji_real_estate_2026-04-20.csv",          "jiji_real_estate"),
    "Fashion":                    ("jiji_fashion_2026-04-20.csv",              "jiji_fashion"),
    "Commercial Equipment & Tools":("jiji_commercial_equipment_2026-04-20.csv","jiji_commercial_equipment"),
}

jiji_desc = {
    "Mobile Phones & Tablets":     "Jiji Ghana mobile phone and tablet listings with asking prices, conditions, and seller locations.",
    "Vehicles":                    "Jiji Ghana vehicle listings (cars, motorbikes, trucks) with prices and conditions.",
    "Electronics":                 "Jiji Ghana electronics listings including TVs, laptops, audio equipment.",
    "Home, Furniture & Appliances":"Jiji Ghana home goods, furniture and appliance listings with prices.",
    "Food, Agriculture & Farming": "Jiji Ghana food, farm produce and agricultural input listings with market prices.",
    "Property / Real Estate":      "Jiji Ghana property listings for sale and rent with asking prices by location.",
    "Fashion":                     "Jiji Ghana fashion, clothing and accessories listings with prices.",
    "Commercial Equipment & Tools":"Jiji Ghana commercial equipment and tools listings with prices.",
}

for cat_name, (raw_file, stem) in jiji_files.items():
    raw_path = os.path.join(RAW, "jiji", raw_file)
    if not os.path.exists(raw_path):
        print(f"   ✗ Missing: {raw_file}")
        continue

    print(f"\n  📱 {cat_name}")
    df = pd.read_csv(raw_path)

    # Rename columns
    df = df.rename(columns=jiji_col_rename)
    # Drop redundant raw price column
    df = df.drop(columns=["Price Raw"], errors="ignore")

    # Deduplicate by URL
    before = len(df)
    df = df.drop_duplicates(subset=["url"])
    if len(df) < before:
        print(f"     Removed {before - len(df)} duplicate listings")

    # Standardise condition
    df["condition"] = df["condition"].apply(standardise_condition)

    # Clean seller badge — strip rating numbers, keep badge label
    df["seller_badge"] = df["seller_badge"].apply(
        lambda x: re.sub(r"^\d+\.\d+,\s*", "", str(x)).strip() if pd.notna(x) else None
    )

    # Price cleaning & outlier flagging
    df["price_ghs"] = pd.to_numeric(df["price_ghs"], errors="coerce")
    df = remove_price_outliers(df, price_col="price_ghs")

    # Ensure clean column order
    cols = ["category","title","price_ghs","condition","region","area","location",
            "seller_badge","description","price_outlier_flag","url","source","scraped_date"]
    df = df[[c for c in cols if c in df.columns]]

    out_file = f"{stem}_{MONTH}.csv"
    save_clean(df, "jiji", out_file, f"Jiji Ghana – {cat_name}", "Jiji Ghana (jiji.com.gh)",
               "Weekly", jiji_desc[cat_name], date_col="scraped_date")
    make_dict(df, "jiji", stem, jiji_col_meta)


# ─────────────────────────────────────────────────────────────
# 2. TONATON DATASETS
# ─────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("  CLEANING TONATON DATASETS")
print("="*60)

tonaton_col_rename = {
    "Category":     "category",
    "Title":        "title",
    "Price GHS":    "price_ghs",
    "Period":       "rental_period",
    "Condition":    "condition",
    "Tags":         "tags",
    "Region":       "region",
    "Area":         "area",
    "Location":     "location",
    "URL":          "url",
    "Source":       "source",
    "Scraped Date": "scraped_date",
}

tonaton_col_meta = {
    "category":           ("Category",      "Product category on Tonaton Ghana",                          ""),
    "title":              ("Title",         "Listing title as entered by seller",                         ""),
    "price_ghs":          ("Price (GHS)",   "Asking price in Ghana Cedis",                                "GHS"),
    "rental_period":      ("Rental Period", "Rental period if applicable (e.g. Per Month)",               ""),
    "condition":          ("Condition",     "Item condition: Brand New, Used, Foreign Used, Refurbished",  ""),
    "tags":               ("Tags",          "All attribute tags from listing (condition, type, brand etc.)",""),
    "region":             ("Region",        "Ghana region (e.g. Greater Accra)",                          ""),
    "area":               ("Area",          "Specific area within the region",                            ""),
    "location":           ("Location",      "Full location string (region + area)",                       ""),
    "price_outlier_flag": ("Outlier Flag",  "True if price is a statistical outlier (IQR × 3) for its category",""),
    "url":                ("URL",           "Direct link to listing on tonaton.com",                      ""),
    "source":             ("Source",        "Data source",                                                ""),
    "scraped_date":       ("Scraped Date",  "Date this record was collected",                             "YYYY-MM-DD"),
}

tonaton_files = {
    "Mobile Phones & Tablets":     ("tonaton_mobile_phones_2026-04-20.csv",        "tonaton_mobile_phones"),
    "Vehicles":                    ("tonaton_vehicles_2026-04-20.csv",             "tonaton_vehicles"),
    "Electronics":                 ("tonaton_electronics_2026-04-20.csv",          "tonaton_electronics"),
    "Home, Furniture & Appliances":("tonaton_home_furniture_2026-04-20.csv",       "tonaton_home_furniture"),
    "Food, Agriculture & Farming": ("tonaton_food_agriculture_2026-04-20.csv",     "tonaton_food_agriculture"),
    "Property / Real Estate":      ("tonaton_real_estate_2026-04-20.csv",          "tonaton_real_estate"),
    "Fashion":                     ("tonaton_fashion_2026-04-20.csv",              "tonaton_fashion"),
    "Commercial Equipment & Tools":("tonaton_commercial_equipment_2026-04-20.csv", "tonaton_commercial_equipment"),
}

tonaton_desc = {
    "Mobile Phones & Tablets":     "Tonaton Ghana mobile phone and tablet listings with asking prices, conditions, and seller locations.",
    "Vehicles":                    "Tonaton Ghana vehicle listings (cars, motorbikes, trucks) with prices and conditions.",
    "Electronics":                 "Tonaton Ghana electronics listings including TVs, laptops, audio equipment.",
    "Home, Furniture & Appliances":"Tonaton Ghana home goods, furniture and appliance listings with prices.",
    "Food, Agriculture & Farming": "Tonaton Ghana food, farm produce and agricultural input listings with market prices.",
    "Property / Real Estate":      "Tonaton Ghana property listings for sale and rent with asking prices by location.",
    "Fashion":                     "Tonaton Ghana fashion, clothing and accessories listings with prices.",
    "Commercial Equipment & Tools":"Tonaton Ghana commercial equipment and tools listings with prices.",
}

for cat_name, (raw_file, stem) in tonaton_files.items():
    raw_path = os.path.join(RAW, "tonaton", raw_file)
    if not os.path.exists(raw_path):
        print(f"   ✗ Missing: {raw_file}")
        continue

    print(f"\n  🛍️  {cat_name}")
    df = pd.read_csv(raw_path)

    # Rename columns
    df = df.rename(columns=tonaton_col_rename)
    df = df.drop(columns=["Price Raw"], errors="ignore")

    # Drop rental_period if entirely empty
    if "rental_period" in df.columns and df["rental_period"].isna().all():
        df = df.drop(columns=["rental_period"])
        tonaton_col_meta.pop("rental_period", None)

    # Deduplicate
    before = len(df)
    df = df.drop_duplicates(subset=["url"])
    if len(df) < before:
        print(f"     Removed {before - len(df)} duplicate listings")

    # Standardise condition
    df["condition"] = df["condition"].apply(standardise_condition)

    # Price cleaning & outlier flagging
    df["price_ghs"] = pd.to_numeric(df["price_ghs"], errors="coerce")
    df = remove_price_outliers(df, price_col="price_ghs")

    # Column order
    cols = ["category","title","price_ghs","condition","tags","region","area","location",
            "price_outlier_flag","url","source","scraped_date"]
    df = df[[c for c in cols if c in df.columns]]

    out_file = f"{stem}_{MONTH}.csv"
    save_clean(df, "tonaton", out_file, f"Tonaton Ghana – {cat_name}", "Tonaton Ghana (tonaton.com)",
               "Weekly", tonaton_desc[cat_name], date_col="scraped_date")
    make_dict(df, "tonaton", stem, tonaton_col_meta)


# ─────────────────────────────────────────────────────────────
# 3. GSS DATASETS
# ─────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("  CLEANING GSS DATASETS")
print("="*60)

gss_files = [
    ("gss_cpi_inflation_2026-04-20.csv",        "gss_cpi_inflation",        "Ghana CPI & Inflation",                      "Monthly",  "Consumer Price Index and inflation rates by indicator, region, and product group."),
    ("gss_ppi_2026-04-20.csv",                  "gss_ppi",                  "Ghana Producer Price Index (PPI)",           "Monthly",  "Producer price indices tracking prices received by domestic producers."),
    ("gss_commodity_prices_2026-04-20.csv",      "gss_commodity_prices",     "Ghana Commodity Prices",                     "Monthly",  "Prices of key commodities traded in Ghana."),
    ("gss_industrial_production_2026-04-20.csv", "gss_industrial_production","Ghana Index of Industrial Production (IIP)", "Quarterly","Index measuring changes in industrial output in Ghana."),
    ("gss_xmpi_2026-04-20.csv",                 "gss_xmpi",                 "Ghana Export & Import Price Indices (XMPI)", "Quarterly","Price indices for Ghana's exports and imports."),
    ("gss_exchange_rates_2026-04-20.csv",        "gss_exchange_rates",       "Ghana Exchange Rates (GSS)",                 "Monthly",  "Interbank and forex bureau exchange rates for major currencies against the Ghana Cedi."),
    ("gss_international_finance_2026-04-20.csv", "gss_international_finance","Ghana International Finance",                "Mixed",    "Ghana's international financial position including reserves and external debt."),
    ("gss_merchandise_trade_2026-04-20.csv",     "gss_merchandise_trade",    "Ghana International Merchandise Trade",      "Mixed",    "Ghana's merchandise trade values by product and trading partner."),
    ("gss_debt_2026-04-20.csv",                  "gss_debt",                 "Ghana Government Debt",                      "Monthly",  "Ghana's total government debt broken down by domestic and external components."),
    ("gss_fiscal_2026-04-20.csv",                "gss_fiscal",               "Ghana Government Fiscal Data",               "Mixed",    "Ghana government revenue, expenditure, and fiscal balance data."),
    ("gss_financial_soundness_2026-04-20.csv",   "gss_financial_soundness",  "Ghana Financial Soundness Indicators",       "Monthly",  "Key financial soundness indicators for Ghana's banking sector."),
    ("gss_interest_rates_2026-04-20.csv",        "gss_interest_rates",       "Ghana Interest Rates (GSS)",                 "Monthly",  "Key interest rates in Ghana including policy rate, T-bill rates and lending rates."),
    ("gss_monetary_2026-04-20.csv",              "gss_monetary",             "Ghana Monetary Data",                        "Monthly",  "Ghana monetary aggregates including money supply (M1, M2) and credit data."),
]

for raw_file, stem, name, freq, desc in gss_files:
    raw_path = os.path.join(RAW, "gss", raw_file)
    if not os.path.exists(raw_path):
        print(f"\n   ✗ Missing: {raw_file}")
        continue

    print(f"\n  📊 {name}")
    df = pd.read_csv(raw_path)

    # Drop metadata columns that are constant / redundant
    df = df.drop(columns=["Dataset", "Source", "Scraped Date"], errors="ignore")

    # Standardise date column — find it
    date_col = None
    for c in ["Month", "Quarter", "Time_Period", "Year"]:
        if c in df.columns:
            date_col = c
            break

    if date_col:
        df[date_col] = df[date_col].apply(parse_gss_date)
        df = df.rename(columns={date_col: "period"})
        date_col = "period"

    # Rename all columns to snake_case
    df.columns = [to_snake(c) for c in df.columns]
    if date_col:
        date_col = to_snake(date_col) if date_col != "period" else "period"

    # Convert all non-period columns to numeric where possible
    for col in df.columns:
        if col == "period":
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows where ALL value columns are null
    val_cols = [c for c in df.columns if c != "period"]
    df = df.dropna(subset=val_cols, how="all")

    # Add source metadata back as clean columns
    df.insert(0, "source", "Ghana Statistical Service (GSS StatsBank)")
    df.insert(1, "last_updated", TODAY)

    out_file = f"{stem}_{MONTH}.csv"
    save_clean(df, "gss", out_file, name, "Ghana Statistical Service (GSS StatsBank)",
               freq, desc, date_col="period")

    # Simple dictionary for GSS (auto-generated from column names)
    col_meta = {c: (c.replace("_", " ").title(), "", "") for c in df.columns}
    col_meta["period"]       = ("Period",       "Time period (YYYY-MM for monthly, YYYY-QN for quarterly)", "")
    col_meta["source"]       = ("Source",       "Data source organisation",                                 "")
    col_meta["last_updated"] = ("Last Updated", "Date data was last refreshed",                             "YYYY-MM-DD")
    make_dict(df, "gss", stem, col_meta)


# ─────────────────────────────────────────────────────────────
# 4. BANK OF GHANA DATASETS
# ─────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("  CLEANING BANK OF GHANA DATASETS")
print("="*60)

bog_files = [
    ("bog_fx_rates_2026-04-20.csv",       "bog_fx_rates",       "Bank of Ghana – FX Rates",            "Monthly",
     "Monthly end-of-period and period-average exchange rates for USD, GBP, EUR, CHF, and JPY against the Ghana Cedi, from Bank of Ghana Statistical Bulletins.",
     {
         "date":                   ("Date",              "Year-month of observation",                         "YYYY-MM"),
         "year":                   ("Year",              "Calendar year",                                     ""),
         "month":                  ("Month",             "Month name",                                        ""),
         "usd_ghs_end_period":     ("USD/GHS (End)",     "US Dollar to Ghana Cedi – end of period rate",      "GHS per USD"),
         "usd_ghs_period_average": ("USD/GHS (Average)", "US Dollar to Ghana Cedi – period average rate",     "GHS per USD"),
         "gbp_ghs_end_period":     ("GBP/GHS (End)",     "British Pound to Ghana Cedi – end of period rate",  "GHS per GBP"),
         "gbp_ghs_period_average": ("GBP/GHS (Average)", "British Pound to Ghana Cedi – period average rate", "GHS per GBP"),
         "eur_ghs_end_period":     ("EUR/GHS (End)",     "Euro to Ghana Cedi – end of period rate",           "GHS per EUR"),
         "eur_ghs_period_average": ("EUR/GHS (Average)", "Euro to Ghana Cedi – period average rate",          "GHS per EUR"),
         "chf_ghs_end_period":     ("CHF/GHS (End)",     "Swiss Franc to Ghana Cedi – end of period rate",    "GHS per CHF"),
         "chf_ghs_period_average": ("CHF/GHS (Average)", "Swiss Franc to Ghana Cedi – period average rate",   "GHS per CHF"),
         "jpy_ghs_end_period":     ("JPY/GHS (End)",     "Japanese Yen to Ghana Cedi – end of period rate",   "GHS per JPY"),
         "jpy_ghs_period_average": ("JPY/GHS (Average)", "Japanese Yen to Ghana Cedi – period average rate",  "GHS per JPY"),
         "source":                 ("Source",            "Data source",                                       ""),
         "last_updated":           ("Last Updated",      "Date data was last refreshed",                      "YYYY-MM-DD"),
     }),
    ("bog_cpi_inflation_2026-04-20.csv",  "bog_cpi_inflation",  "Bank of Ghana – CPI & Inflation",     "Monthly",
     "Monthly Consumer Price Index and year-on-year inflation rates for Ghana including headline and core inflation, from Bank of Ghana Statistical Bulletins.",
     {}),
    ("bog_key_indicators_2026-04-20.csv", "bog_key_indicators", "Bank of Ghana – Key Economic Indicators","Monthly",
     "Monthly summary of Ghana's key macroeconomic indicators: inflation, money supply, credit growth, international reserves, and interest rates.",
     {}),
    ("bog_gdp_2026-04-20.csv",            "bog_gdp",            "Bank of Ghana – GDP Data",            "Quarterly",
     "Quarterly GDP data for Ghana including sectoral breakdown (agriculture, industry, services) from Bank of Ghana Statistical Bulletins.",
     {}),
]

for raw_file, stem, name, freq, desc, col_meta_override in bog_files:
    raw_path = os.path.join(RAW, "bog", raw_file)
    if not os.path.exists(raw_path):
        print(f"\n   ✗ Missing: {raw_file}")
        continue

    print(f"\n  🏦 {name}")
    df = pd.read_csv(raw_path)

    # Rename columns to snake_case
    df.columns = [to_snake(c) for c in df.columns]

    # Rename source/scraped_date
    df = df.rename(columns={"scraped_date": "last_updated"})

    # Find date column
    date_col = None
    for c in ["date", "period"]:
        if c in df.columns:
            date_col = c
            break

    # Convert numeric columns
    skip = {"date", "period", "month", "source", "last_updated", "sheet"}
    for col in df.columns:
        if col not in skip:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows where all value columns are null
    val_cols = [c for c in df.columns if c not in skip]
    df = df.dropna(subset=val_cols, how="all")

    out_file = f"{stem}_{MONTH}.csv"
    save_clean(df, "bog", out_file, name, "Bank of Ghana Statistical Bulletin",
               freq, desc, date_col=date_col)

    # Build dictionary
    col_meta = {c: (c.replace("_", " ").title(), "", "") for c in df.columns}
    col_meta.update(col_meta_override)
    col_meta["last_updated"] = ("Last Updated", "Date data was last refreshed", "YYYY-MM-DD")
    make_dict(df, "bog", stem, col_meta)


# ─────────────────────────────────────────────────────────────
# 5. WORLD BANK DATASETS
# ─────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("  CLEANING WORLD BANK DATASETS")
print("="*60)

wb_files = [
    ("ghana_macroeconomic_2026-04-20.csv",      "wb_ghana_macroeconomic",
     "World Bank – Ghana Macroeconomic Indicators", "Annual",
     "Annual macroeconomic indicators for Ghana including GDP, inflation, exchange rate, government debt, revenue and expenditure. Source: World Bank Open Data API."),
    ("ghana_trade_investment_2026-04-20.csv",   "wb_ghana_trade_investment",
     "World Bank – Ghana Trade & Investment",      "Annual",
     "Annual trade and foreign direct investment data for Ghana including exports, imports, and FDI flows. Source: World Bank Open Data API."),
    ("ghana_social_demographic_2026-04-20.csv", "wb_ghana_social_demographic",
     "World Bank – Ghana Social & Demographic",   "Annual",
     "Annual population, employment, poverty, health and education statistics for Ghana. Source: World Bank Open Data API."),
    ("ghana_financial_sector_2026-04-20.csv",   "wb_ghana_financial_sector",
     "World Bank – Ghana Financial Sector",       "Annual",
     "Annual banking sector, interest rates, credit and financial inclusion data for Ghana. Source: World Bank Open Data API."),
    ("ghana_agriculture_2026-04-20.csv",        "wb_ghana_agriculture",
     "World Bank – Ghana Agriculture",           "Annual",
     "Annual agricultural production, land use, food security and rural economy data for Ghana. Source: World Bank Open Data API."),
    ("ghana_energy_infrastructure_2026-04-20.csv","wb_ghana_energy_infrastructure",
     "World Bank – Ghana Energy & Infrastructure","Annual",
     "Annual energy access, electricity, internet and infrastructure data for Ghana. Source: World Bank Open Data API."),
]

for raw_file, stem, name, freq, desc in wb_files:
    raw_path = os.path.join(RAW, "worldbank", raw_file)
    if not os.path.exists(raw_path):
        print(f"\n   ✗ Missing: {raw_file}")
        continue

    print(f"\n  🌍 {name}")
    df = pd.read_csv(raw_path)

    # Rename columns to snake_case
    df.columns = [to_snake(c) for c in df.columns]
    df = df.rename(columns={"scraped_date": "last_updated"})

    # Round numeric columns to 4 decimal places
    skip = {"year", "country", "country_code", "source", "last_updated"}
    for col in df.columns:
        if col not in skip:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(4)

    # Sort by year descending
    if "year" in df.columns:
        df = df.sort_values("year", ascending=False).reset_index(drop=True)

    out_file = f"{stem}_{MONTH}.csv"
    save_clean(df, "worldbank", out_file, name, "World Bank Open Data", freq, desc, date_col="year")

    # Dictionary
    col_meta = {c: (c.replace("_", " ").title(), "", "") for c in df.columns}
    col_meta["year"]         = ("Year",         "Calendar year of observation",       "")
    col_meta["country"]      = ("Country",      "Country name",                       "")
    col_meta["country_code"] = ("Country Code", "ISO 2-letter country code",          "")
    col_meta["source"]       = ("Source",       "Data source organisation",           "")
    col_meta["last_updated"] = ("Last Updated", "Date data was last refreshed",       "YYYY-MM-DD")
    make_dict(df, "worldbank", stem, col_meta)


# ─────────────────────────────────────────────────────────────
# 6. MASTER CATALOGUE
# ─────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("  GENERATING MASTER CATALOGUE")
print("="*60)

cat_df = pd.DataFrame(catalogue)
cat_path = os.path.join(BASE, "catalogue.csv")
cat_df.to_csv(cat_path, index=False)
print(f"\n  ✓ Master catalogue: {len(cat_df)} datasets → datasets/catalogue.csv")
print(cat_df[["dataset_name","frequency","rows","date_start","date_end"]].to_string(index=False))

print("\n" + "="*60)
print("  ✅ PIPELINE COMPLETE")
print("="*60)
print(f"  Clean datasets:  datasets/clean/")
print(f"  Dictionaries:    datasets/clean/dictionaries/")
print(f"  Master catalogue: datasets/catalogue.csv")
print("="*60)
