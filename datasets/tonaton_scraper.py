"""
SG Datalytics — Tonaton Ghana Market Price Scraper
===================================================
Source: tonaton.com (public listings, robots.txt permits standard browser agents)
Data:   Product prices, locations, conditions across key market categories

Run:  python tonaton_scraper.py
Output: CSV files in ./output/tonaton/ folder, ready for SG Datalytics.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os
import time
import re

OUTPUT_DIR = "output/tonaton"
os.makedirs(OUTPUT_DIR, exist_ok=True)

TODAY = datetime.today().strftime("%Y-%m-%d")
BASE_URL = "https://tonaton.com"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://tonaton.com/",
}

# ─────────────────────────────────────────────────────────────
# CATEGORIES TO SCRAPE
# ─────────────────────────────────────────────────────────────
CATEGORIES = [
    {
        "name": "Mobile Phones & Tablets",
        "slug": "c_mobile-phones-tablets",
        "file": f"tonaton_mobile_phones_{TODAY}.csv",
        "pages": 15,   # 18 listings/page → ~270 records
    },
    {
        "name": "Vehicles",
        "slug": "c_vehicles",
        "file": f"tonaton_vehicles_{TODAY}.csv",
        "pages": 15,
    },
    {
        "name": "Electronics",
        "slug": "c_electronics",
        "file": f"tonaton_electronics_{TODAY}.csv",
        "pages": 10,
    },
    {
        "name": "Home, Furniture & Appliances",
        "slug": "c_home-garden",
        "file": f"tonaton_home_furniture_{TODAY}.csv",
        "pages": 10,
    },
    {
        "name": "Food, Agriculture & Farming",
        "slug": "c_agriculture-and-foodstuff",
        "file": f"tonaton_food_agriculture_{TODAY}.csv",
        "pages": 15,
    },
    {
        "name": "Property / Real Estate",
        "slug": "c_real-estate",
        "file": f"tonaton_real_estate_{TODAY}.csv",
        "pages": 15,
    },
    {
        "name": "Fashion",
        "slug": "c_fashion-and-beauty",
        "file": f"tonaton_fashion_{TODAY}.csv",
        "pages": 8,
    },
    {
        "name": "Commercial Equipment & Tools",
        "slug": "c_office-and-commercial-equipment-tools",
        "file": f"tonaton_commercial_equipment_{TODAY}.csv",
        "pages": 8,
    },
]


# ─────────────────────────────────────────────────────────────
# PARSE A SINGLE LISTING CARD
# ─────────────────────────────────────────────────────────────
def parse_listing(a_tag, category_name):
    """Extract all data fields from a single Tonaton listing anchor tag."""
    record = {
        "Category":     category_name,
        "Source":       "Tonaton Ghana (tonaton.com)",
        "Scraped Date": TODAY,
        "URL":          BASE_URL + a_tag.get("href", "").split("?")[0],
    }

    content = a_tag.find("div", class_="product__content")
    if not content:
        return None

    # Title (confusingly stored in product__description)
    title_el = content.find("p", class_="product__description")
    record["Title"] = title_el.get_text(strip=True) if title_el else None

    # Price (stored in product__title span)
    price_el = content.find("span", class_="product__title")
    if price_el:
        raw_price = price_el.get_text(strip=True)
        record["Price Raw"] = raw_price
        nums = re.findall(r"[\d]+", raw_price.replace(",", ""))
        record["Price GHS"] = float(nums[0]) if nums else None
    else:
        record["Price Raw"] = None
        record["Price GHS"] = None

    # Period (rental period, e.g. "Per Month" — relevant for property/services)
    period_el = content.find("p", class_="product__period")
    record["Period"] = period_el.get_text(strip=True) if period_el else None

    # Location
    loc_el = content.find("p", class_="product__location")
    if loc_el:
        loc_text = loc_el.get_text(strip=True)
        record["Location"] = loc_text
        parts = loc_text.split(",", 1)
        record["Region"] = parts[0].strip() if parts else None
        record["Area"] = parts[1].strip() if len(parts) > 1 else None
    else:
        record["Location"] = None
        record["Region"] = None
        record["Area"] = None

    # Tags (condition, OS, type — e.g. "Brand New", "Windows", "Foreign Used")
    tags_el = content.find("div", class_="product__tags")
    if tags_el:
        tags = [s.get_text(strip=True) for s in tags_el.find_all("span") if s.get_text(strip=True)]
        record["Tags"] = ", ".join(tags) if tags else None
        # Try to extract condition specifically
        condition_keywords = ["Brand New", "New", "Used", "Foreign Used", "Refurbished"]
        condition = next((t for t in tags if any(k.lower() in t.lower() for k in condition_keywords)), None)
        record["Condition"] = condition
    else:
        record["Tags"] = None
        record["Condition"] = None

    return record


# ─────────────────────────────────────────────────────────────
# SCRAPE ONE CATEGORY (ALL PAGES)
# ─────────────────────────────────────────────────────────────
def scrape_category(category):
    name = category["name"]
    slug = category["slug"]
    max_pages = category["pages"]

    print(f"\n🛍️  {name}  (/{slug})")
    all_records = []

    for page in range(1, max_pages + 1):
        url = f"{BASE_URL}/{slug}?page={page}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code != 200:
                print(f"   ✗ Page {page} returned {r.status_code} — stopping")
                break

            soup = BeautifulSoup(r.text, "lxml")
            items = soup.find_all("a", class_="product__item")

            if not items:
                print(f"   ✗ No listings on page {page} — stopping")
                break

            for a in items:
                record = parse_listing(a, name)
                if record:
                    all_records.append(record)

            print(f"   ✓ Page {page:>2}: {len(items)} listings  (total: {len(all_records)})")

        except Exception as e:
            print(f"   ✗ Page {page} error: {e}")
            break

        time.sleep(1.5)  # Polite delay between pages

    if not all_records:
        print(f"   ✗ No data collected for {name}")
        return None

    df = pd.DataFrame(all_records)

    # Reorder columns
    cols = ["Category", "Title", "Price GHS", "Price Raw", "Period", "Condition",
            "Tags", "Region", "Area", "Location", "URL", "Source", "Scraped Date"]
    df = df[[c for c in cols if c in df.columns]]

    filepath = f"{OUTPUT_DIR}/{category['file']}"
    df.to_csv(filepath, index=False)
    print(f"   💾 Saved {len(df)} rows → {filepath}")
    return df


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 62)
    print("  SG DATALYTICS — Tonaton Ghana Market Price Scraper")
    print(f"  Source: tonaton.com (public listings)")
    print(f"  Date:   {datetime.today().strftime('%d %B %Y')}")
    print("=" * 62)

    results = {}
    for cat in CATEGORIES:
        df = scrape_category(cat)
        results[cat["name"]] = df
        time.sleep(3)  # Polite delay between categories

    # Summary
    print("\n" + "=" * 62)
    print("  ✅ SUMMARY")
    print("=" * 62)
    total = 0
    for name, df in results.items():
        if df is not None and not df.empty:
            avg_price = df["Price GHS"].mean()
            print(f"  ✓ {name:<40} {len(df):>4} listings  "
                  f"(avg GH₵ {avg_price:,.0f})" if avg_price else
                  f"  ✓ {name:<40} {len(df):>4} listings")
            total += len(df)
        else:
            print(f"  ✗ {name:<40} Failed")

    print(f"\n  Total listings collected: {total:,}")
    print(f"  Output folder: ./{OUTPUT_DIR}/")
    print("=" * 62)

    # Price summary table
    print("\n  📊 Price Summary by Category:")
    print(f"  {'Category':<40} {'Listings':>8} {'Min GH₵':>10} {'Median GH₵':>12} {'Max GH₵':>10}")
    print("  " + "-" * 82)
    for name, df in results.items():
        if df is not None and "Price GHS" in df.columns:
            p = df["Price GHS"].dropna()
            if len(p) > 0:
                print(f"  {name:<40} {len(df):>8} {p.min():>10,.0f} {p.median():>12,.0f} {p.max():>10,.0f}")
