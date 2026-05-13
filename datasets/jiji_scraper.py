"""
SG Datalytics — Jiji Ghana Market Price Scraper
================================================
Source: jiji.com.gh (public listings, robots.txt permits scraping)
Data:   Product prices, locations, conditions across key market categories

Run:  python jiji_scraper.py
Output: CSV files in ./output/jiji/ folder, ready for SG Datalytics.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os
import time
import re

OUTPUT_DIR = "output/jiji"
os.makedirs(OUTPUT_DIR, exist_ok=True)

TODAY = datetime.today().strftime("%Y-%m-%d")
BASE_URL = "https://jiji.com.gh"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://jiji.com.gh/",
}

# ─────────────────────────────────────────────────────────────
# CATEGORIES TO SCRAPE
# ─────────────────────────────────────────────────────────────
CATEGORIES = [
    {
        "name": "Mobile Phones & Tablets",
        "slug": "mobile-phones-tablets",
        "file": f"jiji_mobile_phones_{TODAY}.csv",
        "pages": 15,   # 24 listings/page → ~360 records
    },
    {
        "name": "Vehicles",
        "slug": "vehicles",
        "file": f"jiji_vehicles_{TODAY}.csv",
        "pages": 15,
    },
    {
        "name": "Electronics",
        "slug": "electronics",
        "file": f"jiji_electronics_{TODAY}.csv",
        "pages": 10,
    },
    {
        "name": "Home, Furniture & Appliances",
        "slug": "home-garden",
        "file": f"jiji_home_furniture_{TODAY}.csv",
        "pages": 10,
    },
    {
        "name": "Food, Agriculture & Farming",
        "slug": "agriculture-and-foodstuff",
        "file": f"jiji_food_agriculture_{TODAY}.csv",
        "pages": 15,   # Scrape more — important for food price data
    },
    {
        "name": "Property / Real Estate",
        "slug": "real-estate",
        "file": f"jiji_real_estate_{TODAY}.csv",
        "pages": 15,
    },
    {
        "name": "Fashion",
        "slug": "fashion-and-beauty",
        "file": f"jiji_fashion_{TODAY}.csv",
        "pages": 8,
    },
    {
        "name": "Commercial Equipment & Tools",
        "slug": "office-and-commercial-equipment-tools",
        "file": f"jiji_commercial_equipment_{TODAY}.csv",
        "pages": 8,
    },
]


# ─────────────────────────────────────────────────────────────
# PARSE A SINGLE LISTING CARD
# ─────────────────────────────────────────────────────────────
def parse_listing(a_tag, category_name):
    """Extract all data fields from a single listing anchor tag."""
    record = {
        "Category":     category_name,
        "Source":       "Jiji Ghana (jiji.com.gh)",
        "Scraped Date": TODAY,
        "URL":          BASE_URL + a_tag.get("href", "").split("?")[0],
    }

    # Title
    title_el = a_tag.find(class_="qa-advert-title")
    record["Title"] = title_el.get_text(strip=True) if title_el else None

    # Price — extract raw text then clean to numeric
    price_el = a_tag.find(class_="qa-advert-price")
    if price_el:
        raw_price = price_el.get_text(strip=True)
        record["Price Raw"] = raw_price
        # Extract numeric value (remove GH₵, commas, spaces)
        nums = re.findall(r"[\d,]+", raw_price.replace(",", ""))
        record["Price GHS"] = float(nums[0]) if nums else None
    else:
        record["Price Raw"] = None
        record["Price GHS"] = None

    # Location
    loc_el = a_tag.find(class_="b-list-advert__region__text")
    if loc_el:
        loc_text = loc_el.get_text(strip=True)
        record["Location"] = loc_text
        # Split into region and area
        parts = loc_text.split(",", 1)
        record["Region"] = parts[0].strip() if parts else None
        record["Area"] = parts[1].strip() if len(parts) > 1 else None
    else:
        record["Location"] = None
        record["Region"] = None
        record["Area"] = None

    # Condition (Brand New / Used / Refurbished / Foreign Used)
    cond_els = a_tag.find_all(class_="b-list-advert-base__item-attr")
    record["Condition"] = ", ".join(
        el.get_text(strip=True) for el in cond_els if el.get_text(strip=True)
    ) or None

    # Seller badge (Enterprise, Premium, Diamond, X+ years on Jiji, etc.)
    badge_els = a_tag.find_all(class_="b-list-advert-base__label__inner")
    record["Seller Badge"] = ", ".join(
        el.get_text(strip=True) for el in badge_els if el.get_text(strip=True)
    ) or None

    # Short description snippet
    desc_el = a_tag.find(class_="b-list-advert-base__description-text")
    record["Description"] = desc_el.get_text(strip=True)[:200] if desc_el else None

    return record


# ─────────────────────────────────────────────────────────────
# SCRAPE ONE CATEGORY (ALL PAGES)
# ─────────────────────────────────────────────────────────────
def scrape_category(category):
    name = category["name"]
    slug = category["slug"]
    max_pages = category["pages"]

    print(f"\n🛒 {name}  (/{slug})")
    all_records = []

    for page in range(1, max_pages + 1):
        url = f"{BASE_URL}/{slug}?page={page}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code != 200:
                print(f"   ✗ Page {page} returned {r.status_code} — stopping")
                break

            soup = BeautifulSoup(r.text, "lxml")
            listings = soup.find_all("a", class_="qa-advert-list-item")

            if not listings:
                print(f"   ✗ No listings on page {page} — stopping")
                break

            for a in listings:
                record = parse_listing(a, name)
                all_records.append(record)

            print(f"   ✓ Page {page:>2}: {len(listings)} listings  (total: {len(all_records)})")

        except Exception as e:
            print(f"   ✗ Page {page} error: {e}")
            break

        time.sleep(1.5)  # Polite delay between pages

    if not all_records:
        print(f"   ✗ No data collected for {name}")
        return None

    df = pd.DataFrame(all_records)

    # Reorder columns for readability
    cols = ["Category", "Title", "Price GHS", "Price Raw", "Condition",
            "Region", "Area", "Location", "Seller Badge", "Description",
            "URL", "Source", "Scraped Date"]
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
    print("  SG DATALYTICS — Jiji Ghana Market Price Scraper")
    print(f"  Source: jiji.com.gh (public listings)")
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
            priced = df["Price GHS"].notna().sum()
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

    # Price summary preview
    print("\n  📊 Price Summary by Category:")
    print(f"  {'Category':<40} {'Listings':>8} {'Min GH₵':>10} {'Median GH₵':>12} {'Max GH₵':>10}")
    print("  " + "-" * 82)
    for name, df in results.items():
        if df is not None and "Price GHS" in df.columns:
            p = df["Price GHS"].dropna()
            if len(p) > 0:
                print(f"  {name:<40} {len(df):>8} {p.min():>10,.0f} {p.median():>12,.0f} {p.max():>10,.0f}")
