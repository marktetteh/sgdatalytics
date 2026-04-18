"""
SG Datalytics — Database Seeder & World Bank Data Fetcher
Seeds sectors, countries, indicators, then pulls real data from World Bank API
and stores it in PostgreSQL.
"""

import psycopg2
import psycopg2.extras
import requests
import time
import sys
from datetime import datetime

# ── CONNECTION ──────────────────────────────────────────────
DB = dict(host='localhost', dbname='sgdatalytics', user='sgdata', password='sgdata2025')

def conn():
    return psycopg2.connect(**DB)

def log(msg, symbol='→'):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"  [{ts}] {symbol} {msg}", flush=True)

# ── SEED DATA ───────────────────────────────────────────────

SECTORS = [
    ('economy',     'Economy & Finance',    '💹', '#a78bfa', 'Macroeconomic indicators, GDP, trade and fiscal data'),
    ('education',   'Education',            '🏫', '#60a5fa', 'Enrollment, literacy, completion and expenditure data'),
    ('health',      'Health & Demographics','🏥', '#f87171', 'Population, mortality, fertility and health system data'),
    ('environment', 'Sustainability',       '🌿', '#34d399', 'CO2, energy, renewables and forest coverage data'),
    ('agriculture', 'Agriculture',          '🌾', '#fbbf24', 'Land use, yield, employment and irrigation data'),
    ('business',    'Business & SMEs',      '📊', '#f59e0b', 'Business climate, labor force and financial access data'),
]

COUNTRIES = [
    # West Africa
    ('GH','GHA','Ghana',          'West Africa',    '🇬🇭', 'Lower middle income'),
    ('NG','NGA','Nigeria',        'West Africa',    '🇳🇬', 'Lower middle income'),
    ('SN','SEN','Senegal',        'West Africa',    '🇸🇳', 'Lower middle income'),
    ('CI','CIV',"Côte d'Ivoire", 'West Africa',    '🇨🇮', 'Lower middle income'),
    ('TG','TGO','Togo',           'West Africa',    '🇹🇬', 'Low income'),
    ('BF','BFA','Burkina Faso',   'West Africa',    '🇧🇫', 'Low income'),
    ('ML','MLI','Mali',           'West Africa',    '🇲🇱', 'Low income'),
    ('GN','GIN','Guinea',         'West Africa',    '🇬🇳', 'Low income'),
    ('LR','LBR','Liberia',        'West Africa',    '🇱🇷', 'Low income'),
    ('SL','SLE','Sierra Leone',   'West Africa',    '🇸🇱', 'Low income'),
    # East Africa
    ('KE','KEN','Kenya',          'East Africa',    '🇰🇪', 'Lower middle income'),
    ('TZ','TZA','Tanzania',       'East Africa',    '🇹🇿', 'Lower middle income'),
    ('UG','UGA','Uganda',         'East Africa',    '🇺🇬', 'Low income'),
    ('ET','ETH','Ethiopia',       'East Africa',    '🇪🇹', 'Low income'),
    ('RW','RWA','Rwanda',         'East Africa',    '🇷🇼', 'Low income'),
    # Southern Africa
    ('ZA','ZAF','South Africa',   'Southern Africa','🇿🇦', 'Upper middle income'),
    ('ZM','ZMB','Zambia',         'Southern Africa','🇿🇲', 'Lower middle income'),
    ('ZW','ZWE','Zimbabwe',       'Southern Africa','🇿🇼', 'Lower middle income'),
    ('BW','BWA','Botswana',       'Southern Africa','🇧🇼', 'Upper middle income'),
    # North Africa
    ('EG','EGY','Egypt',          'North Africa',   '🇪🇬', 'Lower middle income'),
    ('MA','MAR','Morocco',        'North Africa',   '🇲🇦', 'Lower middle income'),
    ('TN','TUN','Tunisia',        'North Africa',   '🇹🇳', 'Lower middle income'),
    # Global Benchmarks
    ('US','USA','United States',  'Global Benchmarks','🇺🇸','High income'),
    ('GB','GBR','United Kingdom', 'Global Benchmarks','🇬🇧','High income'),
    ('CN','CHN','China',          'Global Benchmarks','🇨🇳','Upper middle income'),
    ('IN','IND','India',          'Global Benchmarks','🇮🇳','Lower middle income'),
    ('BR','BRA','Brazil',         'Global Benchmarks','🇧🇷','Upper middle income'),
    ('DE','DEU','Germany',        'Global Benchmarks','🇩🇪','High income'),
]

# (wb_code, name, unit, fmt, sector_code)
INDICATORS = [
    # Economy
    ('NY.GDP.MKTP.CD',   'GDP (current US$)',                    'USD',       'B',   'economy'),
    ('NY.GDP.MKTP.KD.ZG','GDP Growth Rate',                     '%',         'pct', 'economy'),
    ('NY.GDP.PCAP.CD',   'GDP per Capita (current US$)',         'USD',       'num', 'economy'),
    ('FP.CPI.TOTL.ZG',   'Inflation, Consumer Prices',          '%',         'pct', 'economy'),
    ('BN.CAB.XOKA.GD.ZS','Current Account Balance (% of GDP)', '%',         'pct', 'economy'),
    ('GC.DOD.TOTL.GD.ZS','Central Government Debt (% of GDP)', '%',         'pct', 'economy'),
    ('BX.KLT.DINV.WD.GD.ZS','Foreign Direct Investment (% of GDP)','%',     'pct', 'economy'),
    ('NE.EXP.GNFS.ZS',   'Exports of Goods & Services (% of GDP)','%',      'pct', 'economy'),
    # Education
    ('SE.PRM.ENRR',      'Primary School Enrollment (% gross)', '%',         'pct', 'education'),
    ('SE.SEC.ENRR',      'Secondary School Enrollment (% gross)','%',        'pct', 'education'),
    ('SE.TER.ENRR',      'Tertiary Enrollment (% gross)',       '%',         'pct', 'education'),
    ('SE.ADT.LITR.ZS',   'Adult Literacy Rate (% ages 15+)',   '%',         'pct', 'education'),
    ('SE.PRM.CMPT.ZS',   'Primary Completion Rate',            '%',         'pct', 'education'),
    ('SE.XPD.TOTL.GD.ZS','Government Education Expenditure (% of GDP)','%', 'pct', 'education'),
    ('SE.PRM.TENR',      'Primary Net Enrollment Rate',        '%',         'pct', 'education'),
    # Health
    ('SP.POP.TOTL',      'Total Population',                   'people',    'M',   'health'),
    ('SP.DYN.LE00.IN',   'Life Expectancy at Birth (years)',   'years',     'dec', 'health'),
    ('SH.DYN.MORT',      'Under-5 Mortality Rate (per 1,000)', 'per 1,000', 'dec', 'health'),
    ('SH.STA.BRTC.ZS',   'Births Attended by Skilled Staff',  '%',         'pct', 'health'),
    ('SP.DYN.TFRT.IN',   'Fertility Rate (births per woman)',  'births',    'dec', 'health'),
    ('SH.MED.BEDS.ZS',   'Hospital Beds (per 1,000 people)',  'per 1,000', 'dec', 'health'),
    ('SP.URB.TOTL.IN.ZS','Urban Population (% of total)',     '%',         'pct', 'health'),
    # Environment
    ('EN.ATM.CO2E.PC',   'CO2 Emissions (metric tons per capita)','mt/cap', 'dec', 'environment'),
    ('EG.ELC.ACCS.ZS',   'Access to Electricity (% of population)','%',    'pct', 'environment'),
    ('EG.FEC.RNEW.ZS',   'Renewable Energy Consumption (%)',  '%',         'pct', 'environment'),
    ('AG.LND.FRST.ZS',   'Forest Area (% of land area)',      '%',         'pct', 'environment'),
    ('ER.H2O.FWTL.ZS',   'Freshwater Withdrawals (% of resources)','%',    'pct', 'environment'),
    ('EN.ATM.METH.KT.CE','Methane Emissions (kt CO2 equivalent)','kt CO2e','K',   'environment'),
    # Agriculture
    ('AG.LND.AGRI.ZS',   'Agricultural Land (% of land area)','%',         'pct', 'agriculture'),
    ('NV.AGR.TOTL.ZS',   'Agriculture Value Added (% of GDP)','%',         'pct', 'agriculture'),
    ('SL.AGR.EMPL.ZS',   'Employment in Agriculture (%)',     '%',         'pct', 'agriculture'),
    ('AG.YLD.CREL.KG',   'Cereal Yield (kg per hectare)',     'kg/ha',     'num', 'agriculture'),
    ('AG.LND.IRIG.AG.ZS','Agricultural Land Irrigated (%)',   '%',         'pct', 'agriculture'),
    # Business
    ('IC.BUS.EASE.XQ',   'Ease of Doing Business Score',      'score',     'dec', 'business'),
    ('SL.UEM.TOTL.ZS',   'Unemployment Rate (% of labor force)','%',       'pct', 'business'),
    ('SL.TLF.ACTI.ZS',   'Labor Force Participation Rate (%)', '%',        'pct', 'business'),
    ('IC.REG.DURS',      'Time to Start a Business (days)',   'days',      'dec', 'business'),
    ('FB.ATM.TOTL.P5',   'ATMs (per 100,000 adults)',         'per 100k',  'dec', 'business'),
]

# ── SEED FUNCTIONS ──────────────────────────────────────────

def seed_sectors(cur):
    log("Seeding sectors…", "▶")
    for code, name, icon, color, desc in SECTORS:
        cur.execute("""
            INSERT INTO sectors (code, name, icon, color, description)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (code) DO UPDATE
              SET name=EXCLUDED.name, icon=EXCLUDED.icon,
                  color=EXCLUDED.color, description=EXCLUDED.description
        """, (code, name, icon, color, desc))
    log(f"{len(SECTORS)} sectors seeded ✓", "✓")

def seed_countries(cur):
    log("Seeding countries…", "▶")
    for code, iso3, name, region, flag, income in COUNTRIES:
        cur.execute("""
            INSERT INTO countries (code, iso3, name, region, flag, income_level)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (code) DO UPDATE
              SET iso3=EXCLUDED.iso3, name=EXCLUDED.name,
                  region=EXCLUDED.region, flag=EXCLUDED.flag,
                  income_level=EXCLUDED.income_level
        """, (code, iso3, name, region, flag, income))
    log(f"{len(COUNTRIES)} countries seeded ✓", "✓")

def seed_indicators(cur):
    log("Seeding indicators…", "▶")
    for wb_code, name, unit, fmt, sector_code in INDICATORS:
        cur.execute("SELECT id FROM sectors WHERE code=%s", (sector_code,))
        row = cur.fetchone()
        if not row:
            continue
        sector_id = row[0]
        cur.execute("""
            INSERT INTO indicators (wb_code, name, unit, fmt, sector_id, source)
            VALUES (%s,%s,%s,%s,%s,'World Bank')
            ON CONFLICT (wb_code) DO UPDATE
              SET name=EXCLUDED.name, unit=EXCLUDED.unit,
                  fmt=EXCLUDED.fmt, sector_id=EXCLUDED.sector_id
        """, (wb_code, name, unit, fmt, sector_id))
    log(f"{len(INDICATORS)} indicators seeded ✓", "✓")

# ── WORLD BANK FETCH ────────────────────────────────────────

WB_BASE = "https://api.worldbank.org/v2"

def fetch_wb(country_code, indicator_code, year_from=2000, year_to=2023):
    url = f"{WB_BASE}/country/{country_code}/indicator/{indicator_code}"
    params = {"format":"json","per_page":60,"date":f"{year_from}:{year_to}"}
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        if len(data) < 2 or not data[1]:
            return [], url
        records = [(int(d["date"]), float(d["value"]))
                   for d in data[1] if d["value"] is not None]
        return records, url
    except Exception as e:
        return [], url

def store_data_points(cur, country_id, indicator_id, records, url):
    inserted = 0
    for year, value in records:
        cur.execute("""
            INSERT INTO data_points (country_id, indicator_id, year, value)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT (country_id, indicator_id, year)
            DO UPDATE SET value=EXCLUDED.value, fetched_at=NOW()
        """, (country_id, indicator_id, year, value))
        inserted += 1
    cur.execute("""
        INSERT INTO fetch_log
          (country_id, indicator_id, year_from, year_to, records_fetched, source_url)
        VALUES (%s,%s,%s,%s,%s,%s)
    """, (country_id, indicator_id, 2000, 2023, inserted, url))
    return inserted

# Priority: fetch Ghana for all indicators, then key African countries for all, then benchmarks for key indicators
PRIORITY_COUNTRIES = ['GH','NG','KE','ZA','ET','SN','EG','IN','US']
KEY_INDICATORS = [
    'NY.GDP.MKTP.CD','NY.GDP.MKTP.KD.ZG','NY.GDP.PCAP.CD','FP.CPI.TOTL.ZG',
    'SP.POP.TOTL','SP.DYN.LE00.IN','SE.PRM.ENRR','SE.ADT.LITR.ZS',
    'EN.ATM.CO2E.PC','EG.ELC.ACCS.ZS','AG.LND.AGRI.ZS','SL.UEM.TOTL.ZS',
]

def fetch_all(cur, full=False):
    log("Starting World Bank data fetch…", "🌐")

    # Build lookup maps
    cur.execute("SELECT code, id FROM countries")
    country_map = dict(cur.fetchall())

    cur.execute("SELECT wb_code, id FROM indicators")
    ind_map = dict(cur.fetchall())

    total_records = 0
    total_calls   = 0
    errors        = 0

    if full:
        fetch_countries = [c[0] for c in COUNTRIES]
        fetch_indicators = [i[0] for i in INDICATORS]
    else:
        # Smart priority mode: all indicators for priority countries
        fetch_countries = PRIORITY_COUNTRIES
        fetch_indicators = [i[0] for i in INDICATORS]

    total_pairs = len(fetch_countries) * len(fetch_indicators)
    log(f"Fetching {len(fetch_countries)} countries × {len(fetch_indicators)} indicators = {total_pairs} API calls", "📡")
    log("This will take a few minutes. Fetching in parallel batches…", "⏳")

    done = 0
    for c_code in fetch_countries:
        c_id = country_map.get(c_code)
        if not c_id:
            continue
        c_name = next((c[2] for c in COUNTRIES if c[0]==c_code), c_code)
        for ind_code in fetch_indicators:
            ind_id = ind_map.get(ind_code)
            if not ind_id:
                continue
            records, url = fetch_wb(c_code, ind_code)
            recs = store_data_points(cur, c_id, ind_id, records, url)
            total_records += recs
            total_calls   += 1
            done += 1
            if records:
                log(f"{c_name} · {ind_code} → {recs} records", "  ✓")
            else:
                errors += 1
                log(f"{c_name} · {ind_code} → no data", "  –")
            # Be polite to the API
            time.sleep(0.18)

        # Commit after each country
        cur.connection.commit()
        log(f"── {c_name} complete ({done}/{total_pairs}) ──", "★")

    return total_records, total_calls, errors

# ── MAIN ────────────────────────────────────────────────────

def main():
    full = '--full' in sys.argv
    print()
    print("  ╔══════════════════════════════════════════════════╗")
    print("  ║   SG DATALYTICS — Database Seeder & WB Fetcher  ║")
    print("  ╚══════════════════════════════════════════════════╝")
    print()

    c = conn()
    cur = c.cursor()

    log("Connected to sgdatalytics @ localhost:5432", "🔗")
    print()

    # 1. Seed reference tables
    log("── PHASE 1: Seeding reference data ──", "")
    seed_sectors(cur)
    seed_countries(cur)
    seed_indicators(cur)
    c.commit()
    print()

    # 2. Fetch World Bank data
    log("── PHASE 2: Fetching World Bank data ──", "")
    t0 = time.time()
    total_records, total_calls, errors = fetch_all(cur, full=full)
    elapsed = time.time() - t0
    c.commit()
    print()

    # 3. Summary
    log("── PHASE 3: Summary ──", "")
    cur.execute("SELECT COUNT(*) FROM data_points")
    dp_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT country_id) FROM data_points")
    country_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT indicator_id) FROM data_points")
    ind_count = cur.fetchone()[0]
    cur.execute("SELECT MIN(year), MAX(year) FROM data_points")
    yr_min, yr_max = cur.fetchone()

    print()
    print("  ┌─────────────────────────────────────────────────┐")
    print(f"  │  Total data points stored : {dp_count:>10,}          │")
    print(f"  │  Countries with data      : {country_count:>10}          │")
    print(f"  │  Indicators with data     : {ind_count:>10}          │")
    print(f"  │  Year range               : {yr_min} – {yr_max}           │")
    print(f"  │  API calls made           : {total_calls:>10}          │")
    print(f"  │  Records fetched          : {total_records:>10,}          │")
    print(f"  │  Errors / no-data         : {errors:>10}          │")
    print(f"  │  Time elapsed             : {elapsed:>9.1f}s          │")
    print("  └─────────────────────────────────────────────────┘")
    print()
    log("Database ready! ✓", "🎉")

    cur.close()
    c.close()

if __name__ == "__main__":
    main()
