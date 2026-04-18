"""
SG Datalytics — Database Initialiser for Railway
Run once after Railway provisions PostgreSQL:

  railway run python3 init_db.py

Or set it as a Railway job / one-off command.
Reads DATABASE_URL from Railway's environment automatically.
"""

import os
import sys
import psycopg2

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Railway provides DATABASE_URL automatically ──────────────
DATABASE_URL = os.getenv('DATABASE_URL')

if DATABASE_URL:
    # Railway format: postgresql://user:pass@host:port/dbname
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
else:
    # Fallback to individual vars (local dev)
    conn = psycopg2.connect(
        host     = os.getenv('DB_HOST',     'localhost'),
        port     = os.getenv('DB_PORT',     '5432'),
        dbname   = os.getenv('DB_NAME',     'sgdatalytics'),
        user     = os.getenv('DB_USER',     'sgdata'),
        password = os.getenv('DB_PASSWORD', 'sgdata2025'),
    )

cur = conn.cursor()
print("Connected to database ✓")

# ── Apply schema ─────────────────────────────────────────────
schema_path = os.path.join(os.path.dirname(__file__), 'database', 'schema.sql')
with open(schema_path) as f:
    schema_sql = f.read()

print("Applying schema...")
cur.execute(schema_sql)
conn.commit()
print("Schema applied ✓")

# ── Seed reference data ──────────────────────────────────────
SECTORS = [
    ('economy',     'Economy & Finance',    '💹', '#a78bfa', 'Macroeconomic indicators, GDP, trade and fiscal data'),
    ('education',   'Education',            '🏫', '#60a5fa', 'Enrollment, literacy, completion and expenditure data'),
    ('health',      'Health & Demographics','🏥', '#f87171', 'Population, mortality, fertility and health system data'),
    ('environment', 'Sustainability',       '🌿', '#34d399', 'CO2, energy, renewables and forest coverage data'),
    ('agriculture', 'Agriculture',          '🌾', '#fbbf24', 'Land use, yield, employment and irrigation data'),
    ('business',    'Business & SMEs',      '📊', '#f59e0b', 'Business climate, labor force and financial access data'),
]

COUNTRIES = [
    ('GH','GHA','Ghana',          'West Africa',    '🇬🇭','Lower middle income'),
    ('NG','NGA','Nigeria',        'West Africa',    '🇳🇬','Lower middle income'),
    ('SN','SEN','Senegal',        'West Africa',    '🇸🇳','Lower middle income'),
    ('CI','CIV',"Côte d'Ivoire", 'West Africa',    '🇨🇮','Lower middle income'),
    ('TG','TGO','Togo',           'West Africa',    '🇹🇬','Low income'),
    ('BF','BFA','Burkina Faso',   'West Africa',    '🇧🇫','Low income'),
    ('ML','MLI','Mali',           'West Africa',    '🇲🇱','Low income'),
    ('GN','GIN','Guinea',         'West Africa',    '🇬🇳','Low income'),
    ('LR','LBR','Liberia',        'West Africa',    '🇱🇷','Low income'),
    ('SL','SLE','Sierra Leone',   'West Africa',    '🇸🇱','Low income'),
    ('KE','KEN','Kenya',          'East Africa',    '🇰🇪','Lower middle income'),
    ('TZ','TZA','Tanzania',       'East Africa',    '🇹🇿','Lower middle income'),
    ('UG','UGA','Uganda',         'East Africa',    '🇺🇬','Low income'),
    ('ET','ETH','Ethiopia',       'East Africa',    '🇪🇹','Low income'),
    ('RW','RWA','Rwanda',         'East Africa',    '🇷🇼','Low income'),
    ('ZA','ZAF','South Africa',   'Southern Africa','🇿🇦','Upper middle income'),
    ('ZM','ZMB','Zambia',         'Southern Africa','🇿🇲','Lower middle income'),
    ('ZW','ZWE','Zimbabwe',       'Southern Africa','🇿🇼','Lower middle income'),
    ('BW','BWA','Botswana',       'Southern Africa','🇧🇼','Upper middle income'),
    ('EG','EGY','Egypt',          'North Africa',   '🇪🇬','Lower middle income'),
    ('MA','MAR','Morocco',        'North Africa',   '🇲🇦','Lower middle income'),
    ('TN','TUN','Tunisia',        'North Africa',   '🇹🇳','Lower middle income'),
    ('US','USA','United States',  'Global Benchmarks','🇺🇸','High income'),
    ('GB','GBR','United Kingdom', 'Global Benchmarks','🇬🇧','High income'),
    ('CN','CHN','China',          'Global Benchmarks','🇨🇳','Upper middle income'),
    ('IN','IND','India',          'Global Benchmarks','🇮🇳','Lower middle income'),
    ('BR','BRA','Brazil',         'Global Benchmarks','🇧🇷','Upper middle income'),
    ('DE','DEU','Germany',        'Global Benchmarks','🇩🇪','High income'),
]

INDICATORS = [
    ('NY.GDP.MKTP.CD',    'GDP (current US$)',                        'USD',      'B',   'economy'),
    ('NY.GDP.MKTP.KD.ZG', 'GDP Growth Rate',                         '%',        'pct', 'economy'),
    ('NY.GDP.PCAP.CD',    'GDP per Capita (current US$)',             'USD',      'num', 'economy'),
    ('FP.CPI.TOTL.ZG',    'Inflation, Consumer Prices',              '%',        'pct', 'economy'),
    ('BN.CAB.XOKA.GD.ZS', 'Current Account Balance (% of GDP)',     '%',        'pct', 'economy'),
    ('GC.DOD.TOTL.GD.ZS', 'Central Government Debt (% of GDP)',     '%',        'pct', 'economy'),
    ('BX.KLT.DINV.WD.GD.ZS','Foreign Direct Investment (% of GDP)','%',        'pct', 'economy'),
    ('NE.EXP.GNFS.ZS',    'Exports of Goods & Services (% of GDP)', '%',        'pct', 'economy'),
    ('SE.PRM.ENRR',       'Primary School Enrollment (% gross)',     '%',        'pct', 'education'),
    ('SE.SEC.ENRR',       'Secondary School Enrollment (% gross)',   '%',        'pct', 'education'),
    ('SE.TER.ENRR',       'Tertiary Enrollment (% gross)',           '%',        'pct', 'education'),
    ('SE.ADT.LITR.ZS',    'Adult Literacy Rate (% ages 15+)',        '%',        'pct', 'education'),
    ('SE.PRM.CMPT.ZS',    'Primary Completion Rate',                 '%',        'pct', 'education'),
    ('SE.XPD.TOTL.GD.ZS', 'Government Education Expenditure (% GDP)','%',       'pct', 'education'),
    ('SE.PRM.TENR',       'Primary Net Enrollment Rate',             '%',        'pct', 'education'),
    ('SP.POP.TOTL',       'Total Population',                        'people',   'M',   'health'),
    ('SP.DYN.LE00.IN',    'Life Expectancy at Birth (years)',        'years',    'dec', 'health'),
    ('SH.DYN.MORT',       'Under-5 Mortality Rate (per 1,000)',      'per 1,000','dec', 'health'),
    ('SH.STA.BRTC.ZS',    'Births Attended by Skilled Staff',       '%',        'pct', 'health'),
    ('SP.DYN.TFRT.IN',    'Fertility Rate (births per woman)',       'births',   'dec', 'health'),
    ('SH.MED.BEDS.ZS',    'Hospital Beds (per 1,000 people)',        'per 1,000','dec', 'health'),
    ('SP.URB.TOTL.IN.ZS', 'Urban Population (% of total)',          '%',        'pct', 'health'),
    ('EN.ATM.CO2E.PC',    'CO2 Emissions (metric tons per capita)', 'mt/cap',   'dec', 'environment'),
    ('EG.ELC.ACCS.ZS',    'Access to Electricity (%)',              '%',        'pct', 'environment'),
    ('EG.FEC.RNEW.ZS',    'Renewable Energy Consumption (%)',       '%',        'pct', 'environment'),
    ('AG.LND.FRST.ZS',    'Forest Area (% of land area)',           '%',        'pct', 'environment'),
    ('ER.H2O.FWTL.ZS',    'Freshwater Withdrawals (%)',             '%',        'pct', 'environment'),
    ('EN.ATM.METH.KT.CE', 'Methane Emissions (kt CO2 equivalent)', 'kt CO2e',  'K',   'environment'),
    ('AG.LND.AGRI.ZS',    'Agricultural Land (% of land area)',     '%',        'pct', 'agriculture'),
    ('NV.AGR.TOTL.ZS',    'Agriculture Value Added (% of GDP)',     '%',        'pct', 'agriculture'),
    ('SL.AGR.EMPL.ZS',    'Employment in Agriculture (%)',          '%',        'pct', 'agriculture'),
    ('AG.YLD.CREL.KG',    'Cereal Yield (kg per hectare)',          'kg/ha',    'num', 'agriculture'),
    ('AG.LND.IRIG.AG.ZS', 'Agricultural Land Irrigated (%)',        '%',        'pct', 'agriculture'),
    ('IC.BUS.EASE.XQ',    'Ease of Doing Business Score',           'score',    'dec', 'business'),
    ('SL.UEM.TOTL.ZS',    'Unemployment Rate (% of labor force)',   '%',        'pct', 'business'),
    ('SL.TLF.ACTI.ZS',    'Labor Force Participation Rate (%)',     '%',        'pct', 'business'),
    ('IC.REG.DURS',       'Time to Start a Business (days)',        'days',     'dec', 'business'),
    ('FB.ATM.TOTL.P5',    'ATMs (per 100,000 adults)',              'per 100k', 'dec', 'business'),
]

print("Seeding sectors...")
for code, name, icon, color, desc in SECTORS:
    cur.execute("""
        INSERT INTO sectors (code, name, icon, color, description)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (code) DO UPDATE
          SET name=EXCLUDED.name, icon=EXCLUDED.icon,
              color=EXCLUDED.color, description=EXCLUDED.description
    """, (code, name, icon, color, desc))

print("Seeding countries...")
for code, iso3, name, region, flag, income in COUNTRIES:
    cur.execute("""
        INSERT INTO countries (code, iso3, name, region, flag, income_level)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (code) DO UPDATE
          SET name=EXCLUDED.name, region=EXCLUDED.region,
              flag=EXCLUDED.flag, income_level=EXCLUDED.income_level
    """, (code, iso3, name, region, flag, income))

print("Seeding indicators...")
for wb_code, name, unit, fmt, sector_code in INDICATORS:
    cur.execute("SELECT id FROM sectors WHERE code=%s", (sector_code,))
    row = cur.fetchone()
    if not row:
        continue
    cur.execute("""
        INSERT INTO indicators (wb_code, name, unit, fmt, sector_id, source)
        VALUES (%s,%s,%s,%s,%s,'World Bank')
        ON CONFLICT (wb_code) DO UPDATE
          SET name=EXCLUDED.name, unit=EXCLUDED.unit, fmt=EXCLUDED.fmt
    """, (wb_code, name, unit, fmt, row[0]))

conn.commit()

# ── Summary ──────────────────────────────────────────────────
cur.execute("SELECT COUNT(*) FROM sectors")
print(f"  Sectors  : {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM countries")
print(f"  Countries: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM indicators")
print(f"  Indicators: {cur.fetchone()[0]}")

cur.close()
conn.close()
print("\nDatabase initialised successfully ✓")
print("Next: run seed_and_fetch.py to pull World Bank data")
