import os,psycopg2
try:
    from dotenv import load_dotenv; load_dotenv()
except: pass
DATABASE_URL=os.getenv('DATABASE_URL')
conn=psycopg2.connect(DATABASE_URL,sslmode='require') if DATABASE_URL else psycopg2.connect(host=os.getenv('DB_HOST','localhost'),port=os.getenv('DB_PORT','5432'),dbname=os.getenv('DB_NAME','sgdatalytics'),user=os.getenv('DB_USER','sgdata'),password=os.getenv('DB_PASSWORD','sgdata2025'))
cur=conn.cursor()
print("Connected ✓")
cur.execute("""
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE TABLE IF NOT EXISTS sectors(id SERIAL PRIMARY KEY,code VARCHAR(32) UNIQUE NOT NULL,name VARCHAR(100) NOT NULL,icon VARCHAR(8),color VARCHAR(10),description TEXT,created_at TIMESTAMP DEFAULT NOW());
CREATE TABLE IF NOT EXISTS countries(id SERIAL PRIMARY KEY,code CHAR(3) UNIQUE NOT NULL,iso3 CHAR(3),name VARCHAR(100) NOT NULL,region VARCHAR(80),flag VARCHAR(8),income_level VARCHAR(50),created_at TIMESTAMP DEFAULT NOW());
CREATE TABLE IF NOT EXISTS indicators(id SERIAL PRIMARY KEY,wb_code VARCHAR(60) UNIQUE NOT NULL,name VARCHAR(200) NOT NULL,unit VARCHAR(60),fmt VARCHAR(10),sector_id INTEGER REFERENCES sectors(id),source VARCHAR(60) DEFAULT 'World Bank',description TEXT,created_at TIMESTAMP DEFAULT NOW());
CREATE TABLE IF NOT EXISTS data_points(id BIGSERIAL PRIMARY KEY,country_id INTEGER NOT NULL REFERENCES countries(id) ON DELETE CASCADE,indicator_id INTEGER NOT NULL REFERENCES indicators(id) ON DELETE CASCADE,year SMALLINT NOT NULL,value NUMERIC(20,6),fetched_at TIMESTAMP DEFAULT NOW(),UNIQUE(country_id,indicator_id,year));
CREATE INDEX IF NOT EXISTS idx_dp_country ON data_points(country_id);
CREATE INDEX IF NOT EXISTS idx_dp_indicator ON data_points(indicator_id);
CREATE INDEX IF NOT EXISTS idx_dp_year ON data_points(year);
CREATE TABLE IF NOT EXISTS fetch_log(id BIGSERIAL PRIMARY KEY,country_id INTEGER REFERENCES countries(id),indicator_id INTEGER REFERENCES indicators(id),year_from SMALLINT,year_to SMALLINT,records_fetched INTEGER,fetched_at TIMESTAMP DEFAULT NOW(),source_url TEXT);
CREATE TABLE IF NOT EXISTS datasets(id SERIAL PRIMARY KEY,uuid UUID DEFAULT uuid_generate_v4() UNIQUE,title VARCHAR(200) NOT NULL,slug VARCHAR(200) UNIQUE,description TEXT,sector_id INTEGER REFERENCES sectors(id),access_level VARCHAR(20) DEFAULT 'starter',file_formats TEXT[],row_count INTEGER,year_from SMALLINT,year_to SMALLINT,price_ghs NUMERIC(10,2),is_published BOOLEAN DEFAULT FALSE,featured BOOLEAN DEFAULT FALSE,download_count INTEGER DEFAULT 0,created_at TIMESTAMP DEFAULT NOW(),updated_at TIMESTAMP DEFAULT NOW());
CREATE TABLE IF NOT EXISTS users(id SERIAL PRIMARY KEY,uuid UUID DEFAULT uuid_generate_v4() UNIQUE,email VARCHAR(200) UNIQUE NOT NULL,name VARCHAR(100),organisation VARCHAR(200),role VARCHAR(50),plan VARCHAR(20) DEFAULT 'free',plan_expires DATE,country_code CHAR(3),password_hash TEXT,is_active BOOLEAN DEFAULT TRUE,created_at TIMESTAMP DEFAULT NOW(),last_login TIMESTAMP);
CREATE TABLE IF NOT EXISTS downloads(id BIGSERIAL PRIMARY KEY,user_id INTEGER REFERENCES users(id),dataset_id INTEGER REFERENCES datasets(id),format VARCHAR(10),downloaded_at TIMESTAMP DEFAULT NOW(),ip_address INET);
""")
conn.commit()
print("Tables created ✓")
for code,name,icon,color,desc in [('economy','Economy & Finance','💹','#a78bfa','Macro data'),('education','Education','🏫','#60a5fa','Education data'),('health','Health & Demographics','🏥','#f87171','Health data'),('environment','Sustainability','🌿','#34d399','Environment data'),('agriculture','Agriculture','🌾','#fbbf24','Agriculture data'),('business','Business & SMEs','📊','#f59e0b','Business data')]:
    cur.execute("INSERT INTO sectors(code,name,icon,color,description) VALUES(%s,%s,%s,%s,%s) ON CONFLICT(code) DO UPDATE SET name=EXCLUDED.name",(code,name,icon,color,desc))
for code,iso3,name,region,flag,income in [('GH','GHA','Ghana','West Africa','🇬🇭','Lower middle income'),('NG','NGA','Nigeria','West Africa','🇳🇬','Lower middle income'),('KE','KEN','Kenya','East Africa','🇰🇪','Lower middle income'),('ZA','ZAF','South Africa','Southern Africa','🇿🇦','Upper middle income'),('ET','ETH','Ethiopia','East Africa','🇪🇹','Low income'),('US','USA','United States','Global Benchmarks','🇺🇸','High income'),('IN','IND','India','Global Benchmarks','🇮🇳','Lower middle income'),('GB','GBR','United Kingdom','Global Benchmarks','🇬🇧','High income')]:
    cur.execute("INSERT INTO countries(code,iso3,name,region,flag,income_level) VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(code) DO UPDATE SET name=EXCLUDED.name",(code,iso3,name,region,flag,income))
conn.commit()
cur.execute("SELECT TRIM(code),id FROM countries"); cm=dict(cur.fetchall())
for wb_code,name,unit,fmt,sector_code in [('NY.GDP.MKTP.CD','GDP (current US$)','USD','B','economy'),('NY.GDP.MKTP.KD.ZG','GDP Growth Rate','%','pct','economy'),('FP.CPI.TOTL.ZG','Inflation','%','pct','economy'),('SP.POP.TOTL','Total Population','people','M','health'),('SP.DYN.LE00.IN','Life Expectancy','years','dec','health'),('SH.DYN.MORT','Under-5 Mortality','per 1,000','dec','health'),('SE.PRM.ENRR','Primary Enrollment','%','pct','education'),('SE.ADT.LITR.ZS','Adult Literacy','%','pct','education'),('EG.ELC.ACCS.ZS','Electricity Access','%','pct','environment'),('EN.ATM.CO2E.PC','CO2 per capita','mt/cap','dec','environment'),('AG.LND.AGRI.ZS','Agricultural Land','%','pct','agriculture'),('SL.UEM.TOTL.ZS','Unemployment','%','pct','business')]:
    cur.execute("SELECT id FROM sectors WHERE code=%s",(sector_code,)); row=cur.fetchone()
    if row: cur.execute("INSERT INTO indicators(wb_code,name,unit,fmt,sector_id,source) VALUES(%s,%s,%s,%s,%s,'World Bank') ON CONFLICT(wb_code) DO UPDATE SET name=EXCLUDED.name",(wb_code,name,unit,fmt,row[0]))
conn.commit()
cur.execute("SELECT wb_code,id FROM indicators"); im=dict(cur.fetchall())
DATA={'NY.GDP.MKTP.CD':{'GH':{2015:37.54e9,2016:52.37e9,2017:59.02e9,2018:65.56e9,2019:66.98e9,2020:70.87e9,2021:77.59e9,2022:76.37e9,2023:76.34e9},'NG':{2015:481.1e9,2016:404.7e9,2017:376.7e9,2018:421.3e9,2019:448.1e9,2020:432.3e9,2021:440.8e9,2022:477.4e9,2023:506.0e9},'KE':{2015:63.8e9,2016:70.5e9,2017:79.5e9,2018:87.9e9,2019:95.5e9,2020:98.8e9,2021:110.3e9,2022:118.1e9,2023:107.4e9},'ZA':{2015:317.6e9,2016:295.0e9,2017:349.4e9,2018:368.3e9,2019:351.4e9,2020:301.9e9,2021:419.0e9,2022:405.9e9,2023:377.8e9},'US':{2015:18.22e12,2016:18.71e12,2017:19.48e12,2018:20.53e12,2019:21.43e12,2020:20.89e12,2021:23.32e12,2022:25.46e12,2023:27.36e12}},'FP.CPI.TOTL.ZG':{'GH':{2015:17.2,2016:17.5,2017:12.4,2018:9.8,2019:7.1,2020:9.9,2021:10.0,2022:31.9,2023:38.1},'NG':{2015:9.0,2016:15.7,2017:16.5,2018:12.1,2019:11.4,2020:13.2,2021:17.0,2022:18.8,2023:24.7},'KE':{2015:6.6,2016:6.3,2017:8.0,2018:4.7,2019:5.2,2020:5.4,2021:6.1,2022:7.7,2023:7.7},'ZA':{2015:4.6,2016:6.3,2017:5.3,2018:4.6,2019:4.1,2020:3.3,2021:4.5,2022:6.9,2023:6.1},'US':{2015:0.1,2016:1.3,2017:2.1,2018:2.4,2019:1.8,2020:1.2,2021:4.7,2022:8.0,2023:4.1}},'SP.POP.TOTL':{'GH':{2015:27.97e6,2016:28.74e6,2017:29.50e6,2018:30.28e6,2019:31.07e6,2020:31.86e6,2021:32.64e6,2022:33.47e6,2023:34.12e6},'NG':{2015:185.9e6,2016:191.8e6,2017:197.9e6,2018:204.0e6,2019:210.4e6,2020:217.0e6,2021:223.8e6,2022:220.9e6,2023:229.6e6},'KE':{2015:48.5e6,2016:49.7e6,2017:50.9e6,2018:52.2e6,2019:53.5e6,2020:54.9e6,2021:56.1e6,2022:54.0e6,2023:55.1e6},'ZA':{2015:55.4e6,2016:56.2e6,2017:57.0e6,2018:57.8e6,2019:58.6e6,2020:59.3e6,2021:60.0e6,2022:60.6e6,2023:61.0e6},'US':{2015:320.7e6,2016:323.1e6,2017:325.1e6,2018:326.9e6,2019:328.3e6,2020:329.5e6,2021:331.9e6,2022:333.3e6,2023:334.9e6}},'SP.DYN.LE00.IN':{'GH':{2015:63.5,2016:63.8,2017:64.1,2018:64.4,2019:64.6,2020:64.1,2021:63.8,2022:64.3,2023:64.7},'NG':{2015:53.5,2016:53.9,2017:54.2,2018:54.5,2019:54.7,2020:54.4,2021:54.0,2022:54.3,2023:54.6},'KE':{2015:64.3,2016:64.9,2017:65.5,2018:66.0,2019:66.5,2020:66.3,2021:65.8,2022:66.2,2023:66.6},'ZA':{2015:61.7,2016:62.5,2017:63.2,2018:63.9,2019:64.1,2020:63.3,2021:62.4,2022:63.1,2023:63.9},'US':{2015:78.7,2016:78.6,2017:78.5,2018:78.6,2019:78.8,2020:77.0,2021:76.1,2022:76.4,2023:77.5}},'EG.ELC.ACCS.ZS':{'GH':{2015:75.0,2016:77.3,2017:79.3,2018:82.5,2019:84.5,2020:85.9,2021:87.3,2022:88.0,2023:88.6},'NG':{2015:55.5,2016:58.4,2017:59.3,2018:60.5,2019:55.4,2020:57.5,2021:58.5,2022:60.1,2023:62.0},'KE':{2015:47.1,2016:56.0,2017:63.0,2018:70.0,2019:75.0,2020:76.0,2021:74.7,2022:76.0,2023:77.5},'ET':{2015:30.0,2016:35.0,2017:40.7,2018:44.3,2019:45.0,2020:45.3,2021:44.3,2022:48.0,2023:52.0},'ZA':{2015:88.4,2016:84.4,2017:85.3,2018:84.2,2019:84.6,2020:84.5,2021:85.9,2022:86.0,2023:86.3}},'SL.UEM.TOTL.ZS':{'GH':{2015:5.3,2016:5.3,2017:5.2,2018:5.1,2019:4.9,2020:5.4,2021:5.4,2022:5.3,2023:5.2},'NG':{2015:9.0,2016:13.4,2017:16.5,2018:22.7,2019:23.1,2020:27.1,2021:33.3,2022:37.7,2023:34.5},'ZA':{2015:25.3,2016:26.7,2017:27.5,2018:27.1,2019:28.7,2020:29.2,2021:34.3,2022:33.5,2023:32.9},'US':{2015:5.3,2016:4.9,2017:4.4,2018:3.9,2019:3.7,2020:8.1,2021:5.4,2022:3.6,2023:3.6}}}
total=0
for ind_code,cdata in DATA.items():
    ind_id=im.get(ind_code)
    if not ind_id: continue
    for c_code,ydata in cdata.items():
        c_id=cm.get(c_code)
        if not c_id: continue
        for year,value in ydata.items():
            cur.execute("INSERT INTO data_points(country_id,indicator_id,year,value) VALUES(%s,%s,%s,%s) ON CONFLICT(country_id,indicator_id,year) DO UPDATE SET value=EXCLUDED.value",(c_id,ind_id,year,value)); total+=1
conn.commit()
cur.execute("SELECT COUNT(*) FROM data_points"); print(f"Data points: {cur.fetchone()[0]} ✓")
cur.close(); conn.close(); print("Done ✓")
