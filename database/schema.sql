-- ============================================================
--  SG DATALYTICS — PostgreSQL Schema
--  Data Warehouse for Ghana & Multi-Country Indicators
-- ============================================================

-- EXTENSIONS
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ────────────────────────────────────────────────
-- 1. SECTORS
-- ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sectors (
    id          SERIAL PRIMARY KEY,
    code        VARCHAR(32)  UNIQUE NOT NULL,
    name        VARCHAR(100) NOT NULL,
    icon        VARCHAR(8),
    color       VARCHAR(10),
    description TEXT,
    created_at  TIMESTAMP DEFAULT NOW()
);

-- ────────────────────────────────────────────────
-- 2. COUNTRIES
-- ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS countries (
    id          SERIAL PRIMARY KEY,
    code        CHAR(3)      UNIQUE NOT NULL,   -- ISO 3166-1 alpha-2 (GH, NG…)
    iso3        CHAR(3),                        -- alpha-3 (GHA, NGA…)
    name        VARCHAR(100) NOT NULL,
    region      VARCHAR(80),
    flag        VARCHAR(8),
    income_level VARCHAR(50),
    created_at  TIMESTAMP DEFAULT NOW()
);

-- ────────────────────────────────────────────────
-- 3. INDICATORS
-- ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS indicators (
    id          SERIAL PRIMARY KEY,
    wb_code     VARCHAR(60)  UNIQUE NOT NULL,   -- World Bank code e.g. NY.GDP.MKTP.CD
    name        VARCHAR(200) NOT NULL,
    unit        VARCHAR(60),
    fmt         VARCHAR(10),                    -- B, M, K, pct, dec, num
    sector_id   INTEGER REFERENCES sectors(id),
    source      VARCHAR(60)  DEFAULT 'World Bank',
    description TEXT,
    created_at  TIMESTAMP DEFAULT NOW()
);

-- ────────────────────────────────────────────────
-- 4. DATA POINTS  (the core warehouse table)
-- ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS data_points (
    id            BIGSERIAL PRIMARY KEY,
    country_id    INTEGER NOT NULL REFERENCES countries(id) ON DELETE CASCADE,
    indicator_id  INTEGER NOT NULL REFERENCES indicators(id) ON DELETE CASCADE,
    year          SMALLINT NOT NULL,
    value         NUMERIC(20, 6),
    fetched_at    TIMESTAMP DEFAULT NOW(),
    UNIQUE (country_id, indicator_id, year)
);

-- Index for fast queries
CREATE INDEX IF NOT EXISTS idx_dp_country    ON data_points(country_id);
CREATE INDEX IF NOT EXISTS idx_dp_indicator  ON data_points(indicator_id);
CREATE INDEX IF NOT EXISTS idx_dp_year       ON data_points(year);
CREATE INDEX IF NOT EXISTS idx_dp_country_indicator ON data_points(country_id, indicator_id);

-- ────────────────────────────────────────────────
-- 5. CACHE LOG  (tracks what was fetched & when)
-- ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fetch_log (
    id           BIGSERIAL PRIMARY KEY,
    country_id   INTEGER REFERENCES countries(id),
    indicator_id INTEGER REFERENCES indicators(id),
    year_from    SMALLINT,
    year_to      SMALLINT,
    records_fetched INTEGER,
    fetched_at   TIMESTAMP DEFAULT NOW(),
    source_url   TEXT
);

-- ────────────────────────────────────────────────
-- 6. DATASETS  (packaged datasets for sale)
-- ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS datasets (
    id           SERIAL PRIMARY KEY,
    uuid         UUID DEFAULT uuid_generate_v4() UNIQUE,
    title        VARCHAR(200) NOT NULL,
    slug         VARCHAR(200) UNIQUE,
    description  TEXT,
    sector_id    INTEGER REFERENCES sectors(id),
    access_level VARCHAR(20) DEFAULT 'starter',  -- free, starter, pro, enterprise
    file_formats TEXT[],                          -- {CSV, JSON, XLSX}
    row_count    INTEGER,
    year_from    SMALLINT,
    year_to      SMALLINT,
    price_ghs    NUMERIC(10,2),
    is_published BOOLEAN DEFAULT FALSE,
    featured     BOOLEAN DEFAULT FALSE,
    download_count INTEGER DEFAULT 0,
    created_at   TIMESTAMP DEFAULT NOW(),
    updated_at   TIMESTAMP DEFAULT NOW()
);

-- ────────────────────────────────────────────────
-- 7. USERS  (subscribers)
-- ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
    id            SERIAL PRIMARY KEY,
    uuid          UUID DEFAULT uuid_generate_v4() UNIQUE,
    email         VARCHAR(200) UNIQUE NOT NULL,
    name          VARCHAR(100),
    organisation  VARCHAR(200),
    role          VARCHAR(50),                  -- researcher, analyst, government, student
    plan          VARCHAR(20) DEFAULT 'free',   -- free, starter, pro, enterprise
    plan_expires  DATE,
    country_code  CHAR(3),
    password_hash TEXT,
    is_active     BOOLEAN DEFAULT TRUE,
    created_at    TIMESTAMP DEFAULT NOW(),
    last_login    TIMESTAMP
);

-- ────────────────────────────────────────────────
-- 8. DOWNLOADS  (audit trail)
-- ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS downloads (
    id          BIGSERIAL PRIMARY KEY,
    user_id     INTEGER REFERENCES users(id),
    dataset_id  INTEGER REFERENCES datasets(id),
    format      VARCHAR(10),
    downloaded_at TIMESTAMP DEFAULT NOW(),
    ip_address  INET
);

