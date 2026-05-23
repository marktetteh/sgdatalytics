"""
SG Datalytics — SGMPI REST API v2
Connects to 6 Neon databases serving real Ghana market price data.

Run locally : python3 api.py
Run on Railway: gunicorn -w 2 -b 0.0.0.0:$PORT api:app
"""
import os, csv, io, hmac, hashlib, secrets
from datetime import datetime, timedelta, timezone
from flask import Flask, jsonify, request, Response, stream_with_context
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import psycopg2, psycopg2.extras

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

app = Flask(__name__)
CORS(app)

# ── RATE LIMITER ─────────────────────────────────────────────
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["200 per hour"],
    storage_uri="memory://",
)

# ── DB CONNECTIONS ───────────────────────────────────────────
DB = {
    'market_prices':  os.getenv('NEON_MARKET_PRICES'),
    'accommodation':  os.getenv('NEON_ACCOMMODATION'),
    'property':       os.getenv('NEON_PROPERTY'),
    'economic':       os.getenv('NEON_ECONOMIC'),
    'commodities':    os.getenv('NEON_COMMODITIES'),
    'financials':     os.getenv('NEON_FINANCIALS'),
}

def get_conn(db_key):
    url = DB.get(db_key)
    if not url:
        raise Exception(f'No connection string for {db_key}')
    return psycopg2.connect(url, sslmode='require')

def query(db_key, sql, params=None, one=False):
    conn = get_conn(db_key)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql, params or [])
    result = cur.fetchone() if one else cur.fetchall()
    cur.close(); conn.close()
    return result

# ── TOKEN STORE (in-memory) ──────────────────────────────────
# { token: { email, sector, created_at, expires_at, used } }
# Note: resets on Railway restart — acceptable for 24hr tokens
_TOKEN_STORE = {}

# ── SECTOR CONFIG ────────────────────────────────────────────
SECTOR_LABELS = {
    'market_prices': 'Market Prices',
    'accommodation': 'Real Estate & Accommodation',
    'economic':      'Economic, Financial & Agricultural Data',
    'bundle':        'Ghana Complete Data Bundle',
}

# Bundle now only needs 3 download links — all 8 tables are covered
BUNDLE_SECTORS = ['market_prices', 'accommodation', 'economic']

# Exact product key → sector mapping (used with Paystack metadata.product)
PRODUCT_SECTOR_MAP = {
    'market_prices': 'market_prices',
    'accommodation': 'accommodation',
    'economic':      'economic',
    'bundle':        'bundle',
}

# Each entry: (db_key, sql, table_label)
# table_label is written as a section header in the CSV so analysts know where each table starts
SECTOR_QUERIES = {
    # ── Consumer Market Prices ───────────────────────────────────
    # Aggregated like Numbeo: one row per product per city per week
    # Outliers filtered: price_ghs between GHS 1 and GHS 200,000
    'market_prices': [(
        'market_prices', """
        SELECT
            collected_date,
            week_number,
            year,
            product_category,
            COALESCE(NULLIF(normalized_name, ''), search_label)   AS normalized_name,
            COALESCE(NULLIF(location, ''), 'Ghana')               AS city,
            COUNT(*)                                              AS listing_count,
            ROUND(AVG(price_ghs)::numeric,    0)                  AS avg_price_ghs,
            MIN(price_ghs)                                        AS min_price_ghs,
            MAX(price_ghs)                                        AS max_price_ghs,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
                  (ORDER BY price_ghs)::numeric, 0)               AS median_price_ghs
        FROM market_prices
        WHERE price_ghs > 1
          AND price_ghs < 200000
          AND (normalized_name IS NOT NULL AND normalized_name <> ''
               OR search_label IS NOT NULL)
        GROUP BY collected_date, week_number, year,
                 product_category,
                 COALESCE(NULLIF(normalized_name, ''), search_label),
                 COALESCE(NULLIF(location, ''), 'Ghana')
        ORDER BY collected_date DESC, product_category, normalized_name
        """, 'market_prices_aggregated',
    )],

    # ── Real Estate & Accommodation ──────────────────────────────
    'accommodation': [
        ('accommodation', """
        SELECT
            collected_date,
            week_number,
            year,
            'Hotel'                                        AS product_category,
            city || ' — ' || COALESCE(star_rating::text, '?') || '-star hotel'
                                                           AS normalized_name,
            city,
            COALESCE(star_rating::text, 'Unknown')         AS star_rating,
            COUNT(*)                                        AS hotel_count,
            ROUND(AVG(price_per_night_usd)::numeric, 2)    AS avg_price_per_night_usd,
            MIN(price_per_night_usd)                        AS min_price_per_night_usd,
            MAX(price_per_night_usd)                        AS max_price_per_night_usd,
            ROUND(AVG(review_score)::numeric, 1)            AS avg_review_score,
            source_platform
        FROM hotel_prices
        WHERE price_per_night_usd > 0
        GROUP BY collected_date, week_number, year, city, star_rating, source_platform
        ORDER BY collected_date DESC, city, star_rating
        """, 'hotel_prices_aggregated'),

        ('accommodation', """
        SELECT
            collected_date,
            week_number,
            year,
            'Airbnb'                                       AS product_category,
            city || ' — ' || room_type                     AS normalized_name,
            city,
            room_type,
            COUNT(*)                                        AS listing_count,
            ROUND(AVG(price_ghs)::numeric, 0)              AS avg_price_ghs,
            MIN(price_ghs)                                  AS min_price_ghs,
            MAX(price_ghs)                                  AS max_price_ghs,
            ROUND(AVG(rating)::numeric, 1)                  AS avg_rating
        FROM airbnb_prices
        WHERE price_ghs > 0
        GROUP BY collected_date, week_number, year, city, room_type
        ORDER BY collected_date DESC, city, room_type
        """, 'airbnb_prices_aggregated'),

        ('property', """
        SELECT
            collected_date,
            week_number,
            year,
            'Property'                                     AS product_category,
            city || ' — ' || property_type
              || CASE WHEN bedrooms IS NOT NULL
                      THEN ' ' || bedrooms || 'BR' ELSE '' END
                                                           AS normalized_name,
            property_type,
            listing_type,
            city,
            neighborhood,
            bedrooms,
            COUNT(*)                                        AS listing_count,
            ROUND(AVG(price_ghs)::numeric, 0)              AS avg_price_ghs,
            MIN(price_ghs)                                  AS min_price_ghs,
            MAX(price_ghs)                                  AS max_price_ghs
        FROM property_prices
        WHERE price_ghs > 0
        GROUP BY collected_date, week_number, year,
                 property_type, listing_type, city, neighborhood, bedrooms
        ORDER BY collected_date DESC, city, property_type, bedrooms
        """, 'property_prices_aggregated'),
    ],

    # ── Economic, Financial & Agricultural ───────────────────────
    'economic': [
        ('economic', """
        SELECT
            collected_date,
            year,
            month,
            sector                 AS product_category,
            indicator_name         AS normalized_name,
            indicator_code,
            value,
            unit,
            source
        FROM economic_indicators
        ORDER BY collected_date DESC, sector, indicator_name
        """, 'economic_indicators'),

        ('economic', """
        SELECT
            collected_date,
            'Foreign Exchange'     AS product_category,
            currency_pair          AS normalized_name,
            currency_pair,
            rate_ghs,
            source
        FROM exchange_rates
        ORDER BY collected_date DESC, currency_pair
        """, 'exchange_rates'),

        ('financials', """
        SELECT
            collected_date,
            week_number,
            year,
            'Financial Markets'    AS product_category,
            index_name             AS normalized_name,
            index_name,
            value,
            change_points,
            change_pct,
            source
        FROM gse_indices
        ORDER BY collected_date DESC, index_name
        """, 'gse_indices'),

        ('financials', """
        SELECT
            collected_date,
            week_number,
            year,
            'Financial Markets'    AS product_category,
            company_name || ' (' || symbol || ')'
                                   AS normalized_name,
            symbol,
            company_name,
            opening_price_ghs,
            closing_price_ghs,
            change_ghs,
            change_pct,
            volume,
            year_high,
            year_low,
            source
        FROM stock_prices
        ORDER BY collected_date DESC, symbol
        """, 'stock_prices'),

        ('commodities', """
        SELECT
            collected_date,
            week_number,
            year,
            'Agricultural Commodities'   AS product_category,
            commodity_name || ' — ' || market
                                         AS normalized_name,
            commodity_name,
            market,
            region,
            price_ghs,
            unit,
            source
        FROM commodity_prices
        ORDER BY collected_date DESC, commodity_name, market
        """, 'commodity_prices'),

        ('commodities', """
        SELECT
            collected_date,
            week_number,
            year,
            'Energy & Fuel'        AS product_category,
            fuel_type              AS normalized_name,
            fuel_type,
            price_ghs_per_litre,
            currency,
            source
        FROM fuel_prices
        ORDER BY collected_date DESC, fuel_type
        """, 'fuel_prices'),
    ],
}

# Maps Paystack plan name keywords → sector codes
PLAN_SECTOR_MAP = {
    'market':        'market_prices',
    'property':      'property',
    'accommodation': 'accommodation',
    'hotel':         'accommodation',
    'airbnb':        'accommodation',
    'economic':      'economic',
    'commodit':      'economic',
    'agricultur':    'economic',
    'financial':     'economic',
    'fuel':          'commodities',
    'financial':     'financials',
    'stock':         'financials',
    'gse':           'financials',
}

def resolve_sector(plan_name):
    """Map a Paystack plan name to a sector code using keyword matching."""
    name_lower = (plan_name or '').lower()
    for keyword, sector in PLAN_SECTOR_MAP.items():
        if keyword in name_lower:
            return sector
    return 'market_prices'  # safe default

# ═══════════════════════════════════════════════════════════════
# PART 2 — TOKEN GENERATION
# ═══════════════════════════════════════════════════════════════
def generate_download_token(email, sector, expires_hours=24):
    """
    Generate a secure one-time download token.
    Stores it in memory and returns the full download URL.
    """
    token      = secrets.token_urlsafe(32)
    now        = datetime.now(timezone.utc)
    expires_at = now + timedelta(hours=expires_hours)

    _TOKEN_STORE[token] = {
        'email':      email,
        'sector':     sector,
        'created_at': now,
        'expires_at': expires_at,
        'used':       False,
    }

    download_url = f"https://sgdatalytics-production.up.railway.app/api/download?token={token}"
    print(f"[TOKEN] Generated for {email} | sector={sector} | expires={expires_at.isoformat()}")
    return download_url

# ═══════════════════════════════════════════════════════════════
# PART 3 — EMAIL SENDING
# ═══════════════════════════════════════════════════════════════
def send_download_email(email, download_url, sector):
    """
    Send a professional HTML email with the one-time download link.
    Uses Resend HTTP API (RESEND_API_KEY env var) — no SMTP, Railway-safe.
    """
    import requests as req
    api_key = os.getenv('RESEND_API_KEY', '').strip()
    if not api_key:
        raise Exception('RESEND_API_KEY not configured')

    sector_label = SECTOR_LABELS.get(sector, sector.replace('_', ' ').title())

    html = f"""<!DOCTYPE html>
<html>
<body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0">
  <tr><td align="center" style="padding:40px 20px;">
    <table width="600" style="background:#ffffff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08);">
      <tr><td style="background:#00957a;padding:32px;text-align:center;">
        <h1 style="color:#ffffff;margin:0;font-size:26px;letter-spacing:-0.5px;">SG Datalytics</h1>
        <p style="color:#d0f0e8;margin:8px 0 0;font-size:14px;">Ghana Market Intelligence Platform</p>
      </td></tr>
      <tr><td style="padding:40px;">
        <h2 style="color:#1a1a1a;margin:0 0 12px;font-size:20px;">Your data is ready ✓</h2>
        <p style="color:#555;line-height:1.7;margin:0 0 24px;">
          Thank you for your purchase. Your <strong>{sector_label}</strong> dataset
          has been prepared and is ready to download as a CSV file.
        </p>
        <div style="text-align:center;margin:32px 0;">
          <a href="{download_url}"
             style="background:#00957a;color:#ffffff;padding:16px 48px;border-radius:6px;
                    text-decoration:none;font-size:16px;font-weight:bold;display:inline-block;">
            ⬇ Download Your Data
          </a>
        </div>
        <div style="background:#fff8e1;border-left:4px solid #f59e0b;padding:16px 20px;
                    border-radius:4px;margin:24px 0;">
          <p style="margin:0;color:#92400e;font-size:14px;line-height:1.6;">
            ⚠️ <strong>Important:</strong> This link expires in <strong>24 hours</strong>
            and can only be used <strong>once</strong>. Please download your file immediately.
          </p>
        </div>
        <p style="color:#777;font-size:13px;line-height:1.6;">
          If the button doesn't work, copy and paste this link into your browser:<br>
          <a href="{download_url}" style="color:#00957a;word-break:break-all;">{download_url}</a>
        </p>
        <hr style="border:none;border-top:1px solid #eee;margin:28px 0;">
        <p style="color:#999;font-size:12px;margin:0;">
          Questions? <a href="mailto:data@sgdatalytics.org" style="color:#00957a;">data@sgdatalytics.org</a>
        </p>
      </td></tr>
      <tr><td style="background:#f9f9f9;padding:20px;text-align:center;border-top:1px solid #eee;">
        <p style="color:#bbb;font-size:12px;margin:0;">
          &copy; 2026 SG Datalytics &nbsp;|&nbsp;
          <a href="https://sgdatalytics.org" style="color:#00957a;text-decoration:none;">sgdatalytics.org</a>
        </p>
      </td></tr>
    </table>
  </td></tr>
</table>
</body>
</html>"""

    resp = req.post(
        'https://api.resend.com/emails',
        headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'},
        json={
            'from':    'SG Datalytics <data@sgdatalytics.org>',
            'to':      [email],
            'subject': f'Your SG Datalytics Data is Ready — {sector_label}',
            'html':    html,
        },
        timeout=15,
    )
    if resp.status_code not in (200, 201):
        raise Exception(f'Resend API error {resp.status_code}: {resp.text[:200]}')

    print(f"[EMAIL] Sent via Resend to {email} | sector={sector}")


def send_bundle_email(email, sectors):
    """
    Send one email containing separate download links for each sector in the bundle.
    Called when a customer purchases the 'Ghana Complete Data Bundle'.
    """
    import requests as req
    api_key = os.getenv('RESEND_API_KEY', '').strip()
    if not api_key:
        raise Exception('RESEND_API_KEY not configured')

    # Generate a token for each sector
    links_html = ''
    for sector in sectors:
        url   = generate_download_token(email, sector)
        label = SECTOR_LABELS.get(sector, sector.replace('_', ' ').title())
        links_html += f"""
        <tr>
          <td style="padding:14px 0;border-bottom:1px solid #eee;">
            <strong style="color:#1a1a1a;">{label}</strong><br>
            <a href="{url}"
               style="display:inline-block;margin-top:8px;background:#00957a;color:#fff;
                      padding:10px 28px;border-radius:5px;text-decoration:none;font-size:14px;font-weight:bold;">
              ⬇ Download
            </a>
          </td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html>
<body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0">
  <tr><td align="center" style="padding:40px 20px;">
    <table width="620" style="background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08);">
      <tr><td style="background:#00957a;padding:32px;text-align:center;">
        <h1 style="color:#fff;margin:0;font-size:26px;">SG Datalytics</h1>
        <p style="color:#d0f0e8;margin:8px 0 0;font-size:14px;">Ghana Market Intelligence Platform</p>
      </td></tr>
      <tr><td style="padding:40px;">
        <h2 style="color:#1a1a1a;margin:0 0 12px;font-size:20px;">Your Complete Bundle is Ready ✓</h2>
        <p style="color:#555;line-height:1.7;margin:0 0 24px;">
          Thank you for purchasing the <strong>Ghana Complete Data Bundle</strong>.
          Below are your individual download links — one per dataset.
        </p>
        <div style="background:#fff8e1;border-left:4px solid #f59e0b;padding:14px 18px;
                    border-radius:4px;margin:0 0 24px;">
          <p style="margin:0;color:#92400e;font-size:13px;line-height:1.6;">
            ⚠️ Each link is <strong>one-time use</strong> and expires in <strong>24 hours</strong>.
            Download all files now.
          </p>
        </div>
        <table width="100%" cellpadding="0" cellspacing="0">
          {links_html}
        </table>
        <hr style="border:none;border-top:1px solid #eee;margin:28px 0;">
        <p style="color:#999;font-size:12px;margin:0;">
          Questions? <a href="mailto:data@sgdatalytics.org" style="color:#00957a;">data@sgdatalytics.org</a>
        </p>
      </td></tr>
      <tr><td style="background:#f9f9f9;padding:20px;text-align:center;border-top:1px solid #eee;">
        <p style="color:#bbb;font-size:12px;margin:0;">
          &copy; 2026 SG Datalytics &nbsp;|&nbsp;
          <a href="https://sgdatalytics.org" style="color:#00957a;text-decoration:none;">sgdatalytics.org</a>
        </p>
      </td></tr>
    </table>
  </td></tr>
</table>
</body>
</html>"""

    resp = req.post(
        'https://api.resend.com/emails',
        headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'},
        json={
            'from':    'SG Datalytics <data@sgdatalytics.org>',
            'to':      [email],
            'subject': 'Your SG Datalytics Complete Bundle — 4 Download Links Inside',
            'html':    html,
        },
        timeout=15,
    )
    if resp.status_code not in (200, 201):
        raise Exception(f'Resend API error {resp.status_code}: {resp.text[:200]}')

    print(f"[EMAIL] Bundle sent via Resend to {email} | {len(sectors)} sectors")


# ═══════════════════════════════════════════════════════════════
# EXISTING ROUTES (unchanged)
# ═══════════════════════════════════════════════════════════════

@app.route('/')
def root():
    return jsonify({
        "name": "SG Datalytics SGMPI API",
        "version": "2.0.0",
        "description": "Real Ghana market price data across 6 sectors",
        "endpoints": [
            "GET /api/health",
            "GET /api/stats",
            "GET /api/sectors",
            "GET /api/market-prices?category=&location=&limit=50",
            "GET /api/market-prices/categories",
            "GET /api/market-prices/latest",
            "GET /api/property?location=&limit=50",
            "GET /api/accommodation?type=hotel|airbnb&limit=50",
            "GET /api/economic?indicator=&sector=&limit=50",
            "GET /api/economic/indicators",
            "GET /api/commodities?limit=50",
            "GET /api/financials?limit=50",
            "POST /api/webhook/paystack",
            "GET  /api/download?token=TOKEN",
            "POST /api/test-delivery",
        ]
    })

@app.route('/api/health')
def health():
    results = {}
    total   = 0
    for key in DB:
        try:
            r = query(key, "SELECT COUNT(*) AS n FROM " + {
                'market_prices': 'market_prices',
                'accommodation': 'hotel_prices',
                'property':      'property_prices',
                'economic':      'economic_indicators',
                'commodities':   'commodity_prices',
                'financials':    'stock_prices',
            }[key], one=True)
            results[key] = int(r['n'])
            total += int(r['n'])
        except Exception as e:
            results[key] = f'error: {str(e)[:60]}'
    return jsonify({"status": "ok", "total_records": total, "databases": results})

@app.route('/api/stats')
def stats():
    stats = {}
    try:
        r = query('market_prices', "SELECT COUNT(*) AS n, COUNT(DISTINCT product_category) AS cats, COUNT(DISTINCT location) AS locs, MIN(collected_date) AS date_min, MAX(collected_date) AS date_max FROM market_prices", one=True)
        stats['market_prices'] = {'records': int(r['n']), 'categories': int(r['cats']), 'locations': int(r['locs']), 'date_min': str(r['date_min']), 'date_max': str(r['date_max'])}
    except: stats['market_prices'] = {'records': 0}
    try:
        r = query('property', "SELECT COUNT(*) AS n, COUNT(DISTINCT location) AS locs FROM property_prices", one=True)
        stats['property'] = {'records': int(r['n']), 'locations': int(r['locs'])}
    except: stats['property'] = {'records': 0}
    try:
        h = query('accommodation', "SELECT COUNT(*) AS n FROM hotel_prices", one=True)
        a = query('accommodation', "SELECT COUNT(*) AS n FROM airbnb_prices", one=True)
        stats['accommodation'] = {'hotel_records': int(h['n']), 'airbnb_records': int(a['n'])}
    except: stats['accommodation'] = {'records': 0}
    try:
        r = query('economic', "SELECT COUNT(*) AS n, COUNT(DISTINCT indicator_name) AS inds, COUNT(DISTINCT sector) AS secs FROM economic_indicators", one=True)
        stats['economic'] = {'records': int(r['n']), 'indicators': int(r['inds']), 'sectors': int(r['secs'])}
    except: stats['economic'] = {'records': 0}
    try:
        r = query('commodities', "SELECT COUNT(*) AS n FROM commodity_prices", one=True)
        f = query('commodities', "SELECT COUNT(*) AS n FROM fuel_prices", one=True)
        stats['commodities'] = {'commodity_records': int(r['n']), 'fuel_records': int(f['n'])}
    except: stats['commodities'] = {'records': 0}
    try:
        r = query('financials', "SELECT COUNT(*) AS n FROM stock_prices", one=True)
        stats['financials'] = {'records': int(r['n'])}
    except: stats['financials'] = {'records': 0}

    total = sum(v.get('records', 0) + v.get('hotel_records', 0) + v.get('airbnb_records', 0) +
                v.get('commodity_records', 0) + v.get('fuel_records', 0)
                for v in stats.values())
    return jsonify({"total_records": total, "total_sectors": 6, "sectors": stats})

@app.route('/api/sectors')
def sectors():
    return jsonify([
        {"id": 1, "code": "market_prices",  "name": "Market Prices",  "icon": "🛒", "color": "#00957a", "description": "Consumer goods & electronics"},
        {"id": 2, "code": "property",       "name": "Property",       "icon": "🏠", "color": "#2563eb", "description": "Real estate listings"},
        {"id": 3, "code": "accommodation",  "name": "Accommodation",  "icon": "🏨", "color": "#c77c00", "description": "Hotels & Airbnb"},
        {"id": 4, "code": "economic",       "name": "Economic",       "icon": "📈", "color": "#7c3aed", "description": "Macro indicators & FX rates"},
        {"id": 5, "code": "commodities",    "name": "Commodities",    "icon": "⛽", "color": "#dc2626", "description": "Fuel & commodity prices"},
        {"id": 6, "code": "financials",     "name": "Financials",     "icon": "📊", "color": "#059669", "description": "GSE stocks & indices"},
    ])

@app.route('/api/market-prices')
@limiter.limit("60 per minute")
def market_prices():
    category = request.args.get('category')
    location = request.args.get('location')
    limit    = min(int(request.args.get('limit', 50)), 200)
    where, params = [], []
    if category: where.append("product_category ILIKE %s"); params.append(f'%{category}%')
    if location: where.append("location ILIKE %s"); params.append(f'%{location}%')
    sql = "SELECT id, collected_date, week_number, year, product_category, title, price_ghs, location, condition, source FROM market_prices"
    if where: sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY collected_date DESC LIMIT %s"
    params.append(limit)
    rows = query('market_prices', sql, params)
    return jsonify([dict(r) for r in rows])

@app.route('/api/market-prices/categories')
def market_categories():
    rows = query('market_prices', "SELECT DISTINCT product_category, COUNT(*) AS count FROM market_prices GROUP BY product_category ORDER BY count DESC")
    return jsonify([dict(r) for r in rows])

@app.route('/api/market-prices/latest')
def market_latest():
    rows = query('market_prices', "SELECT product_category, ROUND(AVG(price_ghs),2) AS avg_price_ghs, COUNT(*) AS listings, MAX(collected_date) AS last_updated FROM market_prices GROUP BY product_category ORDER BY listings DESC")
    return jsonify([dict(r) for r in rows])

@app.route('/api/property')
@limiter.limit("60 per minute")
def property_prices():
    location = request.args.get('location')
    limit    = min(int(request.args.get('limit', 50)), 200)
    where, params = [], []
    if location: where.append("location ILIKE %s"); params.append(f'%{location}%')
    sql = "SELECT * FROM property_prices"
    if where: sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY collected_date DESC LIMIT %s"
    params.append(limit)
    rows = query('property', sql, params)
    return jsonify([dict(r) for r in rows])

@app.route('/api/accommodation')
@limiter.limit("60 per minute")
def accommodation():
    acc_type = request.args.get('type', 'hotel')
    limit    = min(int(request.args.get('limit', 50)), 200)
    table    = 'airbnb_prices' if acc_type == 'airbnb' else 'hotel_prices'
    rows     = query('accommodation', f"SELECT * FROM {table} ORDER BY collected_date DESC LIMIT %s", [limit])
    return jsonify([dict(r) for r in rows])

@app.route('/api/economic')
@limiter.limit("60 per minute")
def economic():
    indicator = request.args.get('indicator')
    sector    = request.args.get('sector')
    limit     = min(int(request.args.get('limit', 50)), 200)
    where, params = [], []
    if indicator: where.append("indicator_name ILIKE %s"); params.append(f'%{indicator}%')
    if sector:    where.append("sector ILIKE %s"); params.append(f'%{sector}%')
    sql = "SELECT id, collected_date, year, month, indicator_code, indicator_name, sector, value, unit, source FROM economic_indicators"
    if where: sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY collected_date DESC LIMIT %s"
    params.append(limit)
    rows = query('economic', sql, params)
    return jsonify([dict(r) for r in rows])

@app.route('/api/economic/indicators')
def economic_indicators():
    rows = query('economic', "SELECT DISTINCT indicator_name, indicator_code, sector, unit, COUNT(*) AS records FROM economic_indicators GROUP BY indicator_name, indicator_code, sector, unit ORDER BY sector, indicator_name")
    return jsonify([dict(r) for r in rows])

@app.route('/api/economic/exchange-rates')
def exchange_rates():
    limit = min(int(request.args.get('limit', 50)), 200)
    rows  = query('economic', "SELECT * FROM exchange_rates ORDER BY collected_date DESC LIMIT %s", [limit])
    return jsonify([dict(r) for r in rows])

@app.route('/api/commodities')
@limiter.limit("60 per minute")
def commodities():
    limit = min(int(request.args.get('limit', 50)), 200)
    rows  = query('commodities', "SELECT * FROM commodity_prices ORDER BY collected_date DESC LIMIT %s", [limit])
    return jsonify([dict(r) for r in rows])

@app.route('/api/fuel')
def fuel():
    limit = min(int(request.args.get('limit', 50)), 200)
    rows  = query('commodities', "SELECT * FROM fuel_prices ORDER BY collected_date DESC LIMIT %s", [limit])
    return jsonify([dict(r) for r in rows])

@app.route('/api/financials/stocks')
def stocks():
    limit = min(int(request.args.get('limit', 50)), 200)
    rows  = query('financials', "SELECT * FROM stock_prices ORDER BY collected_date DESC LIMIT %s", [limit])
    return jsonify([dict(r) for r in rows])

@app.route('/api/financials/indices')
def indices():
    limit = min(int(request.args.get('limit', 50)), 200)
    rows  = query('financials', "SELECT * FROM gse_indices ORDER BY collected_date DESC LIMIT %s", [limit])
    return jsonify([dict(r) for r in rows])

# ═══════════════════════════════════════════════════════════════
# PART 1 — PAYSTACK WEBHOOK
# ═══════════════════════════════════════════════════════════════
@app.route('/api/webhook/paystack', methods=['POST'])
@limiter.limit("30 per minute")
def paystack_webhook():
    secret_key = os.getenv('PAYSTACK_SECRET_KEY', '')
    signature  = request.headers.get('x-paystack-signature', '')
    raw_body   = request.get_data()

    # Verify Paystack HMAC-SHA512 signature
    expected = hmac.new(secret_key.encode('utf-8'), raw_body, hashlib.sha512).hexdigest()
    if not hmac.compare_digest(expected, signature):
        print(f"[WEBHOOK] Invalid signature — possible spoofed request")
        return jsonify({'error': 'Invalid signature'}), 401

    payload = request.get_json(force=True) or {}
    event   = payload.get('event', '')
    print(f"[WEBHOOK] Event received: {event}")

    if event == 'charge.success':
        data      = payload.get('data', {})
        email     = data.get('customer', {}).get('email', '')
        amount    = data.get('amount', 0) / 100   # Paystack sends in pesewas
        meta      = data.get('metadata', {}) or {}

        # Prefer exact product key from metadata (set by frontend triggerPaystack)
        # Fall back to plan-name keyword matching for subscription-style flows
        product_key = meta.get('product', '')
        if product_key in PRODUCT_SECTOR_MAP:
            sector = PRODUCT_SECTOR_MAP[product_key]
        else:
            plan      = data.get('plan', {})
            plan_name = plan.get('name', '') if isinstance(plan, dict) else str(plan)
            sector    = resolve_sector(plan_name)

        if not email:
            print(f"[WEBHOOK] charge.success with no email — skipping")
            return jsonify({'status': 'ok'}), 200

        print(f"[WEBHOOK] ✓ {email} | product='{product_key}' → sector={sector} | GH₵{amount:.2f}")

        if sector == 'bundle':
            # Send a separate download link for each sector in the bundle
            try:
                send_bundle_email(email, BUNDLE_SECTORS)
            except Exception as e:
                print(f"[WEBHOOK] Bundle email failed for {email}: {e}")
        else:
            download_url = generate_download_token(email, sector)
            try:
                send_download_email(email, download_url, sector)
            except Exception as e:
                print(f"[WEBHOOK] Email failed for {email}: {e}")
                # Still return 200 — token was created, email failure is non-fatal

    return jsonify({'status': 'ok'}), 200

# ═══════════════════════════════════════════════════════════════
# PART 3B — GHANA MARKET PRICE INDEX (GMPI)
# ═══════════════════════════════════════════════════════════════

@app.route('/api/gmpi')
@limiter.limit("60 per minute")
def get_gmpi():
    """
    Ghana Market Price Index — weekly composite price index across all tracked
    consumer goods categories. Base week = 100 (first week of data collection).

    Returns:
      - Weekly index values (overall + per category)
      - Week-on-week change (%)
      - Number of products and listings tracked each week
    """
    # ── Overall weekly index ──────────────────────────────────
    overall_sql = """
    WITH weekly AS (
        SELECT
            week_number,
            year,
            collected_date,
            product_category,
            COALESCE(NULLIF(normalized_name,''), search_label) AS product_name,
            AVG(price_ghs) AS avg_price
        FROM market_prices
        WHERE price_ghs > 1
          AND price_ghs < 200000
          AND (normalized_name IS NOT NULL AND normalized_name <> ''
               OR search_label IS NOT NULL)
        GROUP BY week_number, year, collected_date, product_category,
                 COALESCE(NULLIF(normalized_name,''), search_label)
    ),
    weekly_composite AS (
        SELECT
            week_number,
            year,
            collected_date,
            AVG(avg_price)             AS composite_avg_price,
            COUNT(DISTINCT product_name) AS products_tracked,
            SUM(1)                     AS category_product_count
        FROM weekly
        GROUP BY week_number, year, collected_date
        ORDER BY year, week_number
    ),
    base AS (
        SELECT composite_avg_price AS base_price
        FROM weekly_composite
        ORDER BY year, week_number
        LIMIT 1
    )
    SELECT
        w.week_number,
        w.year,
        w.collected_date                                    AS week_date,
        ROUND((w.composite_avg_price / b.base_price * 100)::numeric, 2)
                                                            AS gmpi,
        ROUND(w.composite_avg_price::numeric, 2)            AS avg_price_ghs,
        w.products_tracked,
        LAG(ROUND((w.composite_avg_price / b.base_price * 100)::numeric, 2))
            OVER (ORDER BY w.year, w.week_number)           AS prev_gmpi,
        ROUND(
            (ROUND((w.composite_avg_price / b.base_price * 100)::numeric, 2) -
             LAG(ROUND((w.composite_avg_price / b.base_price * 100)::numeric, 2))
                 OVER (ORDER BY w.year, w.week_number))
        , 2)                                                AS gmpi_change,
        ROUND(
            (ROUND((w.composite_avg_price / b.base_price * 100)::numeric, 2) -
             LAG(ROUND((w.composite_avg_price / b.base_price * 100)::numeric, 2))
                 OVER (ORDER BY w.year, w.week_number))
            / NULLIF(LAG(ROUND((w.composite_avg_price / b.base_price * 100)::numeric, 2))
                 OVER (ORDER BY w.year, w.week_number), 0) * 100
        , 2)                                                AS gmpi_change_pct
    FROM weekly_composite w, base b
    ORDER BY w.year DESC, w.week_number DESC
    """

    # ── Category-level index ──────────────────────────────────
    category_sql = """
    WITH weekly_cat AS (
        SELECT
            week_number,
            year,
            collected_date,
            product_category,
            AVG(price_ghs)               AS category_avg_price,
            COUNT(DISTINCT COALESCE(NULLIF(normalized_name,''), search_label))
                                         AS products_tracked
        FROM market_prices
        WHERE price_ghs > 1
          AND price_ghs < 200000
        GROUP BY week_number, year, collected_date, product_category
    ),
    base_cat AS (
        SELECT
            product_category,
            FIRST_VALUE(category_avg_price)
                OVER (PARTITION BY product_category
                      ORDER BY year, week_number) AS base_price
        FROM weekly_cat
    )
    SELECT DISTINCT
        w.week_number,
        w.year,
        w.collected_date               AS week_date,
        w.product_category,
        ROUND((w.category_avg_price / b.base_price * 100)::numeric, 2)
                                       AS category_index,
        ROUND(w.category_avg_price::numeric, 2)
                                       AS avg_price_ghs,
        w.products_tracked
    FROM weekly_cat w
    JOIN base_cat b
      ON w.product_category = b.product_category
    ORDER BY w.year DESC, w.week_number DESC, w.product_category
    """

    try:
        overall   = query('market_prices', overall_sql)
        by_cat    = query('market_prices', category_sql)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    return jsonify({
        'index_name':  'Ghana Market Price Index (GMPI)',
        'description': 'Composite weekly price index across all tracked consumer goods. Base = 100 at first collection week.',
        'base_note':   'GMPI of 110 means prices are 10% higher than the base week.',
        'overall':     overall,
        'by_category': by_cat,
    })


# ── Public GMPI summary (no auth — for website display) ──────
@app.route('/api/gmpi/latest')
@limiter.limit("120 per minute")
def get_gmpi_latest():
    """Latest GMPI value + week-on-week change. Safe for public display."""
    sql = """
    WITH weekly AS (
        SELECT
            week_number, year, collected_date,
            AVG(price_ghs) AS avg_price,
            COUNT(DISTINCT COALESCE(NULLIF(normalized_name,''), search_label)) AS products
        FROM market_prices
        WHERE price_ghs > 1 AND price_ghs < 200000
        GROUP BY week_number, year, collected_date
        ORDER BY year, week_number
    ),
    base AS (SELECT avg_price AS base_price FROM weekly ORDER BY year, week_number LIMIT 1)
    SELECT
        w.week_number, w.year, w.collected_date AS week_date,
        ROUND((w.avg_price / b.base_price * 100)::numeric, 2) AS gmpi,
        w.products AS products_tracked
    FROM weekly w, base b
    ORDER BY w.year DESC, w.week_number DESC
    LIMIT 2
    """
    try:
        rows = query('market_prices', sql)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    if not rows:
        return jsonify({'gmpi': None})

    latest = rows[0]
    prev   = rows[1] if len(rows) > 1 else None
    change = round(latest['gmpi'] - prev['gmpi'], 2) if prev else None
    change_pct = round((change / prev['gmpi']) * 100, 2) if prev and prev['gmpi'] else None

    return jsonify({
        'gmpi':             latest['gmpi'],
        'week':             latest['week_number'],
        'year':             latest['year'],
        'week_date':        str(latest['week_date']),
        'products_tracked': latest['products_tracked'],
        'change':           change,
        'change_pct':       change_pct,
        'label':            f"W{latest['week_number']} {latest['year']}",
    })


# ═══════════════════════════════════════════════════════════════
# PART 4 — DOWNLOAD ENDPOINT
# ═══════════════════════════════════════════════════════════════
@app.route('/api/download')
@limiter.limit("10 per minute")
def download_data():
    token = request.args.get('token', '').strip()
    if not token:
        return jsonify({'error': 'Token required'}), 400

    record = _TOKEN_STORE.get(token)
    if not record:
        return jsonify({'error': 'Invalid or expired token. Please purchase again.'}), 404

    now = datetime.now(timezone.utc)
    if now > record['expires_at']:
        _TOKEN_STORE.pop(token, None)
        return jsonify({'error': 'This download link has expired (24hr limit). Please purchase again.'}), 410

    if record['used']:
        return jsonify({'error': 'This download link has already been used (one-time only).'}), 410

    # Mark as used immediately before streaming
    record['used'] = True

    sector   = record['sector']
    email    = record['email']
    date_str = now.strftime('%Y-%m-%d')
    filename = f"sgdatalytics_{sector}_{date_str}.csv"
    queries  = SECTOR_QUERIES.get(sector, [])

    def generate_csv():
        output      = io.StringIO()
        first_table = True

        for entry in queries:
            db_key, sql, table_label = entry if len(entry) == 3 else (*entry, '')
            try:
                rows = query(db_key, sql)
            except Exception as e:
                print(f"[DOWNLOAD] DB error for {db_key}: {e}")
                continue

            if not rows:
                continue

            # Separator comment between tables so analysts know where one ends
            if not first_table:
                output.write(f"\n# === {table_label} ===\n")
            else:
                if table_label:
                    output.write(f"# === {table_label} ===\n")
                first_table = False

            # Fresh writer per table — each table gets its own header row
            writer = csv.DictWriter(
                output,
                fieldnames=rows[0].keys(),
                lineterminator='\n',
                extrasaction='ignore',
            )
            writer.writeheader()
            output.seek(0)
            yield output.read()
            output.seek(0); output.truncate(0)

            for row in rows:
                writer.writerow({k: (str(v) if v is not None else '') for k, v in row.items()})

            output.seek(0)
            yield output.read()
            output.seek(0); output.truncate(0)

    print(f"[DOWNLOAD] {email} downloaded '{sector}' at {now.isoformat()}")

    return Response(
        stream_with_context(generate_csv()),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'}
    )

# ═══════════════════════════════════════════════════════════════
# TEST ENDPOINT — manual trigger without Paystack payment
# ═══════════════════════════════════════════════════════════════
@app.route('/api/test-delivery', methods=['POST'])
@limiter.limit("5 per minute")
def test_delivery():
    """
    Test the full delivery pipeline without a real Paystack payment.
    Body: { "email": "test@example.com", "sector": "market_prices" }
    """
    data   = request.get_json() or {}
    email  = data.get('email', '').strip()
    sector = data.get('sector', 'market_prices').strip()

    if not email:
        return jsonify({'error': 'email is required'}), 400

    valid_sectors = list(SECTOR_QUERIES.keys())
    if sector not in valid_sectors:
        return jsonify({'error': f'Invalid sector. Choose from: {valid_sectors}'}), 400

    download_url = generate_download_token(email, sector, expires_hours=24)

    try:
        send_download_email(email, download_url, sector)
        return jsonify({
            'status':       'ok',
            'message':      f'Email sent to {email}',
            'sector':       sector,
            'download_url': download_url,
        })
    except Exception as e:
        return jsonify({
            'status':       'email_failed',
            'message':      str(e),
            'download_url': download_url,  # still return URL so you can test the download manually
        }), 500

# ── ERROR HANDLERS ───────────────────────────────────────────
@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Endpoint not found", "available": "/"}), 404

@app.errorhandler(429)
def rate_limited(e):
    return jsonify({"error": "Too many requests — please slow down"}), 429

@app.errorhandler(500)
def server_error(e):
    return jsonify({"error": "Internal server error", "message": str(e)}), 500

# ── START ─────────────────────────────────────────────────────
if __name__ == '__main__':
    port = int(os.getenv('PORT', os.getenv('FLASK_PORT', 5050)))
    print(f"\n  SG Datalytics SGMPI API v2 → http://0.0.0.0:{port}")
    print(f"  Connected databases: {[k for k,v in DB.items() if v]}\n")
    app.run(host='0.0.0.0', port=port, debug=os.getenv('FLASK_ENV') != 'production')
