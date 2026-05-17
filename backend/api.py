"""
SG Datalytics — SGMPI REST API v2
Connects to 6 Neon databases serving real Ghana market price data.

Run locally : python3 api.py
Run on Railway: gunicorn -w 2 -b 0.0.0.0:$PORT api:app
"""
import os, hmac, hashlib, json
from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2, psycopg2.extras
import resend

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

app = Flask(__name__)
CORS(app)

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

# ── ROOT ─────────────────────────────────────────────────────
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
        ]
    })

# ── HEALTH ───────────────────────────────────────────────────
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

# ── STATS ────────────────────────────────────────────────────
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

# ── SECTORS ──────────────────────────────────────────────────
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

# ── MARKET PRICES ────────────────────────────────────────────
@app.route('/api/market-prices')
def market_prices():
    category = request.args.get('category')
    location = request.args.get('location')
    limit    = min(int(request.args.get('limit', 50)), 200)
    offset   = max(0, int(request.args.get('offset', 0)))
    where, params = [], []
    if category: where.append("product_category ILIKE %s"); params.append(f'%{category}%')
    if location: where.append("location ILIKE %s"); params.append(f'%{location}%')
    # Only show quality-gated rows (normalized_name present)
    where.append("normalized_name IS NOT NULL AND normalized_name <> ''")
    sql = "SELECT id, collected_date, week_number, year, product_category, title, price_ghs, location, condition, source FROM market_prices"
    if where: sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])
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

# ── PROPERTY ─────────────────────────────────────────────────
@app.route('/api/property')
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

# ── ACCOMMODATION ────────────────────────────────────────────
@app.route('/api/accommodation')
def accommodation():
    acc_type = request.args.get('type', 'hotel')
    limit    = min(int(request.args.get('limit', 50)), 200)
    table    = 'airbnb_prices' if acc_type == 'airbnb' else 'hotel_prices'
    rows     = query('accommodation', f"SELECT * FROM {table} ORDER BY collected_date DESC LIMIT %s", [limit])
    return jsonify([dict(r) for r in rows])

# ── ECONOMIC ─────────────────────────────────────────────────
@app.route('/api/economic')
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

# ── COMMODITIES ──────────────────────────────────────────────
@app.route('/api/commodities')
def commodities():
    limit = min(int(request.args.get('limit', 50)), 200)
    rows  = query('commodities', "SELECT * FROM commodity_prices ORDER BY collected_date DESC LIMIT %s", [limit])
    return jsonify([dict(r) for r in rows])

@app.route('/api/fuel')
def fuel():
    limit = min(int(request.args.get('limit', 50)), 200)
    rows  = query('commodities', "SELECT * FROM fuel_prices ORDER BY collected_date DESC LIMIT %s", [limit])
    return jsonify([dict(r) for r in rows])

# ── FINANCIALS ───────────────────────────────────────────────
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

# ── PAYSTACK WEBHOOK & EMAIL ─────────────────────────────────

# Map Paystack payment page slug → product details + connection strings
# Connection strings are stored as Railway env vars (set after running Neon SQL)
PRODUCTS = {
    '72ruuze8qn': {
        'name':    'SGMPI Consumer Prices',
        'price':   'GHS 150/month',
        'strings': [
            ('Market Prices Database', 'CONN_SGMPI'),
        ],
        'views':   ['sgmpi_product'],
        'example': 'SELECT * FROM sgmpi_product WHERE year = 2026 LIMIT 100',
    },
    'cr1fxldrkx': {
        'name':    'Real Estate & Accommodation',
        'price':   'GHS 100/month',
        'strings': [
            ('Property Database',       'CONN_REALESTATE_PROPERTY'),
            ('Accommodation Database',  'CONN_REALESTATE_ACCOMMODATION'),
        ],
        'views':   ['property_product', 'hotels_product', 'airbnb_product'],
        'example': "SELECT * FROM property_product WHERE city = 'Accra' LIMIT 100",
    },
    'g7ug9brvpm': {
        'name':    'Macro & Commodities',
        'price':   'GHS 100/month',
        'strings': [
            ('Economic Database',    'CONN_MACRO_ECONOMIC'),
            ('Commodities Database', 'CONN_MACRO_COMMODITIES'),
        ],
        'views':   ['economic_product', 'fx_product', 'commodities_product', 'fuel_product'],
        'example': "SELECT * FROM economic_product WHERE sector = 'Monetary' LIMIT 100",
    },
    '1q3vr4l02p': {
        'name':    'Ghana Complete Bundle',
        'price':   'GHS 300/month',
        'strings': [
            ('Market Prices Database',  'CONN_SGMPI'),
            ('Property Database',       'CONN_REALESTATE_PROPERTY'),
            ('Accommodation Database',  'CONN_REALESTATE_ACCOMMODATION'),
            ('Economic Database',       'CONN_MACRO_ECONOMIC'),
            ('Commodities Database',    'CONN_MACRO_COMMODITIES'),
        ],
        'views':   ['sgmpi_product', 'property_product', 'hotels_product', 'airbnb_product',
                    'economic_product', 'fx_product', 'commodities_product', 'fuel_product'],
        'example': 'SELECT * FROM sgmpi_product LIMIT 100',
    },
}

def build_email_html(name, product):
    """Build the HTML welcome email with connection strings."""
    strings_html = ''
    for label, env_key in product['strings']:
        conn = os.getenv(env_key, '')
        if conn:
            strings_html += f'''
            <div style="margin-bottom:16px;">
              <p style="margin:0 0 6px;font-size:12px;color:#6b7280;font-family:monospace;text-transform:uppercase;letter-spacing:0.08em;">{label}</p>
              <div style="background:#f3f4f6;border:1px solid #e5e7eb;border-radius:6px;padding:12px 16px;font-family:monospace;font-size:13px;color:#1a2535;word-break:break-all;">{conn}</div>
            </div>'''

    views_list = ''.join(f'<li style="margin-bottom:4px;"><code style="background:#f3f4f6;padding:2px 6px;border-radius:4px;font-size:13px;">{v}</code></li>' for v in product['views'])

    return f"""
    <!DOCTYPE html>
    <html>
    <body style="margin:0;padding:0;background:#f0f4f8;font-family:'Helvetica Neue',Arial,sans-serif;">
      <table width="100%" cellpadding="0" cellspacing="0" style="padding:40px 20px;">
        <tr><td align="center">
          <table width="580" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 2px 16px rgba(0,0,0,0.08);">

            <!-- Header -->
            <tr><td style="background:#1a2535;padding:28px 36px;">
              <p style="margin:0;font-size:22px;font-weight:700;color:#ffffff;letter-spacing:0.04em;">SG <span style="color:#00957a;">DATALYTICS</span></p>
              <p style="margin:6px 0 0;font-size:13px;color:rgba(255,255,255,0.5);">Ghana's Data Marketplace</p>
            </td></tr>

            <!-- Body -->
            <tr><td style="padding:32px 36px;">
              <h2 style="margin:0 0 8px;font-size:20px;color:#1a2535;">You're in, {name}!</h2>
              <p style="margin:0 0 24px;color:#6b7280;font-size:14px;line-height:1.6;">
                Your subscription to <strong style="color:#1a2535;">{product['name']}</strong> ({product['price']}) is now active.
                Your connection string(s) are below — paste into Python, R, Excel, Tableau or Power BI and you're live.
              </p>

              <!-- Connection strings -->
              <div style="background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;padding:20px 24px;margin-bottom:24px;">
                <p style="margin:0 0 14px;font-size:13px;font-weight:700;color:#1a2535;text-transform:uppercase;letter-spacing:0.08em;">Your Connection String(s)</p>
                {strings_html if strings_html else '<p style="color:#dc2626;font-size:13px;">Connection strings are being configured — you will receive a follow-up email shortly.</p>'}
              </div>

              <!-- Views -->
              <p style="margin:0 0 8px;font-size:13px;font-weight:700;color:#1a2535;">Views you can query:</p>
              <ul style="margin:0 0 24px;padding-left:20px;color:#374151;font-size:14px;line-height:1.8;">
                {views_list}
              </ul>

              <!-- Quick start -->
              <p style="margin:0 0 8px;font-size:13px;font-weight:700;color:#1a2535;">Quick start (Python):</p>
              <div style="background:#1a2535;border-radius:8px;padding:16px 20px;margin-bottom:24px;font-family:monospace;font-size:13px;color:#00957a;line-height:1.8;">
                import pandas as pd<br/>
                conn = "YOUR_CONNECTION_STRING_ABOVE"<br/>
                df = pd.read_sql("{product['example']}", conn)<br/>
                print(df.head())
              </div>

              <!-- Note -->
              <div style="background:#fef9ec;border:1px solid #fde68a;border-radius:8px;padding:14px 18px;margin-bottom:24px;">
                <p style="margin:0;font-size:13px;color:#92400e;line-height:1.6;">
                  <strong>Monthly rotation:</strong> Your connection string is refreshed on the first Monday of each month for security.
                  We'll email you the new one — just replace the old string in your code.
                </p>
              </div>

              <p style="margin:0;font-size:13px;color:#6b7280;line-height:1.6;">
                Need help connecting? Reply to this email and we'll get you set up.<br/>
                Thank you for subscribing to SG Datalytics.
              </p>
            </td></tr>

            <!-- Footer -->
            <tr><td style="background:#f9fafb;border-top:1px solid #e5e7eb;padding:18px 36px;text-align:center;">
              <p style="margin:0;font-size:11px;color:#9ca3af;">
                © 2026 SG Datalytics · <a href="https://sgdatalytics.org" style="color:#00957a;text-decoration:none;">sgdatalytics.org</a> ·
                <a href="mailto:data@sgdatalytics.org" style="color:#00957a;text-decoration:none;">data@sgdatalytics.org</a>
              </p>
            </td></tr>

          </table>
        </td></tr>
      </table>
    </body>
    </html>
    """

def send_access_email(to_email, name, product):
    """Send connection string email via Resend."""
    resend.api_key = os.getenv('RESEND_API_KEY', '')
    if not resend.api_key:
        print(f'[webhook] RESEND_API_KEY not set — cannot email {to_email}')
        return False
    try:
        resend.Emails.send({
            'from':    'SG Datalytics <data@sgdatalytics.org>',
            'to':      [to_email],
            'subject': f'Your SG Datalytics Access — {product["name"]}',
            'html':    build_email_html(name, product),
        })
        print(f'[webhook] Access email sent → {to_email} ({product["name"]})')
        return True
    except Exception as e:
        print(f'[webhook] Email failed: {e}')
        return False

@app.route('/webhook/paystack', methods=['POST'])
def paystack_webhook():
    # ── 1. Verify Paystack signature ─────────────────────────
    secret = os.getenv('PAYSTACK_SECRET_KEY', '')
    signature = request.headers.get('x-paystack-signature', '')
    body = request.get_data()
    expected = hmac.new(secret.encode('utf-8'), body, hashlib.sha512).hexdigest()
    if not hmac.compare_digest(signature, expected):
        print('[webhook] Invalid Paystack signature — rejected')
        return jsonify({'error': 'invalid signature'}), 400

    # ── 2. Parse payload ──────────────────────────────────────
    try:
        payload = json.loads(body)
    except Exception:
        return jsonify({'error': 'bad json'}), 400

    event = payload.get('event', '')
    if event != 'charge.success':
        return jsonify({'status': 'ignored', 'event': event}), 200

    data     = payload.get('data', {})
    customer = data.get('customer', {})
    email    = customer.get('email', '')
    name     = customer.get('first_name') or customer.get('name') or 'Subscriber'
    source   = data.get('source') or {}
    slug     = source.get('identifier', '')

    print(f'[webhook] charge.success — {email} — page: {slug}')

    # ── 3. Match product ──────────────────────────────────────
    product = PRODUCTS.get(slug)
    if not product:
        # Fallback: try matching by amount (Paystack sends GHS in pesewas × 100)
        amount = data.get('amount', 0)
        amount_map = {15000: '72ruuze8qn', 10000: 'cr1fxldrkx', 30000: '1q3vr4l02p'}
        fallback_slug = amount_map.get(amount)
        product = PRODUCTS.get(fallback_slug)

    if not product or not email:
        print(f'[webhook] Unrecognized product or missing email — slug={slug}')
        return jsonify({'status': 'unrecognized'}), 200

    # ── 4. Send access email ──────────────────────────────────
    send_access_email(email, name, product)
    return jsonify({'status': 'ok', 'product': product['name'], 'email': email}), 200

# ── ERROR HANDLERS ───────────────────────────────────────────
@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Endpoint not found", "available": "/"}), 404

@app.errorhandler(500)
def server_error(e):
    return jsonify({"error": "Internal server error", "message": str(e)}), 500

# ── START ─────────────────────────────────────────────────────
if __name__ == '__main__':
    port = int(os.getenv('PORT', os.getenv('FLASK_PORT', 5050)))
    print(f"\n  SG Datalytics SGMPI API v2 → http://0.0.0.0:{port}")
    print(f"  Connected databases: {[k for k,v in DB.items() if v]}\n")
    app.run(host='0.0.0.0', port=port, debug=os.getenv('FLASK_ENV') != 'production')
