"""
SG Datalytics — SGMPI REST API v2
Connects to 6 Neon databases serving real Ghana market price data.

Run locally : python3 api.py
Run on Railway: gunicorn -w 2 -b 0.0.0.0:$PORT api:app
"""
import os
from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2, psycopg2.extras

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
