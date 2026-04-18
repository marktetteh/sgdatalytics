"""
SG Datalytics — REST API
Railway auto-injects DATABASE_URL. Falls back to individual DB_* vars for local dev.

Run locally : python3 api.py
Run on Railway: gunicorn -w 2 -b 0.0.0.0:$PORT api:app  (handled by Procfile)
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
CORS(app)  # allows Netlify frontend to call this API

# ── DB CONNECTION ────────────────────────────────────────────
# Railway sets DATABASE_URL automatically when you add a Postgres plugin
DATABASE_URL = os.getenv('DATABASE_URL')

def get_db():
    if DATABASE_URL:
        return psycopg2.connect(DATABASE_URL, sslmode='require')
    return psycopg2.connect(
        host     = os.getenv('DB_HOST',     'localhost'),
        port     = os.getenv('DB_PORT',     '5432'),
        dbname   = os.getenv('DB_NAME',     'sgdatalytics'),
        user     = os.getenv('DB_USER',     'sgdata'),
        password = os.getenv('DB_PASSWORD', 'sgdata2025'),
    )

def query(sql, params=None, one=False):
    conn = get_db()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql, params or [])
    result = cur.fetchone() if one else cur.fetchall()
    cur.close(); conn.close()
    return result

# ── ROOT ─────────────────────────────────────────────────────
@app.route('/')
def root():
    return jsonify({
        "name": "SG Datalytics API", "version": "1.0.0",
        "endpoints": [
            "GET /api/health",
            "GET /api/stats",
            "GET /api/sectors",
            "GET /api/countries",
            "GET /api/indicators?sector=economy",
            "GET /api/data?indicator=NY.GDP.MKTP.CD&countries=GH,NG,KE&from=2015&to=2023",
            "GET /api/latest?indicator=NY.GDP.MKTP.CD",
            "GET /api/country/GH",
        ]
    })

# ── HEALTH ───────────────────────────────────────────────────
@app.route('/api/health')
def health():
    try:
        r = query("SELECT COUNT(*) AS n FROM data_points", one=True)
        return jsonify({"status": "ok", "data_points": r['n']})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# ── STATS ────────────────────────────────────────────────────
@app.route('/api/stats')
def stats():
    r = query("""SELECT
        (SELECT COUNT(*)                     FROM data_points) AS total_points,
        (SELECT COUNT(*)                     FROM countries)   AS total_countries,
        (SELECT COUNT(*)                     FROM indicators)  AS total_indicators,
        (SELECT COUNT(*)                     FROM sectors)     AS total_sectors,
        (SELECT MIN(year)                    FROM data_points) AS year_min,
        (SELECT MAX(year)                    FROM data_points) AS year_max,
        (SELECT COUNT(DISTINCT country_id)   FROM data_points) AS countries_with_data,
        (SELECT COUNT(DISTINCT indicator_id) FROM data_points) AS indicators_with_data
    """, one=True)
    return jsonify(dict(r))

# ── SECTORS ──────────────────────────────────────────────────
@app.route('/api/sectors')
def sectors():
    rows = query("SELECT id,code,name,icon,color,description FROM sectors ORDER BY id")
    return jsonify([dict(r) for r in rows])

# ── COUNTRIES ────────────────────────────────────────────────
@app.route('/api/countries')
def countries():
    rows = query("SELECT id,TRIM(code) AS code,iso3,name,region,flag,income_level FROM countries ORDER BY region,name")
    return jsonify([dict(r) for r in rows])

# ── INDICATORS ───────────────────────────────────────────────
@app.route('/api/indicators')
def indicators():
    sector = request.args.get('sector')
    base   = """SELECT i.id,i.wb_code,i.name,i.unit,i.fmt,i.source,
                s.code AS sector_code,s.name AS sector_name,s.icon AS sector_icon
                FROM indicators i JOIN sectors s ON i.sector_id=s.id"""
    rows = query(base + (" WHERE s.code=%s ORDER BY i.name" if sector else " ORDER BY s.code,i.name"),
                 [sector] if sector else None)
    return jsonify([dict(r) for r in rows])

# ── DATA (time-series) ───────────────────────────────────────
@app.route('/api/data')
def data():
    ind_code = request.args.get('indicator', 'NY.GDP.MKTP.CD')
    ctries   = request.args.get('countries', 'GH').upper().split(',')
    yr_from  = int(request.args.get('from', 2010))
    yr_to    = int(request.args.get('to',   2023))
    ph       = ','.join(['%s'] * len(ctries))
    rows     = query(f"""
        SELECT TRIM(c.code) AS country_code, c.name AS country_name, c.flag, c.region,
               i.wb_code AS indicator_code, i.name AS indicator_name, i.unit, i.fmt,
               s.name AS sector, dp.year, dp.value
        FROM data_points dp
        JOIN countries  c ON dp.country_id   = c.id
        JOIN indicators i ON dp.indicator_id = i.id
        JOIN sectors    s ON i.sector_id     = s.id
        WHERE i.wb_code=%s AND TRIM(c.code) IN ({ph}) AND dp.year BETWEEN %s AND %s
        ORDER BY c.name, dp.year
    """, [ind_code] + ctries + [yr_from, yr_to])
    grouped, meta = {}, {}
    for r in rows:
        code = r['country_code']
        if code not in grouped:
            grouped[code] = []
            meta[code]    = {'country_code': code, 'country_name': r['country_name'],
                             'flag': r['flag'], 'region': r['region']}
        grouped[code].append({'year': r['year'], 'value': float(r['value']) if r['value'] else None})
    return jsonify({
        'indicator': {'code': ind_code,
                      'name': rows[0]['indicator_name'] if rows else ind_code,
                      'unit': rows[0]['unit'] if rows else '',
                      'fmt':  rows[0]['fmt']  if rows else ''},
        'year_range': {'from': yr_from, 'to': yr_to},
        'countries':  [{**meta[c], 'data': grouped[c]} for c in grouped],
        'total_records': len(rows)
    })

# ── LATEST ───────────────────────────────────────────────────
@app.route('/api/latest')
def latest():
    ind_code = request.args.get('indicator', 'NY.GDP.MKTP.CD')
    rows = query("""
        SELECT TRIM(c.code) AS country_code, c.name AS country_name,
               c.flag, c.region, i.wb_code, i.name AS indicator_name,
               i.unit, i.fmt, dp.year, dp.value
        FROM data_points dp
        JOIN countries  c ON dp.country_id   = c.id
        JOIN indicators i ON dp.indicator_id = i.id
        JOIN (SELECT country_id, indicator_id, MAX(year) AS max_year
              FROM data_points GROUP BY country_id, indicator_id) latest
          ON dp.country_id=latest.country_id AND dp.indicator_id=latest.indicator_id
         AND dp.year=latest.max_year
        WHERE i.wb_code=%s ORDER BY dp.value DESC NULLS LAST
    """, [ind_code])
    return jsonify([dict(r) for r in rows])

# ── COUNTRY PROFILE ──────────────────────────────────────────
@app.route('/api/country/<code>')
def country_profile(code):
    code    = code.upper()
    country = query("SELECT TRIM(code) AS code,name,flag,region,income_level FROM countries WHERE TRIM(code)=%s", [code], one=True)
    if not country:
        return jsonify({'error': 'Country not found'}), 404
    rows = query("""
        SELECT i.wb_code, i.name AS indicator_name, i.unit, i.fmt,
               s.name AS sector, s.icon, dp.year, dp.value
        FROM data_points dp
        JOIN indicators i ON dp.indicator_id=i.id
        JOIN sectors    s ON i.sector_id=s.id
        JOIN countries  c ON dp.country_id=c.id
        WHERE TRIM(c.code)=%s ORDER BY s.name, i.name, dp.year
    """, [code])
    grouped = {}
    for r in rows:
        k = r['wb_code']
        if k not in grouped:
            grouped[k] = {'wb_code': k, 'name': r['indicator_name'], 'unit': r['unit'],
                          'fmt': r['fmt'], 'sector': r['sector'], 'icon': r['icon'], 'data': []}
        if r['value'] is not None:
            grouped[k]['data'].append({'year': r['year'], 'value': float(r['value'])})
    return jsonify({'country': dict(country), 'indicators': list(grouped.values())})

# ── START ─────────────────────────────────────────────────────
if __name__ == '__main__':
    port = int(os.getenv('PORT', os.getenv('FLASK_PORT', 5050)))
    print(f"\n  SG Datalytics API → http://0.0.0.0:{port}")
    print(f"  DB mode: {'DATABASE_URL' if DATABASE_URL else 'individual vars'}\n")
    app.run(host='0.0.0.0', port=port, debug=os.getenv('FLASK_ENV') != 'production')
