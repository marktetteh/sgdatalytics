# SG Datalytics — Deployment Guide
## Netlify (Frontend) + Railway (Backend + PostgreSQL)

---

## Architecture

```
GitHub Repo
    │
    ├──► Netlify ──► sgdatalytics.netlify.app  (frontend HTML)
    │                  marketplace.html
    │                  explorer.html
    │                  comparison.html
    │                  admin.html
    │
    └──► Railway ──► sgdatalytics.up.railway.app  (Flask API)
                       /api/health
                       /api/data
                       /api/stats
                       PostgreSQL database
```

---

## PART 1 — Push to GitHub (do this first)

1. Create a new repo on GitHub:
   - Go to https://github.com/new
   - Name: `sgdatalytics`
   - Visibility: Public (or Private)
   - Do NOT tick "Add README" — leave it empty
   - Click **Create repository**

2. Push your local files:
   ```bash
   cd sgdatalytics          # your project folder
   git init
   git branch -m main
   git add .
   git commit -m "Initial commit"
   git remote add origin https://github.com/YOUR_USERNAME/sgdatalytics.git
   git push -u origin main
   ```

---

## PART 2 — Deploy Backend to Railway

### Step 1 — Sign up / log in
Go to https://railway.app and sign in with your GitHub account.

### Step 2 — New Project
- Click **New Project**
- Choose **Deploy from GitHub repo**
- Select your `sgdatalytics` repository
- Click **Deploy Now**

### Step 3 — Add PostgreSQL database
- Inside your Railway project, click **+ New**
- Choose **Database → PostgreSQL**
- Railway automatically creates the database and sets `DATABASE_URL`

### Step 4 — Set environment variables
- Click your **sgdatalytics service** (not the database)
- Go to **Variables** tab
- Add these:
  ```
  FLASK_ENV    = production
  ```
  (DATABASE_URL is already set automatically by Railway)

### Step 5 — Set the start command
- Go to **Settings** tab of your service
- Under **Start Command**, enter:
  ```
  cd backend && gunicorn -w 2 -b 0.0.0.0:$PORT api:app
  ```

### Step 6 — Initialise the database
- Go to Railway **Dashboard → your service → Settings**
- Click **Deploy → One-off command** (or use Railway CLI):
  ```bash
  railway run python3 database/init_db.py
  ```
- This creates all 8 tables and seeds 6 sectors, 28 countries, 38 indicators.

### Step 7 — Seed World Bank data
```bash
railway run python3 database/seed_and_fetch.py
```
This fetches 672+ real data points from the World Bank API.

### Step 8 — Get your Railway URL
- Go to your service → **Settings → Networking**
- Click **Generate Domain**
- Copy the URL — it looks like: `https://sgdatalytics-production.up.railway.app`

### Step 9 — Test the API
Visit these URLs in your browser:
```
https://YOUR-APP.up.railway.app/api/health
https://YOUR-APP.up.railway.app/api/stats
https://YOUR-APP.up.railway.app/api/data?indicator=NY.GDP.MKTP.CD&countries=GH,NG
```

---

## PART 3 — Update Frontend Config

Open `frontend/config.js` and replace the Railway URL:
```js
API_URL: window.location.hostname === 'localhost'
  ? 'http://localhost:5050'
  : 'https://YOUR-APP.up.railway.app',   // ← paste your Railway URL here
```

Commit and push:
```bash
git add frontend/config.js
git commit -m "Set Railway API URL in config"
git push
```

---

## PART 4 — Deploy Frontend to Netlify

### Option A — Drag & Drop (fastest, no CLI needed)

1. Go to https://app.netlify.com
2. Click **Add new site → Deploy manually**
3. Drag your entire `frontend/` folder onto the Netlify drop zone
4. Netlify gives you a URL instantly like `random-name.netlify.app`
5. Go to **Site settings → General → Change site name**
6. Change it to `sgdatalytics` → your site becomes `sgdatalytics.netlify.app`

### Option B — Connect GitHub (auto-deploys on every push)

1. Go to https://app.netlify.com
2. Click **Add new site → Import an existing project**
3. Choose **Deploy with GitHub**
4. Select your `sgdatalytics` repo
5. Set build settings:
   - **Base directory**: (leave empty)
   - **Build command**: (leave empty)
   - **Publish directory**: `frontend`
6. Click **Deploy site**
7. Every time you `git push`, Netlify redeploys automatically ✓

### Custom domain (optional)
- In Netlify: **Domain management → Add custom domain**
- Add `sgdatalytics.org`
- Follow DNS instructions to point your domain to Netlify

---

## PART 5 — Final Checklist

- [ ] GitHub repo created and code pushed
- [ ] Railway project deployed with PostgreSQL plugin
- [ ] `DATABASE_URL` environment variable set automatically by Railway
- [ ] `init_db.py` run → 8 tables created
- [ ] `seed_and_fetch.py` run → data loaded
- [ ] Railway API URL tested: `/api/health` returns `{"status":"ok"}`
- [ ] `frontend/config.js` updated with Railway URL
- [ ] Netlify deployed from `frontend/` folder
- [ ] Netlify URL works: `sgdatalytics.netlify.app`
- [ ] Netlify can call Railway API (check browser console for CORS errors)

---

## Troubleshooting

**CORS error in browser?**
- The API already has `CORS(app)` enabled for all origins
- If still failing, check Railway logs for errors

**Railway build fails?**
- Make sure `backend/requirements.txt` exists
- Check the start command: `cd backend && gunicorn -w 2 -b 0.0.0.0:$PORT api:app`

**Database connection error?**
- Railway's `DATABASE_URL` uses `postgres://` prefix — psycopg2 needs `postgresql://`
- The api.py handles this automatically via the `sslmode='require'` flag

**Netlify shows blank page?**
- Make sure **Publish directory** is set to `frontend` not the repo root
- Check the `netlify.toml` is in the repo root

---

## Free Tier Limits

| Platform | Free Limit | Notes |
|----------|-----------|-------|
| Railway  | $5 credit/month | Enough for low-traffic API + small DB |
| Netlify  | 100GB bandwidth, 300 build mins/month | More than enough for static HTML |
| GitHub   | Unlimited public repos | Free |
