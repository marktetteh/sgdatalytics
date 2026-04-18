/**
 * SG Datalytics — Frontend Config
 *
 * Set RAILWAY_API_URL to your Railway backend URL after deploying.
 * Example: "https://sgdatalytics-production.up.railway.app"
 *
 * During local development, set to "http://localhost:5050"
 */

const SGDATA_CONFIG = {
  // ── Change this to your Railway URL after deploying ──────
  API_URL: window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1'
    ? 'http://localhost:5050'
    : 'https://YOUR-APP.up.railway.app',   // ← replace after Railway deploy

  WB_API: 'https://api.worldbank.org/v2',

  VERSION: '1.0.0',
  SITE_NAME: 'SG Datalytics',
};
