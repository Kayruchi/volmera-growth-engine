// growth-routes.js — Volmera Growth Engine Express Router
// Mounted into Blog & Post Automation server.js via:
//   import growthRouter from '../../Volmera Growth Engine/src/growth-routes.js';
//   app.use(growthRouter);

import { createRequire } from 'node:module';
import path from 'path';
import { fileURLToPath } from 'url';
import { google } from 'googleapis';
import { glog } from './growth-logger.js';
import { acquireLock, releaseLock, getActiveLocks, isAnyGrowthJobRunning } from './growth-lock.js';
import { getStatusCounts } from './growth-sheets.js';
import { runCrawl, crawlProgress, DEFAULT_SEARCH_URL } from './growth-crawl.js';
import { runFetch, fetchProgress, DEFAULT_FETCH_LIMIT } from './growth-fetch.js';
import { runEnrich, enrichProgress, DEFAULT_ENRICH_LIMIT } from './growth-enrich.js';
import { runPulse, pulseProgress, DEFAULT_PULSE_LIMIT } from './growth-pulse.js';
import { readFileSync, writeFileSync, existsSync } from 'fs';

const __dirname   = path.dirname(fileURLToPath(import.meta.url));
const BLOG_ROOT   = path.resolve(__dirname, '..', '..', 'Volmera Blog & Post Automation');
const req         = createRequire(path.join(BLOG_ROOT, 'package.json'));
const { Router }  = req('express');
const DATA_DIR    = path.join(__dirname, '..', 'data-growth');
const CRAWL_LOGS  = path.join(DATA_DIR, 'crawl-logs.json');
const JOB_STATE   = path.join(DATA_DIR, 'job-state.json');

const router = Router();

// ── AUTH MIDDLEWARE ───────────────────────────────────────────────────────────

function requireSecret(req, res, next) {
  const secret = process.env.API_SECRET;
  if (!secret) return next();
  const provided = req.headers['x-api-secret'] || req.query.secret;
  if (provided !== secret) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

// ── HELPERS ───────────────────────────────────────────────────────────────────

function readCrawlLogs() {
  try { return JSON.parse(readFileSync(CRAWL_LOGS, 'utf8')); } catch { return []; }
}
function saveCrawlLogs(logs) {
  writeFileSync(CRAWL_LOGS, JSON.stringify(logs.slice(-3), null, 2)); // keep last 3
}
function readJobState() {
  try { return JSON.parse(readFileSync(JOB_STATE, 'utf8')); } catch { return {}; }
}
function saveJobState(state) {
  writeFileSync(JOB_STATE, JSON.stringify(state, null, 2));
}

// ── GOOGLE OAUTH ──────────────────────────────────────────────────────────────
// One-time setup. Visit /oauth/google → approve → copy refresh token to .env

const GOOGLE_SCOPES = ['https://www.googleapis.com/auth/spreadsheets'];

// OAuth callback MUST point to localhost — ngrok/production URL won't work here
const GOOGLE_REDIRECT_URI = 'http://localhost:3000/oauth/google/callback';

function getOAuthClient() {
  return new google.auth.OAuth2(
    process.env.GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET,
    GOOGLE_REDIRECT_URI
  );
}

router.get('/oauth/google', (_req, res) => {
  if (!process.env.GOOGLE_CLIENT_ID) {
    return res.send('<h2>GOOGLE_CLIENT_ID not set in .env</h2>');
  }
  const url = getOAuthClient().generateAuthUrl({
    access_type: 'offline',
    scope: GOOGLE_SCOPES,
    prompt: 'consent', // force refresh_token every time
  });
  res.redirect(url);
});

router.get('/oauth/google/callback', async (req, res) => {
  const { code, error } = req.query;
  if (error) return res.send(`Google OAuth error: ${error}`);
  if (!code)  return res.send('No code returned from Google');
  try {
    const { tokens } = await getOAuthClient().getToken(code);
    res.send(`
      <h2>Google OAuth — copy the refresh token into your .env</h2>
      <p><strong>Add this line to your .env file (in Blog &amp; Post Automation folder):</strong></p>
      <pre style="background:#111;color:#0f0;padding:16px;border-radius:8px;font-size:13px">GOOGLE_REFRESH_TOKEN=${tokens.refresh_token || '(no refresh token — re-run with prompt: consent)'}</pre>
      <p>Also add these if not already present:</p>
      <pre style="background:#111;color:#0f0;padding:16px;border-radius:8px;font-size:13px">GOOGLE_CLIENT_ID=${process.env.GOOGLE_CLIENT_ID}
GOOGLE_CLIENT_SECRET=${process.env.GOOGLE_CLIENT_SECRET}
GROWTH_SHEET_ID=1tbGNSHsUZihsR4vciqV5dXlIf4Iy4nzlZS2-8DkRBIg</pre>
      <p>Once saved, restart the server. Google Sheets will be ready.</p>
    `);
  } catch (e) {
    res.send(`Token exchange failed: ${e.message}`);
  }
});

// ── GROWTH STATUS ─────────────────────────────────────────────────────────────

// GET /api/growth/status — sheet status counts + active locks
router.get('/api/growth/status', requireSecret, async (_req, res) => {
  try {
    const [counts, locks] = await Promise.all([
      getStatusCounts(),
      Promise.resolve(getActiveLocks()),
    ]);
    res.json({ counts, locks, hasGoogleAuth: !!process.env.GOOGLE_REFRESH_TOKEN });
  } catch (e) {
    glog.error('[Routes] Status fetch failed', e);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/growth/crawl-logs — last 3 crawl run records
router.get('/api/growth/crawl-logs', requireSecret, (_req, res) => {
  res.json(readCrawlLogs());
});

// GET /api/growth/job-state — progress state for animated dashboard counters
router.get('/api/growth/job-state', requireSecret, (_req, res) => {
  res.json(readJobState());
});

// GET /api/growth/locks — which jobs are currently running
router.get('/api/growth/locks', requireSecret, (_req, res) => {
  res.json(getActiveLocks());
});

// GET /api/growth/crawl-progress — live progress polled by dashboard every 2s
router.get('/api/growth/crawl-progress', requireSecret, (_req, res) => {
  res.json(crawlProgress);
});

// GET /api/growth/fetch-progress
router.get('/api/growth/fetch-progress', requireSecret, (_req, res) => {
  res.json(fetchProgress);
});

// GET /api/growth/enrich-progress
router.get('/api/growth/enrich-progress', requireSecret, (_req, res) => {
  res.json(enrichProgress);
});

// ── JOB TRIGGERS ─────────────────────────────────────────────────────────────

// POST /api/growth/crawl
router.post('/api/growth/crawl', requireSecret, async (req, res) => {
  if (!acquireLock('crawl')) {
    return res.status(409).json({ error: 'Crawl job already running.' });
  }

  const searchUrl = req.body?.searchUrl || DEFAULT_SEARCH_URL;
  res.json({ message: 'Crawl started', searchUrl });

  // Run async — response already sent
  runCrawl({ searchUrl })
    .then(result => {
      const logs = readCrawlLogs();
      logs.push(result);
      saveCrawlLogs(logs);
      glog.info('[Routes] Crawl complete, log saved');
    })
    .catch(e => glog.error('[Routes] Crawl failed', e))
    .finally(() => releaseLock('crawl'));
});

// POST /api/growth/fetch — Job 2: Playwright visits each Scraped profile, extracts Name/Title/Company
router.post('/api/growth/fetch', requireSecret, async (req, res) => {
  if (!acquireLock('fetch')) {
    return res.status(409).json({ error: 'Fetch job already running.' });
  }

  const limit = Number(req.body?.limit) || DEFAULT_FETCH_LIMIT;
  res.json({ message: 'Fetch started', limit });

  runFetch({ limit })
    .then(result => {
      const state = readJobState();
      state.lastFetch = result;
      saveJobState(state);
      glog.info('[Routes] Fetch complete, state saved');
    })
    .catch(e => glog.error('[Routes] Fetch failed', e))
    .finally(() => releaseLock('fetch'));
});

// POST /api/growth/enrich — Job 3: Brave Search + Claude ICP scoring on Fetched rows
router.post('/api/growth/enrich', requireSecret, async (req, res) => {
  if (!acquireLock('enrich')) {
    return res.status(409).json({ error: 'Enrich job already running.' });
  }

  const limit = Number(req.body?.limit) || DEFAULT_ENRICH_LIMIT;
  res.json({ message: 'Enrich started', limit });

  runEnrich({ limit })
    .then(result => {
      const state = readJobState();
      state.lastEnrich = result;
      saveJobState(state);
      glog.info('[Routes] Enrich complete, state saved');
    })
    .catch(e => glog.error('[Routes] Enrich failed', e))
    .finally(() => releaseLock('enrich'));
});

// GET /api/growth/pulse-progress
router.get('/api/growth/pulse-progress', requireSecret, (_req, res) => {
  res.json(pulseProgress);
});

// POST /api/growth/pulse — Job 4: Playwright checks for job changes, re-enriches if changed
router.post('/api/growth/pulse', requireSecret, async (req, res) => {
  if (!acquireLock('pulse')) {
    return res.status(409).json({ error: 'Pulse job already running.' });
  }

  const limit = Number(req.body?.limit) || DEFAULT_PULSE_LIMIT;
  res.json({ message: 'Pulse started', limit });

  runPulse({ limit })
    .then(result => {
      const state = readJobState();
      state.lastPulse = result;
      saveJobState(state);
      glog.info('[Routes] Pulse complete, state saved');
    })
    .catch(e => glog.error('[Routes] Pulse failed', e))
    .finally(() => releaseLock('pulse'));
});

// POST /api/growth/invite
router.post('/api/growth/invite', requireSecret, async (_req, res) => {
  if (!acquireLock('invite')) {
    return res.status(409).json({ error: 'Another job is already running. Check /api/growth/locks.' });
  }
  res.json({ message: 'Invite job will be available in Phase 5' });
  releaseLock('invite');
});

// POST /api/growth/message/approve/:rowIndex — approve & send a marketing message
router.post('/api/growth/message/approve/:rowIndex', requireSecret, async (req, res) => {
  res.json({ message: 'Message approval will be available in Phase 6' });
});

// POST /api/growth/followup/approve/:rowIndex
router.post('/api/growth/followup/approve/:rowIndex', requireSecret, async (req, res) => {
  res.json({ message: 'Follow-up approval will be available in Phase 7' });
});

// POST /api/growth/message-update — paste a received reply, Claude classifies + updates status
router.post('/api/growth/message-update', requireSecret, async (req, res) => {
  res.json({ message: 'Message Update will be available in Phase 6' });
});

// GET /api/growth/message-update/search?q=name — search people we messaged
router.get('/api/growth/message-update/search', requireSecret, async (req, res) => {
  res.json({ message: 'Message Update search will be available in Phase 6' });
});

export { readCrawlLogs, saveCrawlLogs, readJobState, saveJobState };
export default router;
