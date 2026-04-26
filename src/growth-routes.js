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
import { runFetch, fetchProgress, DEFAULT_FETCH_LIMIT, scrapeProfile } from './growth-fetch.js';
import { runEnrich, enrichProgress, DEFAULT_ENRICH_LIMIT } from './growth-enrich.js';
import { runPulse, pulseProgress, DEFAULT_PULSE_LIMIT, getPulseStats } from './growth-pulse.js';
import { runInvite, inviteProgress, DEFAULT_INVITE_LIMIT, getInviteStats } from './growth-invite.js';
import {
  runConnectionsCheck, connectionsProgress, getConnectionsStats,
  getPendingMessages, savePendingMessages, generatePendingMessages, rewriteMessage,
  approveAndSendMessage, getSendingRowIndex,
} from './growth-connections.js';
import { peekQueue, claimCommand, resolveResult } from './growth-extension-queue.js';
import { getLinkedInPage } from './growth-browser.js';
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

// GET /api/growth/pulse-stats — KPIs for dashboard card
router.get('/api/growth/pulse-stats', requireSecret, async (_req, res) => {
  try {
    const stats = await getPulseStats();
    res.json(stats);
  } catch (e) {
    glog.error('[Routes] Pulse stats failed', e);
    res.status(500).json({ error: e.message });
  }
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

// GET /api/growth/invite-progress
router.get('/api/growth/invite-progress', requireSecret, (_req, res) => {
  res.json(inviteProgress);
});

// GET /api/growth/invite-stats — KPIs for dashboard card
router.get('/api/growth/invite-stats', requireSecret, async (_req, res) => {
  try {
    const stats = await getInviteStats();
    res.json(stats);
  } catch (e) {
    glog.error('[Routes] Invite stats failed', e);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/growth/invite-candidates — list top 10 eligible profiles (debug)
router.get('/api/growth/invite-candidates', requireSecret, async (_req, res) => {
  try {
    const { getAllRows, getBlacklist } = await import('./growth-sheets.js');
    const [all, blacklist] = await Promise.all([getAllRows(), getBlacklist()]);
    const blacklistUrls = new Set(blacklist.map(b => b.profileUrl?.trim()).filter(Boolean));
    const candidates = all
      .filter(r => r.status === 'Enriched' && !r.requestSent && !blacklistUrls.has(r.profileUrl?.trim()))
      .sort((a, b) => b.icpScore - a.icpScore)
      .slice(0, 10)
      .map(r => ({ name: r.name, score: r.icpScore, url: r.profileUrl, location: r.location }));
    res.json(candidates);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/growth/invite — Batch 5: send LinkedIn connection invitations
router.post('/api/growth/invite', requireSecret, async (req, res) => {
  if (!acquireLock('invite')) {
    return res.status(409).json({ error: 'Invite job already running.' });
  }

  const limit = Number(req.body?.limit) || DEFAULT_INVITE_LIMIT;
  res.json({ message: 'Invite started', limit });

  runInvite({ limit })
    .then(result => {
      const state = readJobState();
      state.lastInvite = result;
      saveJobState(state);
      glog.info('[Routes] Invite complete, state saved');
    })
    .catch(e => glog.error('[Routes] Invite failed', e))
    .finally(() => releaseLock('invite'));
});

// POST /api/extension/test-url — debug: enqueue any URL directly, returns result
router.post('/api/extension/test-url', requireSecret, async (req, res) => {
  const { profileUrl, personName = 'Test User' } = req.body || {};
  if (!profileUrl) return res.status(400).json({ error: 'profileUrl required' });
  const { enqueueExtensionInvite, awaitExtensionResult } = await import('./growth-extension-queue.js');
  const cmdId = enqueueExtensionInvite({ profileUrl, note: '', personName });
  glog.info(`[Debug] Test enqueued cmdId:${cmdId} url:${profileUrl}`);
  try {
    const result = await Promise.race([
      awaitExtensionResult(cmdId),
      new Promise((_, reject) => setTimeout(() => reject(new Error('timeout 120s')), 120_000)),
    ]);
    glog.info(`[Debug] Test result for ${profileUrl}: ${JSON.stringify(result)}`);
    res.json({ profileUrl, result });
  } catch (e) {
    res.status(500).json({ profileUrl, error: e.message });
  }
});

// ── CHROME EXTENSION QUEUE ENDPOINTS ─────────────────────────────────────────
// The Chrome extension polls these endpoints to pick up invite commands and
// report results. No Playwright involved — extension runs in the user's real Chrome.

// GET /api/extension/peek — extension polls this to check for pending commands
router.get('/api/extension/peek', requireSecret, (_req, res) => {
  res.json(peekQueue());
});

// GET /api/extension/command-for-url — content script calls this on page load
// to check if it was opened by Volmera and should execute an invite command.
router.get('/api/extension/command-for-url', requireSecret, (req, res) => {
  const raw = (req.query.url || '').replace(/\/$/, '').replace(/\?.*/,'').replace(/#.*/,'').toLowerCase();
  const cmd = peekQueue();
  if (!cmd) return res.json(null);
  const cmdUrl = cmd.profileUrl.replace(/\/$/, '').replace(/\?.*/,'').replace(/#.*/,'').toLowerCase();
  if (cmdUrl !== raw) return res.json(null);
  claimCommand(cmd.id);
  res.json(cmd);
});

// POST /api/extension/claim/:id — extension claims a command (marks in-progress)
router.post('/api/extension/claim/:id', requireSecret, (req, res) => {
  claimCommand(req.params.id);
  res.json({ ok: true });
});

// POST /api/extension/result — extension reports outcome
router.post('/api/extension/result', requireSecret, (req, res) => {
  const { cmdId, status, detail, noteAdded } = req.body || {};
  glog.info(`[Extension] Result for ${cmdId}: ${status} — ${detail} | note: ${noteAdded}`);
  resolveResult(cmdId, { status, detail, noteAdded });
  res.json({ ok: true });
});

// POST /api/growth/backfill-location
// Visits rows that have been processed (Enriched/Pending/Ready/Engaged/Followup/Lead/Success)
// but are missing location data. Uses a standalone location extractor — no scrapeProfile dependency.
// Playwright-only — zero Claude API cost. Safe to run multiple times.
router.post('/api/growth/backfill-location', requireSecret, async (_req, res) => {
  if (!acquireLock('backfill')) {
    return res.status(409).json({ error: 'A backfill job is already running.' });
  }

  res.json({ message: 'Location backfill started — check server logs for progress' });

  (async () => {
    let browser;
    try {
      const { getAllRows, batchUpdateRows } = await import('./growth-sheets.js');
      const all = await getAllRows();

      // Only backfill rows that have been fully processed — skip Scraped (Batch 2 will write location when it fetches them)
      const PROCESSED = new Set(['Enriched','Fetched','Pending','Ready','Engaged','Followup','Lead','Success','Dead']);
      const need = all.filter(r => PROCESSED.has(r.status) && !r.location && r.profileUrl);
      glog.info(`[Backfill] Location needed for ${need.length} processed rows`);

      if (need.length === 0) {
        glog.info('[Backfill] All processed rows already have location — nothing to do');
        return;
      }

      const session = await getLinkedInPage();
      browser = session.browser;
      const page = session.page;
      const { normalizeLinkedInUrl } = await import('./growth-fetch.js');
      let updated = 0, failed = 0;

      for (const row of need) {
        try {
          const safeUrl = normalizeLinkedInUrl(row.profileUrl);
          await page.goto(safeUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
          await page.waitForTimeout(2000);

          // Self-contained location extractor — no external function references
          const location = await page.evaluate(() => {
            const lines = (document.body.innerText || '').split('\n').map(s => s.trim()).filter(Boolean);
            const nameEl = document.querySelector('h1');
            const name   = nameEl ? nameEl.innerText.trim() : '';
            const nameIdx = name ? lines.indexOf(name) : -1;
            if (nameIdx < 0) return '';
            // Location is typically within 12 lines of the name, contains commas, no digits
            for (let i = nameIdx + 1; i < Math.min(nameIdx + 15, lines.length); i++) {
              const l = lines[i];
              if (/\b(connection|follower|following|contact)\b/i.test(l)) break;
              if (l.includes(',') && !l.match(/\d/) && l.length > 4 && l.length < 80) return l;
            }
            return '';
          });

          if (location) {
            await batchUpdateRows([{ rowIndex: row.rowIndex, data: { location } }]);
            glog.info(`[Backfill] ${row.name} → "${location}"`);
            updated++;
          } else {
            glog.info(`[Backfill] ${row.name} — no location found on profile`);
          }
        } catch (e) {
          glog.error(`[Backfill] Failed ${row.profileUrl}: ${e.message}`);
          failed++;
        }
        await page.waitForTimeout(3000 + Math.random() * 2000);
      }

      glog.info(`[Backfill] Done — updated: ${updated}, no-location: ${need.length - updated - failed}, failed: ${failed}`);
    } catch (e) {
      glog.error('[Backfill] Fatal error', e);
    } finally {
      if (browser) try { await browser.close(); } catch { /* ignore */ }
      releaseLock('backfill');
    }
  })();
});

// POST /api/growth/fix-junk-companies
// One-time fix: scans all rows for LinkedIn placeholder company text and sets them to "Not working".
// No Playwright, no API cost — pure sheet read/write.
router.post('/api/growth/fix-junk-companies', requireSecret, async (_req, res) => {
  try {
    const { getAllRows, batchUpdateRows } = await import('./growth-sheets.js');
    const { isLinkedInPlaceholder } = await import('./growth-fetch.js');
    const all = await getAllRows();
    const dirty = all.filter(r => r.company && isLinkedInPlaceholder(r.company));
    if (dirty.length === 0) {
      return res.json({ fixed: 0, message: 'No junk company values found' });
    }
    await batchUpdateRows(dirty.map(r => ({ rowIndex: r.rowIndex, data: { company: 'Not working' } })));
    const names = dirty.map(r => r.name).join(', ');
    glog.info(`[Fix] Junk companies corrected for ${dirty.length} row(s): ${names}`);
    res.json({ fixed: dirty.length, rows: dirty.map(r => ({ name: r.name, was: r.company })) });
  } catch (e) {
    glog.error('[Fix] fix-junk-companies failed', e);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/growth/repair-row
// Reset a specific contact row's name and clear lastPulse so Pulse re-scrapes it.
// Body: { rowIndex: number, name: string }
router.post('/api/growth/repair-row', requireSecret, async (req, res) => {
  try {
    const { getAllRows, batchUpdateRows } = await import('./growth-sheets.js');
    const { rowIndex, name } = req.body || {};
    if (!rowIndex || !name) return res.status(400).json({ error: 'rowIndex and name required' });
    const all = await getAllRows();
    const row = all.find(r => r.rowIndex === Number(rowIndex));
    if (!row) return res.status(404).json({ error: `Row ${rowIndex} not found` });
    await batchUpdateRows([{ rowIndex: Number(rowIndex), data: { name, lastPulse: '' } }]);
    glog.info(`[Fix] Repaired row ${rowIndex}: name → "${name}", lastPulse cleared`);
    res.json({ repaired: true, rowIndex, name, profileUrl: row.profileUrl });
  } catch (e) {
    glog.error('[Fix] repair-row failed', e);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/growth/fix-locale-urls
// One-time fix: finds all Contact rows with LinkedIn locale suffixes in the URL (/en/, /pt/, etc.)
// Normalizes the URL in the sheet. Also resets name to the last word of the old name (best effort)
// if the current name is suspected to be wrong (detected by checking if normalized URL was different).
// Zero Playwright — pure sheet read/write.
router.post('/api/growth/fix-locale-urls', requireSecret, async (_req, res) => {
  try {
    const { getAllRows, batchUpdateRows } = await import('./growth-sheets.js');
    const { normalizeLinkedInUrl } = await import('./growth-fetch.js');
    const all = await getAllRows();
    const dirty = all.filter(r => r.profileUrl && normalizeLinkedInUrl(r.profileUrl) !== r.profileUrl);
    if (dirty.length === 0) {
      return res.json({ fixed: 0, message: 'No locale-suffixed URLs found' });
    }
    // Update URL only — name/data will be corrected by running Pulse on these rows next
    await batchUpdateRows(dirty.map(r => ({
      rowIndex: r.rowIndex,
      data: { profileUrl: normalizeLinkedInUrl(r.profileUrl) },
    })));
    const details = dirty.map(r => ({ row: r.rowIndex, name: r.name, was: r.profileUrl, now: normalizeLinkedInUrl(r.profileUrl) }));
    glog.info(`[Fix] Locale URLs corrected for ${dirty.length} row(s): ${dirty.map(r => r.name).join(', ')}`);
    res.json({ fixed: dirty.length, rows: details });
  } catch (e) {
    glog.error('[Fix] fix-locale-urls failed', e);
    res.status(500).json({ error: e.message });
  }
});

// ── BATCH 6: CONNECTIONS CHECK + MARKETING MESSAGES ──────────────────────────

// GET /api/growth/connections-progress — live progress polled by dashboard
router.get('/api/growth/connections-progress', requireSecret, (_req, res) => {
  res.json(connectionsProgress);
});

// GET /api/growth/connections-stats — KPI numbers for dashboard card
router.get('/api/growth/connections-stats', requireSecret, async (_req, res) => {
  try {
    const stats = await getConnectionsStats();
    res.json(stats);
  } catch (e) {
    glog.error('[Routes] Connections stats failed', e);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/growth/connections-check — trigger network scan via Chrome extension
router.post('/api/growth/connections-check', requireSecret, async (req, res) => {
  if (!acquireLock('connections')) {
    return res.status(409).json({ error: 'Connections check already running.' });
  }

  const state     = readJobState();
  const sinceDate = state.lastConnectionsCheck || null;
  res.json({ message: 'Connections check started', sinceDate });

  runConnectionsCheck({ sinceDate })
    .then(result => {
      const s = readJobState();
      s.lastConnectionsCheck  = new Date().toISOString().split('T')[0]; // YYYY-MM-DD
      s.lastConnectionsResult = result;
      saveJobState(s);
      glog.info(`[Routes] Connections check complete — found: ${result.found}, matched: ${result.matched}`);
    })
    .catch(e => glog.error('[Routes] Connections check failed', e))
    .finally(() => releaseLock('connections'));
});

// POST /api/extension/connections-result — extension reports scraped connections
router.post('/api/extension/connections-result', requireSecret, (req, res) => {
  const { cmdId, connections } = req.body || {};
  glog.info(`[Extension] Connections result for ${cmdId}: ${connections?.length} connections`);
  resolveResult(cmdId, { connections: connections || [] });
  res.json({ ok: true });
});

// GET /api/growth/messages/pending — list pending messages + sending state
router.get('/api/growth/messages/pending', requireSecret, (_req, res) => {
  const msgs         = getPendingMessages();
  const sendingIdx   = getSendingRowIndex();
  const withState    = msgs.map(m => ({ ...m, sending: m.rowIndex === sendingIdx }));
  res.json(withState);
});

// POST /api/growth/messages/generate — generate Claude messages for all Ready people
router.post('/api/growth/messages/generate', requireSecret, async (req, res) => {
  if (!acquireLock('messages')) {
    return res.status(409).json({ error: 'Message generation already running.' });
  }

  const limit = Number(req.body?.limit) || 20;
  res.json({ message: 'Message generation started', limit });

  generatePendingMessages({ limit })
    .then(msgs => glog.info(`[Routes] Generated messages — total pending: ${msgs.length}`))
    .catch(e => glog.error('[Routes] Message generation failed', e))
    .finally(() => releaseLock('messages'));
});

// POST /api/growth/messages/rewrite/:rowIndex — rewrite with redirect instruction
router.post('/api/growth/messages/rewrite/:rowIndex', requireSecret, async (req, res) => {
  const rowIndex = Number(req.params.rowIndex);
  const { redirect } = req.body || {};
  if (!redirect) return res.status(400).json({ error: 'redirect instruction required' });

  try {
    const updated = await rewriteMessage({ rowIndex, redirect });
    res.json({ ok: true, message: updated });
  } catch (e) {
    glog.error('[Routes] Rewrite failed', e);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/growth/messages/approve/:rowIndex — approve, translate, send via extension
router.post('/api/growth/messages/approve/:rowIndex', requireSecret, async (req, res) => {
  const rowIndex = Number(req.params.rowIndex);
  res.json({ queued: true });

  // Run in background — dashboard polls /messages/pending to detect completion
  approveAndSendMessage({ rowIndex })
    .then(r  => glog.info(`[Routes] Message sent for row ${rowIndex}: ${r.name}`))
    .catch(e => glog.error(`[Routes] Approve/send failed for row ${rowIndex}: ${e.message}`));
});

// POST /api/growth/messages/skip/:rowIndex — remove card from tray without sending
router.post('/api/growth/messages/skip/:rowIndex', requireSecret, (req, res) => {
  const rowIndex = Number(req.params.rowIndex);
  const msgs = getPendingMessages();
  savePendingMessages(msgs.filter(m => m.rowIndex !== rowIndex));
  glog.info(`[Routes] Skipped message card for rowIndex ${rowIndex}`);
  res.json({ ok: true });
});

// POST /api/growth/followup/approve/:rowIndex
router.post('/api/growth/followup/approve/:rowIndex', requireSecret, async (req, res) => {
  res.json({ message: 'Follow-up approval will be available in Phase 7' });
});

// POST /api/growth/message-update — paste a received reply, Claude classifies + updates status
router.post('/api/growth/message-update', requireSecret, async (req, res) => {
  res.json({ message: 'Message Update will be available in Phase 7' });
});

// GET /api/growth/message-update/search?q=name
router.get('/api/growth/message-update/search', requireSecret, async (req, res) => {
  res.json({ message: 'Message Update search will be available in Phase 7' });
});

// POST /api/system/restart — restart the server process so new code is picked up
router.post('/api/system/restart', requireSecret, (_req, res) => {
  res.json({ ok: true, message: 'Restarting…' });
  glog.info('[System] Restart requested via dashboard');
  setTimeout(() => process.exit(0), 300);
});

export { readCrawlLogs, saveCrawlLogs, readJobState, saveJobState };
export default router;
