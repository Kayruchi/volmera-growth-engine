// growth-pulse.js — Job 4: Pulse Check
// Playwright visits each eligible profile, compares current title/company to what's
// in the sheet. If changed → log to Changes tab + re-enrich. Either way → update Last Pulse date.
//
// Eligibility: lastPulse empty OR lastPulse older than 90 days.
// Priority order: Pending, Ready, Engaged, Followup, Lead first → then Enriched.
// Skipped statuses: Scraped, Fetched (not processed yet), Dead.

import { getLinkedInPage } from './growth-browser.js';
import { getAllRows, batchUpdateRows, appendChange } from './growth-sheets.js';
import { enrichProfile } from './growth-enrich.js';
import { scrapeProfile } from './growth-fetch.js';
import { glog } from './growth-logger.js';

export const DEFAULT_PULSE_LIMIT = 50;
const PULSE_INTERVAL_DAYS = 90;

// ── IN-MEMORY PROGRESS ────────────────────────────────────────────────────────
export const pulseProgress = {
  status:    'idle',
  total:     0,
  done:      0,
  unchanged: 0,
  changed:   0,
  failed:    0,
  current:   null,
  error:     null,
  updatedAt: null,
};

function setProgress(update) {
  Object.assign(pulseProgress, update, { updatedAt: new Date().toISOString() });
}

// ── MAIN EXPORT ───────────────────────────────────────────────────────────────

export async function runPulse({ limit = DEFAULT_PULSE_LIMIT } = {}) {
  setProgress({ status: 'running', total: 0, done: 0, unchanged: 0, changed: 0, failed: 0, current: null, error: null });
  glog.info(`[Pulse] Starting — limit: ${limit}`);

  let browser;

  try {
    // ── Load and filter eligible rows ─────────────────────────────────────────
    const all = await getAllRows();
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - PULSE_INTERVAL_DAYS);

    const PRIORITY_STATUSES = ['Pending', 'Ready', 'Engaged', 'Followup', 'Lead'];
    const SECONDARY_STATUSES = ['Enriched'];
    const SKIP_STATUSES = new Set(['Scraped', 'Fetched', 'Dead', '']);

    const isEligible = (row) => {
      if (SKIP_STATUSES.has(row.status)) return false;
      if (!row.lastPulse) return true;             // never pulse-checked
      return new Date(row.lastPulse) < cutoff;     // last check was 90+ days ago
    };

    const priority  = all.filter(r => PRIORITY_STATUSES.includes(r.status) && isEligible(r));
    const secondary = all.filter(r => SECONDARY_STATUSES.includes(r.status) && isEligible(r));

    // Fill the batch: priority first, backfill with secondary
    const batch = [
      ...priority.slice(0, limit),
      ...secondary.slice(0, Math.max(0, limit - priority.length)),
    ].slice(0, limit);

    setProgress({ total: batch.length });
    glog.info(`[Pulse] Eligible — priority: ${priority.length}, secondary: ${secondary.length}. Processing: ${batch.length}`);

    if (batch.length === 0) {
      glog.info('[Pulse] No eligible rows — all recently checked or no qualifying statuses');
      setProgress({ status: 'done' });
      return { total: 0, unchanged: 0, changed: 0, failed: 0 };
    }

    // ── Launch LinkedIn browser ───────────────────────────────────────────────
    const session = await getLinkedInPage();
    browser = session.browser;
    const page = session.page;

    for (const row of batch) {
      setProgress({ current: row.profileUrl });
      const today = new Date().toISOString().slice(0, 10);

      try {
        const scraped = await scrapeProfile(page, row.profileUrl);

        // Normalise for comparison — treat empty strings as equivalent
        const oldTitle   = (row.title   || '').trim().toLowerCase();
        const oldCompany = (row.company === 'Not working' ? '' : row.company || '').trim().toLowerCase();
        const newTitle   = (scraped.title   || '').trim().toLowerCase();
        const newCompany = (scraped.company || '').trim().toLowerCase();

        const titleChanged   = newTitle   && newTitle   !== oldTitle;
        const companyChanged = newCompany && newCompany !== oldCompany;
        const changed = titleChanged || companyChanged;

        if (changed) {
          const changeDesc = [
            titleChanged   ? `title: "${row.title}" → "${scraped.title}"` : null,
            companyChanged ? `company: "${row.company}" → "${scraped.company}"` : null,
          ].filter(Boolean).join(' | ');
          glog.info(`[Pulse] CHANGE detected — ${row.name} | ${changeDesc}`);

          // 1. Log to Changes tab (old values before overwriting)
          await appendChange({
            profileUrl: row.profileUrl,
            name:       row.name,
            oldTitle:   row.title   || '',
            oldCompany: row.company || '',
          });

          // 2. Re-enrich with new data
          const rowWithNew = {
            ...row,
            name:          scraped.name    || row.name,
            title:         scraped.title,
            company:       scraped.company,
            isCurrentRole: scraped.isCurrentRole,
            roleDate:      scraped.roleDate,
          };

          try {
            const enrichData = await enrichProfile(rowWithNew);
            await batchUpdateRows([{
              rowIndex: row.rowIndex,
              data: {
                name:          scraped.name    || row.name,
                title:         scraped.title,
                company:       scraped.isCurrentRole === false ? 'Not working' : scraped.company,
                relevantProds: enrichData.relevantProds,
                opEstimate:    enrichData.opEstimate,
                icpReason:     enrichData.icpReason,
                icpScore:      enrichData.icpScore,
                lastPulse:     today,
              },
            }]);
            glog.info(`[Pulse] Re-enriched — ${row.name} | new score: ${enrichData.icpScore}`);
          } catch (enrichErr) {
            // Enrich failed — still update name/title/company and last pulse
            await batchUpdateRows([{
              rowIndex: row.rowIndex,
              data: {
                name:      scraped.name    || row.name,
                title:     scraped.title,
                company:   scraped.isCurrentRole === false ? 'Not working' : scraped.company,
                lastPulse: today,
              },
            }]);
            glog.warn(`[Pulse] Re-enrich failed for ${row.profileUrl}: ${enrichErr.message}`);
          }

          setProgress({ changed: pulseProgress.changed + 1 });

        } else {
          // No change — just update last pulse date
          await batchUpdateRows([{ rowIndex: row.rowIndex, data: { lastPulse: today } }]);
          setProgress({ unchanged: pulseProgress.unchanged + 1 });
          glog.info(`[Pulse] No change — ${row.name} @ ${row.company}`);
        }

      } catch (e) {
        setProgress({ failed: pulseProgress.failed + 1 });
        glog.warn(`[Pulse] Failed ${row.profileUrl}: ${e.message}`);
        // Do NOT update lastPulse — row stays eligible so it can be retried
      }

      setProgress({ done: pulseProgress.done + 1 });

      // LinkedIn polite delay between profile visits
      if (pulseProgress.done < batch.length) {
        await page.waitForTimeout(3000 + Math.random() * 2000);
      }
    }

    const result = {
      total:       batch.length,
      unchanged:   pulseProgress.unchanged,
      changed:     pulseProgress.changed,
      failed:      pulseProgress.failed,
      completedAt: new Date().toISOString(),
    };

    setProgress({ status: 'done', current: null });
    glog.info(`[Pulse] Done — unchanged: ${result.unchanged}, changed: ${result.changed}, failed: ${result.failed}`);
    return result;

  } catch (e) {
    setProgress({ status: 'error', error: e.message });
    glog.error('[Pulse] Failed', e);
    throw e;
  } finally {
    if (browser) {
      try { await browser.close(); } catch { /* ignore */ }
    }
  }
}
