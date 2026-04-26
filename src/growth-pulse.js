// growth-pulse.js — Job 4: Pulse Check
// Playwright visits each eligible profile, compares current title/company to what's
// in the sheet. If changed → log to Changes tab + re-enrich. Either way → update Last Pulse date.
//
// Eligibility: lastPulse empty OR lastPulse older than 90 days.
// Priority order: Pending, Ready, Engaged, Followup, Lead first → then Enriched.
// Skipped statuses: Scraped, Fetched (not processed yet), Dead.

import { getLinkedInPage } from './growth-browser.js';
import { getAllRows, batchUpdateRows, appendChange, today, getBlacklist, appendBlacklist, updateBlacklistCheck, deleteContactRow, deleteBlacklistRow, appendRow } from './growth-sheets.js';
import { enrichProfile } from './growth-enrich.js';
import { scrapeProfile, isLinkedInPlaceholder } from './growth-fetch.js';
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
  const contactsToBlacklist = []; // collected during Sweep 1, deleted after loop

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
      if (!row.lastPulse) return true;  // never pulse-checked
      // lastPulse stored as dd-mm-yyyy — parse to Date for comparison
      const [dd, mm, yyyy] = row.lastPulse.split('-');
      return new Date(`${yyyy}-${mm}-${dd}`) < cutoff;
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
      const todayStr = today();

      try {
        const scraped = await scrapeProfile(page, row.profileUrl);

        // Normalise for comparison — treat empty strings as equivalent
        const oldTitle      = (row.title   || '').trim().toLowerCase();
        const oldCompany    = (row.company === 'Not working' ? '' : row.company || '').trim().toLowerCase();
        const newTitle      = (scraped.title   || '').trim().toLowerCase();
        const rawNewCompany = (scraped.company || '').trim();
        // Treat LinkedIn placeholder texts as empty — same filter as scrapeProfile
        const newCompany    = isLinkedInPlaceholder(rawNewCompany) ? '' : rawNewCompany.toLowerCase();

        // Title: only flag as changed if BOTH old and new are non-empty and differ.
        // If old was empty (Experience didn't load in Batch 2), getting a title now
        // is a data gap being filled — not a job change.
        const titleChanged   = oldTitle && newTitle && newTitle !== oldTitle;
        // Company: flag if new company is non-empty and differs from old.
        // "Not working" is mapped to '' above, so '' → new company = real job change.
        const companyChanged = newCompany && newCompany !== oldCompany;
        const changed = titleChanged || companyChanged;

        // Statuses that mean this person is already in the network.
        // Only these get logged to the Changes tab so Batch 6 can send a congrats message.
        // Enriched = not yet invited — job change is re-enriched silently, no Changes entry.
        const CONNECTED_STATUSES = new Set(['Pending', 'Ready', 'Engaged', 'Followup', 'Lead', 'Success']);

        if (changed) {
          const changeDesc = [
            titleChanged   ? `title: "${row.title}" → "${scraped.title}"` : null,
            companyChanged ? `company: "${row.company}" → "${scraped.company}"` : null,
          ].filter(Boolean).join(' | ');
          glog.info(`[Pulse] CHANGE detected — ${row.name} | ${changeDesc} | connected: ${CONNECTED_STATUSES.has(row.status)}`);

          // 1. Log to Changes tab ONLY for people already in the network.
          //    Enriched people who changed jobs are re-enriched silently — Batch 6 must not
          //    send them a congrats message before they've even accepted an invite.
          if (CONNECTED_STATUSES.has(row.status)) {
            await appendChange({
              profileUrl: row.profileUrl,
              name:       row.name,
              oldTitle:   row.title   || '',
              oldCompany: row.company || '',
            });
          }

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

            if (enrichData.shouldBlacklist) {
              // Person changed job to a blacklisted company — queue for removal
              glog.info(`[Pulse] BLACKLIST — ${row.name} now at blacklisted company. Reason: ${enrichData.blacklistReason}`);
              contactsToBlacklist.push({
                rowIndex:   row.rowIndex,
                name:       scraped.name || row.name,
                profileUrl: row.profileUrl,
                reason:     enrichData.blacklistReason || 'Detected by Batch 4 pulse check',
              });
            } else {
              await batchUpdateRows([{
                rowIndex: row.rowIndex,
                data: {
                  name:          scraped.name    || row.name,
                  title:         scraped.title,
                  company:       scraped.company || row.company,
                  relevantProds: enrichData.relevantProds,
                  opEstimate:    enrichData.opEstimate,
                  icpReason:     enrichData.icpReason,
                  icpScore:      enrichData.icpScore,
                  lastPulse:     todayStr,
                  // Write location only if scraped has one and sheet is still empty
                  ...(scraped.location && !row.location ? { location: scraped.location } : {}),
                },
              }]);
              glog.info(`[Pulse] Re-enriched — ${row.name} | new score: ${enrichData.icpScore}`);
            }
          } catch (enrichErr) {
            // Enrich failed — still update name/title/company and last pulse
            const safeCompany = scraped.isCurrentRole === false
              ? 'Not working'
              : (isLinkedInPlaceholder(scraped.company) ? row.company : scraped.company);
            await batchUpdateRows([{
              rowIndex: row.rowIndex,
              data: {
                name:      scraped.name    || row.name,
                title:     scraped.title,
                company:   safeCompany,
                lastPulse: todayStr,
                ...(scraped.location && !row.location ? { location: scraped.location } : {}),
              },
            }]);
            glog.error(`[Pulse] Re-enrich failed for ${row.profileUrl}: ${enrichErr.message}`);
          }

          setProgress({ changed: pulseProgress.changed + 1 });

        } else {
          // No change — update last pulse date and backfill location if still empty
          await batchUpdateRows([{
            rowIndex: row.rowIndex,
            data: {
              lastPulse: todayStr,
              ...(scraped.location && !row.location ? { location: scraped.location } : {}),
            },
          }]);
          setProgress({ unchanged: pulseProgress.unchanged + 1 });
          glog.info(`[Pulse] No change — ${row.name} @ ${row.company}`);
        }

      } catch (e) {
        setProgress({ failed: pulseProgress.failed + 1 });
        glog.error(`[Pulse] Failed ${row.profileUrl}: ${e.message}`);
        // Do NOT update lastPulse — row stays eligible so it can be retried
      }

      setProgress({ done: pulseProgress.done + 1 });

      // LinkedIn polite delay between profile visits
      if (pulseProgress.done < batch.length) {
        await page.waitForTimeout(3000 + Math.random() * 2000);
      }
    }

    // ── Post-loop: move blacklisted contacts to Blacklist tab ─────────────────
    if (contactsToBlacklist.length > 0) {
      glog.info(`[Pulse] Moving ${contactsToBlacklist.length} contact(s) to Blacklist…`);
      for (const entry of contactsToBlacklist) {
        try {
          await appendBlacklist({ name: entry.name, profileUrl: entry.profileUrl, reason: entry.reason });
        } catch (e) {
          glog.error(`[Pulse] appendBlacklist failed for ${entry.profileUrl}: ${e.message}`);
        }
      }
      // Delete in reverse rowIndex order to prevent index drift
      const sortedDesc = [...contactsToBlacklist].sort((a, b) => b.rowIndex - a.rowIndex);
      for (const entry of sortedDesc) {
        try {
          await deleteContactRow(entry.rowIndex);
          glog.info(`[Pulse] Deleted contact row ${entry.rowIndex} — ${entry.name}`);
        } catch (e) {
          glog.error(`[Pulse] deleteContactRow failed for row ${entry.rowIndex}: ${e.message}`);
        }
      }
    }

    // ── Sweep 2: re-check Blacklist people for job changes ────────────────────
    glog.info('[Pulse] Sweep 2 — checking Blacklist for rehabilitated contacts…');
    const blacklist = await getBlacklist();
    const blCutoff = new Date();
    blCutoff.setDate(blCutoff.getDate() - PULSE_INTERVAL_DAYS);

    const blEligible = blacklist.filter(bl => {
      if (!bl.batch4Check) return true;
      const [dd, mm, yyyy] = bl.batch4Check.split('-');
      return new Date(`${yyyy}-${mm}-${dd}`) < blCutoff;
    });

    glog.info(`[Pulse] Blacklist eligible for re-check: ${blEligible.length}`);

    // Reuse existing Playwright page if browser is still open
    let blPage = null;
    if (browser && blEligible.length > 0) {
      try { blPage = await browser.newPage(); } catch { /* browser closed, open a new one below */ }
    }

    let blBrowser = browser;
    if (!blBrowser && blEligible.length > 0) {
      const blSession = await getLinkedInPage();
      blBrowser = blSession.browser;
      blPage    = blSession.page;
    }

    const blToDelete = []; // blacklist rows to delete after loop

    for (const bl of blEligible) {
      if (!bl.profileUrl) continue;
      setProgress({ current: bl.profileUrl });
      try {
        const scraped = await scrapeProfile(blPage, bl.profileUrl);
        const rowForEnrich = {
          name:    scraped.name || bl.name,
          title:   scraped.title,
          company: scraped.company,
        };
        const enrichData = await enrichProfile(rowForEnrich);

        if (!enrichData.shouldBlacklist) {
          // Person is now at a clean company — move back to Contacts as Enriched
          glog.info(`[Pulse] Blacklist → Contacts: ${bl.name} — no longer at blacklisted company`);
          await appendRow({
            profileUrl:    bl.profileUrl,
            name:          scraped.name || bl.name,
            title:         scraped.title,
            company:       scraped.company,
            relevantProds: enrichData.relevantProds,
            opEstimate:    enrichData.opEstimate,
            icpReason:     enrichData.icpReason,
            icpScore:      enrichData.icpScore,
            status:        'Enriched',
          });
          blToDelete.push(bl);
        } else {
          // Still blacklisted — just update the check date
          await updateBlacklistCheck(bl.rowIndex, today());
          glog.info(`[Pulse] Blacklist check updated — ${bl.name} still at blacklisted company`);
        }
      } catch (e) {
        glog.error(`[Pulse] Blacklist re-check failed for ${bl.profileUrl}: ${e.message}`);
        // Don't update check date — allow retry next run
      }

      if (blPage && blEligible.indexOf(bl) < blEligible.length - 1) {
        await blPage.waitForTimeout(3000 + Math.random() * 2000);
      }
    }

    // Delete rehabilitated blacklist rows in reverse order
    const blSortedDesc = [...blToDelete].sort((a, b) => b.rowIndex - a.rowIndex);
    for (const bl of blSortedDesc) {
      try {
        await deleteBlacklistRow(bl.rowIndex);
        glog.info(`[Pulse] Deleted Blacklist row ${bl.rowIndex} — ${bl.name}`);
      } catch (e) {
        glog.error(`[Pulse] deleteBlacklistRow failed for row ${bl.rowIndex}: ${e.message}`);
      }
    }

    // Close Sweep 2 browser if it was opened separately
    if (blBrowser && blBrowser !== browser) {
      try { await blBrowser.close(); } catch { /* ignore */ }
    }

    const result = {
      total:           batch.length,
      unchanged:       pulseProgress.unchanged,
      changed:         pulseProgress.changed,
      failed:          pulseProgress.failed,
      blacklisted:     contactsToBlacklist.length,
      blChecked:       blEligible.length,
      blRehabilitated: blToDelete.length,
      completedAt:     new Date().toISOString(),
    };

    setProgress({ status: 'done', current: null });
    glog.info(`[Pulse] Done — unchanged: ${result.unchanged}, changed: ${result.changed}, failed: ${result.failed}, blacklisted: ${result.blacklisted}, bl-rehabilitated: ${result.blRehabilitated}`);
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

// ── STATS HELPER ──────────────────────────────────────────────────────────────
// Returns KPI counts for the dashboard card, applying the same eligibility
// filter the real job uses (lastPulse empty OR older than PULSE_INTERVAL_DAYS).

export async function getPulseStats() {
  const all = await getAllRows();
  const TRACKED_STATUSES = new Set(['Pending', 'Ready', 'Engaged', 'Followup', 'Lead', 'Enriched']);

  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - PULSE_INTERVAL_DAYS);

  const parseLastPulse = (lastPulse) => {
    if (!lastPulse) return null;
    const [dd, mm, yyyy] = lastPulse.split('-');
    return new Date(`${yyyy}-${mm}-${dd}`);
  };

  let activeEligible   = 0;  // lastPulse empty OR older than 90 days
  let recentlyPulsed   = 0;  // lastPulse within 90 days

  for (const row of all) {
    if (!TRACKED_STATUSES.has(row.status)) continue;
    const d = parseLastPulse(row.lastPulse);
    if (!d || d < cutoff) {
      activeEligible++;
    } else {
      recentlyPulsed++;
    }
  }

  return {
    activeEligible,
    recentlyPulsed,
    totalEligible: activeEligible + recentlyPulsed,
  };
}
