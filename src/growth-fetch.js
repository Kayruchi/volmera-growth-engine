// growth-fetch.js — Job 2: Fetch Profile Data (with pipeline enrich)
// Playwright visits each Scraped profile URL, extracts Name, Title, Company.
// Status: Scraped → Fetched → Enriched (inline, during LinkedIn delays).
// Job 3 (Enrich) remains as a standalone button for re-enriching Fetched rows.

import { getLinkedInPage } from './growth-browser.js';
import { getRowsByStatus, batchUpdateRows } from './growth-sheets.js';
import { enrichProfile } from './growth-enrich.js';
import { glog } from './growth-logger.js';

export const DEFAULT_FETCH_LIMIT = 50;

// ── IN-MEMORY PROGRESS ────────────────────────────────────────────────────────
export const fetchProgress = {
  status:       'idle',   // idle | running | done | error
  total:        0,
  done:         0,
  fetched:      0,
  braved:       0,   // rows where Brave Search completed
  enriched:     0,   // rows where Claude ICP scoring completed
  enrichFailed: 0,   // rows where background enrich failed (stay as Fetched for Batch 3 retry)
  failed:       0,
  current:      null,
  error:        null,
  updatedAt:    null,
};

function setProgress(update) {
  Object.assign(fetchProgress, update, { updatedAt: new Date().toISOString() });
}

// ── MAIN EXPORT ───────────────────────────────────────────────────────────────

export async function runFetch({ limit = DEFAULT_FETCH_LIMIT } = {}) {
  setProgress({ status: 'running', total: 0, done: 0, fetched: 0, braved: 0, enriched: 0, enrichFailed: 0, failed: 0, current: null, error: null });
  glog.info(`[Fetch] Starting — limit: ${limit}`);

  let browser;
  const enrichPromises = []; // background enrich tasks — run during LinkedIn delays

  try {
    // ── Load Scraped rows ─────────────────────────────────────────────────────
    const rows = await getRowsByStatus('Scraped');
    const batch = rows.slice(0, limit);
    setProgress({ total: batch.length });
    glog.info(`[Fetch] ${rows.length} Scraped rows found, processing ${batch.length}`);

    if (batch.length === 0) {
      glog.warn('[Fetch] No Scraped rows — nothing to do');
      setProgress({ status: 'done' });
      return { total: 0, fetched: 0, braved: 0, enriched: 0, failed: 0 };
    }

    // ── Launch LinkedIn browser ───────────────────────────────────────────────
    const session = await getLinkedInPage();
    browser = session.browser;
    const page = session.page;

    for (const row of batch) {
      setProgress({ current: row.profileUrl });

      try {
        const data = await scrapeProfile(page, row.profileUrl);

        // Write Name/Title/Company + Fetched status immediately
        await batchUpdateRows([{
          rowIndex: row.rowIndex,
          data: { name: data.name, title: data.title, company: data.company, status: 'Fetched' },
        }]);
        setProgress({ fetched: fetchProgress.fetched + 1 });
        glog.info(`[Fetch] Scraped — "${data.name}" | "${data.title}" | "${data.company}" | loc: "${data.location}"`);

        // ── Fire background enrich — runs during next LinkedIn delay ──────────
        // Merges scraped data into the row object so enrichProfile has name/company
        const rowWithData = { ...row, name: data.name, title: data.title, company: data.company };
        const enrichTask = enrichProfile(rowWithData, () => {
          setProgress({ braved: fetchProgress.braved + 1 });
        })
          .then(async (enrichData) => {
            await batchUpdateRows([{
              rowIndex: row.rowIndex,
              data: {
                relevantProds: enrichData.relevantProds,
                opEstimate:    enrichData.opEstimate,
                icpReason:     enrichData.icpReason,
                icpScore:      enrichData.icpScore,
                status:        'Enriched',
              },
            }]);
            setProgress({ enriched: fetchProgress.enriched + 1 });
            glog.info(`[Fetch] Enriched — "${data.name}" score: ${enrichData.icpScore} | ${enrichData.relevantProds}`);
          })
          .catch(e => {
            setProgress({ enrichFailed: fetchProgress.enrichFailed + 1 });
            glog.warn(`[Fetch] Background enrich failed for ${row.profileUrl}: ${e.message}`);
            // Row stays as Fetched — Job 3 will pick it up on next run
          });
        enrichPromises.push(enrichTask);

      } catch (e) {
        setProgress({ failed: fetchProgress.failed + 1 });
        glog.warn(`[Fetch] Scrape failed ${row.profileUrl}: ${e.message}`);
        // Row stays as Scraped — retried on next Job 2 run
      }

      setProgress({ done: fetchProgress.done + 1 });

      // LinkedIn polite delay (3–5s) — background enrich runs during this window
      if (fetchProgress.done < batch.length) {
        await page.waitForTimeout(3000 + Math.random() * 2000);
      }
    }

    // ── Wait for any still-running enriches ───────────────────────────────────
    if (enrichPromises.length > 0) {
      const pending = enrichPromises.length - fetchProgress.enriched - fetchProgress.enrichFailed;
      if (pending > 0) {
        setProgress({ current: `Waiting for ${pending} enrichment(s) to finish...` });
        glog.info(`[Fetch] Scraping done — waiting for ${pending} background enriches`);
      }
      await Promise.allSettled(enrichPromises);
    }

    const result = {
      total:        batch.length,
      fetched:      fetchProgress.fetched,
      braved:       fetchProgress.braved,
      enriched:     fetchProgress.enriched,
      enrichFailed: fetchProgress.enrichFailed,
      failed:       fetchProgress.failed,
      completedAt:  new Date().toISOString(),
    };

    setProgress({ status: 'done', current: null });
    glog.info(`[Fetch] Done — fetched: ${result.fetched}, enriched: ${result.enriched}, enrichFailed: ${result.enrichFailed}, failed: ${result.failed}`);
    return result;

  } catch (e) {
    setProgress({ status: 'error', error: e.message });
    glog.error('[Fetch] Failed', e);
    throw e;
  } finally {
    if (browser) {
      try { await browser.close(); } catch { /* ignore */ }
    }
  }
}

// ── PROFILE SCRAPER ───────────────────────────────────────────────────────────

async function scrapeProfile(page, profileUrl) {
  await page.goto(profileUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
  await page.waitForTimeout(4000); // fixed wait — no waitFor, no polling

  // Detect auth wall / login redirect
  const landedUrl = page.url();
  if (
    landedUrl.includes('/login') ||
    landedUrl.includes('/authwall') ||
    landedUrl.includes('/signup') ||
    landedUrl.includes('/checkpoint')
  ) {
    throw new Error(`Auth wall — redirected to ${landedUrl}. Session may be expired.`);
  }


  // Scroll to trigger lazy-load of experience section
  await page.keyboard.press('End');
  await page.waitForTimeout(1500);

  const result = await page.evaluate(() => {
    const allLines = (document.body.innerText || '')
      .split('\n').map(s => s.trim()).filter(Boolean);

    const NAV_ITEMS = new Set([
      'Home','My Network','Jobs','Messaging','Notifications','Me',
      'For Business','Learning','More','Connect','Message','Follow',
      'Pending','Open to','Hiring','Promote profile',
    ]);

    // ── (1) Name — LinkedIn has no h1; name is the first non-nav line after "Learning"
    // Nav bar ends with "Learning"; profile content starts immediately after.
    let name = '';
    let learningIdx = -1;
    for (let i = 0; i < allLines.length; i++) {
      if (allLines[i] === 'Learning') learningIdx = i;
    }
    if (learningIdx >= 0) {
      for (let i = learningIdx + 1; i < Math.min(learningIdx + 6, allLines.length); i++) {
        const l = allLines[i];
        if (l && l.length > 2 && l.length < 80 && !l.match(/^\d/) && !NAV_ITEMS.has(l)) {
          name = l;
          break;
        }
      }
    }
    // Fallback: first two-word line without digits that isn't a nav item
    if (!name) {
      for (const l of allLines.slice(0, 30)) {
        if (l.length > 3 && l.length < 60 && !l.match(/\d/) && l.includes(' ') && !NAV_ITEMS.has(l)) {
          name = l;
          break;
        }
      }
    }

    // ── (2) Location — "City, State, Country" pattern near name, for language detection
    let location = '';
    const nameIdx = name ? allLines.indexOf(name) : -1;
    if (nameIdx >= 0) {
      for (let i = nameIdx + 1; i < Math.min(nameIdx + 12, allLines.length); i++) {
        const l = allLines[i];
        if (l.toLowerCase().includes('connection') || l.toLowerCase().includes('follower')) break;
        if (l.includes(',') && !l.match(/\d/) && l.length > 4 && l.length < 80) {
          location = l;
          break;
        }
      }
    }

    // ── (3) Title + (4) Company — primary source: Experience section ──────────
    // Parse job entries; prefer "Present"/"Atual" entry, fall back to most recent.
    const YEAR_RE    = /\d{4}/;
    const PRESENT_RE = /\bpresent\b|\batual\b|\bo momento\b/i;
    const MONTH_RE   = /^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|Fev|Abr|Mai|Ago|Set|Out|Dez)/i;
    const SECTION_END = /^(Education|Educação|Skills|Habilidades|Languages|Idiomas|Certifications|Certificações|Recomendações|Interests|Volunteer|Projects)$/i;

    let title = '', company = '';

    const expIdx = allLines.findIndex(l =>
      l === 'Experience' || l === 'Experiência' || l === 'Cargo atual'
    );

    if (expIdx >= 0) {
      const expLines = [];
      for (let i = expIdx + 1; i < allLines.length; i++) {
        if (SECTION_END.test(allLines[i])) break;
        expLines.push(allLines[i]);
      }

      const entries = [];
      for (let j = 0; j < expLines.length; j++) {
        const l = expLines[j];
        if (!l || l.length < 3 || YEAR_RE.test(l) || MONTH_RE.test(l)) continue;

        let dateFound = false, isPresent = false, entryCompany = '';
        for (let k = j + 1; k < Math.min(j + 6, expLines.length); k++) {
          const next = expLines[k];
          if (!next) continue;
          if (YEAR_RE.test(next) || MONTH_RE.test(next)) {
            dateFound = true;
            isPresent = PRESENT_RE.test(next);
            break;
          }
          if (!entryCompany && next.length > 1) {
            entryCompany = next.includes(' · ') ? next.split(' · ')[0].trim() : next;
          }
        }

        if (dateFound) entries.push({ title: l, company: entryCompany, isPresent });
      }

      const current = entries.find(e => e.isPresent) || entries[0];
      if (current) { title = current.title; company = current.company; }
    }

    // ── Fallback for title — LinkedIn headline sits right after the name in the intro card
    if (!title && nameIdx >= 0) {
      for (let i = nameIdx + 1; i < Math.min(nameIdx + 6, allLines.length); i++) {
        const l = allLines[i];
        if (l && l.length > 3 && !NAV_ITEMS.has(l) && !l.startsWith('·') && l !== name) {
          title = l;
          break;
        }
      }
    }

    // ── Fallback for company — intro card shows current employer after "Contact info"
    if (!company) {
      const contactIdx = allLines.indexOf('Contact info');
      if (contactIdx >= 0) {
        for (let i = contactIdx + 1; i < Math.min(contactIdx + 5, allLines.length); i++) {
          const l = allLines[i];
          if (l && l.length > 1 && !l.match(/^\d/) && l !== '·' && !l.startsWith('·') && l !== 'See all') {
            company = l;
            break;
          }
        }
      }
    }

    return { name, title, company, location };
  });

  if (!result.name) {
    throw new Error(`Name empty — LinkedIn body did not contain a recognisable name after the nav`);
  }

  return result;
}
