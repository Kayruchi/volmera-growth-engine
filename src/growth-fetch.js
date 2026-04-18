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
        glog.info(`[Fetch] Scraped — "${data.name}" | "${data.title}" | "${data.company}" | current: ${data.isCurrentRole} | date: "${data.roleDate}"`);

        // ── Fire background enrich — runs during next LinkedIn delay ──────────
        // Merges scraped data into the row object so enrichProfile has name/company
        const rowWithData = { ...row, name: data.name, title: data.title, company: data.company, isCurrentRole: data.isCurrentRole, roleDate: data.roleDate };
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


  // Scroll in 3 stages. LinkedIn may use a container div instead of window scroll,
  // so we scroll both the window and the main content container.
  await page.evaluate(() => {
    const main = document.querySelector('.scaffold-layout__main, main, #main') || document.documentElement;
    window.scrollTo(0, 900);
    main.scrollTop = 900;
  });
  await page.waitForTimeout(1200);
  await page.evaluate(() => {
    const main = document.querySelector('.scaffold-layout__main, main, #main') || document.documentElement;
    window.scrollTo(0, 1800);
    main.scrollTop = 1800;
  });
  await page.waitForTimeout(1200);
  await page.evaluate(() => {
    const main = document.querySelector('.scaffold-layout__main, main, #main') || document.documentElement;
    window.scrollTo(0, 99999);
    main.scrollTop = 99999;
  });
  await page.waitForTimeout(3000);

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
    const PRESENT_RE = /\bpresent\b|\batual\b|\bo momento\b|\bpresente\b|\batualmente\b/i;
    const MONTH_RE   = /^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|Fev|Abr|Mai|Ago|Set|Out|Dez)/i;
    const SECTION_END = /^(Education|Educação|Skills|Habilidades|Languages|Idiomas|Certifications|Certificações|Recomendações|Interests|Volunteer|Projects)$/i;
    // Lines that look like durations ("3 yrs 2 mos", "2 anos 4 meses") or employment types
    const DURATION_RE = /\d+\s*(yr|mo|ano|mê|mes)/i;
    const EMPLOYMENT_TYPES = new Set([
      'Full-time','Part-time','Contract','Freelance','Internship','Self-employed','Seasonal',
      'Tempo integral','Meio período','Autônomo','Estágio','Temporário',
    ]);
    // Words that only appear in job titles/seniority — NOT in company names.
    // Used to detect whether an Experience outer line is a job title (standard layout)
    // or a company group header (grouped layout like "DHL Supply Chain → title below").
    // Do NOT add industry words (logistics, supply chain, operações) — they appear in company names.
    const JOB_TITLE_RE = /\b(gerente|diretor|director|manager|coordenador|supervisor|analista|analyst|engenheiro|engineer|head of|\bvp\b|vice.?president|\bceo\b|\bcoo\b|\bcfo\b|\bcto\b|presidente|president|especialista|specialist|líder|lider|\blead\b|chefe|responsável|responsavel|\bcoord\b)\b/i;
    // Work mode labels LinkedIn appends to location lines: "São Paulo, Brazil · On-site"
    // These must be filtered out — they are NOT job titles or company names.
    const WORK_MODES = new Set(['On-site','Remote','Hybrid','Presencial','Remoto','Híbrido','On site']);

    let title = '', company = '', isCurrentRole = null, roleDate = '';
    // isCurrentRole: null = Experience section not found/parsed
    //                true = confirmed current (Present date found)
    //                false = confirmed past (no Present in any entry)

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
        // Skip location lines: "São Paulo, Brazil · On-site" / "Ribeirão Preto, São Paulo, Brazil"
        // Detected by: last segment after ' · ' is a work mode, OR line ends with a known work mode
        const lastSeg = l.includes(' · ') ? l.split(' · ').pop().trim() : l.trim();
        if (WORK_MODES.has(lastSeg)) continue;
        // Also skip lines that look like "City, State, Country" with no job/company signal
        if (!l.includes(' · ') && l.includes(', ') && /\b(Brazil|Brasil|São Paulo|Rio de Janeiro|Minas Gerais|Paraná|Santa Catarina|Goiás|Mato Grosso|Bahia|Pernambuco|Ceará|Espírito Santo)\b/i.test(l)) continue;

        let dateFound = false, isPresent = false, entryCompany = '', entryTitle = '', entryDate = '';

        // Detect LinkedIn grouped layout: outer line is a company group header (no job title words)
        // vs standard layout: outer line is the job title itself.
        const outerIsCompanyHeader = !JOB_TITLE_RE.test(l);

        for (let k = j + 1; k < Math.min(j + 8, expLines.length); k++) {
          const next = expLines[k];
          if (!next) continue;
          if (YEAR_RE.test(next) || MONTH_RE.test(next)) {
            dateFound = true;
            isPresent = PRESENT_RE.test(next);
            entryDate = next; // e.g. "Jan 2020 - Present" or "Mar 2018 - Dec 2024"
            break;
          }
          // Skip duration strings, employment type labels, work modes, and digit-only artifacts
          if (DURATION_RE.test(next) || EMPLOYMENT_TYPES.has(next) || /^\d/.test(next)) continue;
          const nextLastSeg = next.includes(' · ') ? next.split(' · ').pop().trim() : next.trim();
          if (WORK_MODES.has(nextLastSeg)) continue;

          const candidate = next.includes(' · ') ? next.split(' · ')[0].trim() : next;
          if (!candidate || DURATION_RE.test(candidate) || /^\d/.test(candidate)) continue;

          if (outerIsCompanyHeader) {
            // Grouped layout — outer = company, first valid inner = actual job title
            if (!entryTitle)   entryTitle   = candidate;
            if (!entryCompany) entryCompany = l.includes(' · ') ? l.split(' · ')[0].trim() : l;
          } else {
            // Standard layout — outer = title (l), first valid inner = company
            if (!entryCompany) entryCompany = candidate;
          }
        }

        // In grouped layout with no title found (company header → Full-time → date),
        // leave title blank — better than storing the company name as title.
        const resolvedTitle   = outerIsCompanyHeader ? entryTitle   : l;
        const resolvedCompany = outerIsCompanyHeader ? entryCompany : entryCompany;

        if (dateFound) entries.push({ title: resolvedTitle, company: resolvedCompany, isPresent, date: entryDate });
      }

      const presentEntry = entries.find(e => e.isPresent);
      const current = presentEntry || entries[0];
      // null = section not parsed; true = Present found; false = section parsed, no Present
      isCurrentRole = entries.length > 0 ? !!presentEntry : null;
      if (current) { title = current.title; company = current.company; roleDate = current.date; }
    }

    // Title fallback intentionally removed — title must come from Experience section only.
    // The LinkedIn headline (top of page) is user-customized and unreliable as a job title.
    // If Experience section didn't load, title stays empty.

    // ── Fallback for company — intro card shows current employer after "Contact info"
    if (!company) {
      const contactIdx = allLines.indexOf('Contact info');
      if (contactIdx >= 0) {
        for (let i = contactIdx + 1; i < Math.min(contactIdx + 5, allLines.length); i++) {
          const l = allLines[i];
          if (l && l.length > 1 && !l.match(/^\d/) && l !== '·' && !l.startsWith('·') && l !== 'See all'
              && !EMPLOYMENT_TYPES.has(l) && !DURATION_RE.test(l)) {
            company = l;
            break;
          }
        }
      }
    }

    return { name, title, company, location, isCurrentRole, roleDate };
  });

  if (!result.name) {
    throw new Error(`Name empty — LinkedIn body did not contain a recognisable name after the nav`);
  }

  return result;
}
