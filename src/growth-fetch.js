// growth-fetch.js — Job 2: Fetch Profile Data
// Playwright visits each Scraped profile URL, extracts Name, Title, Company.
// Status: Scraped → Fetched. No enrichment — that is Job 3 (growth-enrich.js).

import { getLinkedInPage } from './growth-browser.js';
import { getRowsByStatus, batchUpdateRows } from './growth-sheets.js';
import { glog } from './growth-logger.js';

export const DEFAULT_FETCH_LIMIT = 50;

// ── URL NORMALIZER ────────────────────────────────────────────────────────────
// LinkedIn profile URLs sometimes have a 2-letter locale suffix appended by browsers
// or scraping tools: /in/username/en/ or /in/username/pt/
// These locale suffixes cause LinkedIn to serve a DIFFERENT person's profile.
// Always strip them before navigating to a profile.
export function normalizeLinkedInUrl(url) {
  if (!url) return url;
  // Strip trailing 2-letter locale suffix from profile URLs
  // e.g., /in/rui-cacela/en/ → /in/rui-cacela/
  return url.replace(/(\/in\/[^/?#]+)\/[a-z]{2}\/?$/, '$1/');
}

// ── JUNK COMPANY FILTER ───────────────────────────────────────────────────────
// LinkedIn shows placeholder texts in the Experience section when a profile has
// no real experience entered. These must never be stored as company names.
// Exported so growth-pulse.js can reuse the same filter.
export function isLinkedInPlaceholder(text) {
  if (!text || !text.trim()) return true;
  const t = text.trim().toLowerCase();
  return (
    t.includes('will appear here') ||        // "Experience that X adds will appear here."
    t.includes('adds will appear') ||
    t === 'nothing to see for now' ||
    t === 'no experience listed' ||
    t === 'experience' ||
    t === 'experiência' ||
    t === 'no experiences listed' ||
    t.startsWith('experience that') ||        // catch all variants of this pattern
    t.includes('nothing here yet') ||
    t.includes('no content available')
  );
}

// ── IN-MEMORY PROGRESS ────────────────────────────────────────────────────────
export const fetchProgress = {
  status:    'idle',   // idle | running | done | error
  total:     0,
  done:      0,
  fetched:   0,
  failed:    0,
  current:   null,
  error:     null,
  updatedAt: null,
};

function setProgress(update) {
  Object.assign(fetchProgress, update, { updatedAt: new Date().toISOString() });
}

// ── MAIN EXPORT ───────────────────────────────────────────────────────────────

export async function runFetch({ limit = DEFAULT_FETCH_LIMIT } = {}) {
  setProgress({ status: 'running', total: 0, done: 0, fetched: 0, failed: 0, current: null, error: null });
  glog.info(`[Fetch] Starting — limit: ${limit}`);

  let browser;

  try {
    // ── Load Scraped rows ─────────────────────────────────────────────────────
    const rows = await getRowsByStatus('Scraped');
    const batch = rows.slice(0, limit);
    setProgress({ total: batch.length });
    glog.info(`[Fetch] ${rows.length} Scraped rows found, processing ${batch.length}`);

    if (batch.length === 0) {
      glog.warn('[Fetch] No Scraped rows — nothing to do');
      setProgress({ status: 'done' });
      return { total: 0, fetched: 0, failed: 0 };
    }

    // ── Launch LinkedIn browser ───────────────────────────────────────────────
    const session = await getLinkedInPage();
    browser = session.browser;
    const page = session.page;

    for (const row of batch) {
      setProgress({ current: row.profileUrl });

      try {
        const data = await scrapeProfile(page, row.profileUrl);

        // Write Name/Title/Company/Location + Fetched status.
        // If isCurrentRole === false (confirmed past role), show "Not working" in Company.
        const displayCompany = data.isCurrentRole === false ? 'Not working' : data.company;
        await batchUpdateRows([{
          rowIndex: row.rowIndex,
          data: { name: data.name, title: data.title, company: displayCompany, location: data.location || '', status: 'Fetched' },
        }]);
        setProgress({ fetched: fetchProgress.fetched + 1 });
        glog.info(`[Fetch] Scraped — "${data.name}" | "${data.title}" | "${data.company}" | current: ${data.isCurrentRole} | date: "${data.roleDate}"`);

      } catch (e) {
        setProgress({ failed: fetchProgress.failed + 1 });
        glog.error(`[Fetch] Scrape failed ${row.profileUrl}: ${e.message}`);
        // Row stays as Scraped — retried on next Job 2 run
      }

      setProgress({ done: fetchProgress.done + 1 });

      // LinkedIn polite delay between profile visits
      if (fetchProgress.done < batch.length) {
        await page.waitForTimeout(3000 + Math.random() * 2000);
      }
    }

    const result = {
      total:       batch.length,
      fetched:     fetchProgress.fetched,
      failed:      fetchProgress.failed,
      completedAt: new Date().toISOString(),
    };

    setProgress({ status: 'done', current: null });
    glog.info(`[Fetch] Done — fetched: ${result.fetched}, failed: ${result.failed}`);
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

export async function scrapeProfile(page, profileUrl) {
  const safeUrl = normalizeLinkedInUrl(profileUrl);
  if (safeUrl !== profileUrl) {
    glog.warn(`[Fetch] URL normalized: ${profileUrl} → ${safeUrl}`);
  }
  await page.goto(safeUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
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

  // Detect dead / deleted profiles — check page text before scrolling
  const pageText = await page.evaluate(() => (document.body.innerText || '').slice(0, 500));
  if (
    pageText.includes("This page doesn't exist") ||
    pageText.includes("Página não encontrada") ||
    pageText.includes("Page not found") ||
    pageText.includes("profile is not available")
  ) {
    throw new Error(`Dead profile — page does not exist: ${profileUrl}`);
  }


  // Scroll in 4 stages. LinkedIn uses IntersectionObserver to lazy-load Experience section.
  // We scroll both the window and the main container to cover all LinkedIn layouts.
  // The final pass is done TWICE — first pass triggers the lazy load, second pass
  // scrolls any newly rendered content (longer profiles push Experience further down).
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
  await page.waitForTimeout(2500);  // wait for Experience section to render

  // Click any "Show all X experiences" / "Ver todas as X experiências" expand buttons.
  // LinkedIn collapses long Experience sections — must click before reading body text.
  try {
    await page.evaluate(() => {
      const buttons = Array.from(document.querySelectorAll('button, a'));
      for (const el of buttons) {
        const txt = (el.innerText || el.textContent || '').trim().toLowerCase();
        if (
          (txt.includes('show all') && (txt.includes('experience') || txt.includes('experiência') || txt.includes('position'))) ||
          (txt.includes('ver tod') && txt.includes('experiên')) ||
          (txt.includes('mostrar') && txt.includes('experiên'))
        ) {
          el.click();
          break;
        }
      }
    });
    await page.waitForTimeout(1500);  // wait for expansion to render
  } catch { /* ignore — button may not exist */ }

  // Second bottom pass — page may have grown taller after lazy-load rendered Experience
  await page.evaluate(() => {
    const main = document.querySelector('.scaffold-layout__main, main, #main') || document.documentElement;
    window.scrollTo(0, 99999);
    main.scrollTop = 99999;
  });
  await page.waitForTimeout(2000);

  const result = await page.evaluate(() => {
    const allLines = (document.body.innerText || '')
      .split('\n').map(s => s.trim()).filter(Boolean);

    const NAV_ITEMS = new Set([
      'Home','My Network','Jobs','Messaging','Notifications','Me',
      'For Business','Learning','More','Connect','Message','Follow',
      'Pending','Open to','Hiring','Promote profile',
    ]);
    // Section headers and UI labels that must never be treated as names
    const NOT_A_NAME = new Set([
      'Experience','Experiência','Education','Educação','Skills','Habilidades',
      'Highlights','About','Sobre','Featured','Recommendations','Recomendações',
      'Activity','Atividade','Interests','Volunteer','Projects','Languages','Idiomas',
      'Certifications','Certificações','Contact info','Show all','See more','See less',
    ]);

    // ── (1) Name — LinkedIn has no h1; name is the first non-nav, non-header line after "Learning".
    // Names always contain at least one space (first + last name).
    // Nav bar ends with "Learning"; profile content starts immediately after.
    let name = '';
    let learningIdx = -1;
    for (let i = 0; i < allLines.length; i++) {
      if (allLines[i] === 'Learning') learningIdx = i;
    }
    if (learningIdx >= 0) {
      for (let i = learningIdx + 1; i < Math.min(learningIdx + 8, allLines.length); i++) {
        const l = allLines[i];
        if (l && l.includes(' ') && l.length > 2 && l.length < 80
            && !l.match(/^\d/) && !NAV_ITEMS.has(l) && !NOT_A_NAME.has(l)) {
          name = l;
          break;
        }
      }
    }
    // Fallback: first two-word line without digits that isn't a nav item or section header
    if (!name) {
      for (const l of allLines.slice(0, 30)) {
        if (l.length > 3 && l.length < 60 && !l.match(/\d/) && l.includes(' ')
            && !NAV_ITEMS.has(l) && !NOT_A_NAME.has(l)) {
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
        // Skip blank/short lines, year/month date lines, and aggregate duration lines like "10 yrs 11 mos"
        if (!l || l.length < 3 || YEAR_RE.test(l) || MONTH_RE.test(l) || DURATION_RE.test(l)) continue;
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

        for (let k = j + 1; k < Math.min(j + 15, expLines.length); k++) {
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

        // Push if we have a date, OR if we have at least a title/company (date was beyond lookahead).
        // dateFound=false entries are treated as past roles (isPresent=false, isCurrentRole stays false).
        if (dateFound || resolvedTitle || resolvedCompany) {
          entries.push({ title: resolvedTitle, company: resolvedCompany, isPresent, date: entryDate });
        }
      }

      const presentEntry = entries.find(e => e.isPresent);
      const anyDateFound = entries.some(e => e.date);
      const current = presentEntry || entries[0];
      // null = section not found/parsed (no entries at all)
      // true  = Present date confirmed
      // false = section parsed with dates, no Present → confirmed past role
      // null  = section parsed but NO date lines found at all → treat as unverified (null)
      isCurrentRole = entries.length === 0 ? null
                    : presentEntry          ? true
                    : anyDateFound          ? false
                    : null;  // entries exist but no dates — unverified (soft cap at 7, not hard cap at 5)
      if (current) { title = current.title; company = current.company; roleDate = current.date; }
    }

    // Title fallback intentionally removed — title must come from Experience section only.
    // The LinkedIn headline (top of page) is user-customized and unreliable as a job title.
    // If Experience section didn't load, title stays empty.

    // ── Fallback for company — intro card shows current employer after "Contact info"
    // Many LinkedIn UI strings can leak here — filter them all out aggressively.
    const JUNK_LINE = (l) =>
      !l || l.length < 2 || l.length > 120 ||
      /^\d/.test(l) ||                                        // starts with digit (counts)
      l === '·' || l.startsWith('·') ||
      l === 'See all' || l === 'Show all' ||
      EMPLOYMENT_TYPES.has(l) ||
      DURATION_RE.test(l) ||
      NOT_A_NAME.has(l) ||
      /\b(connection|follower|following)\b/i.test(l) ||       // "500 connections", "1,234 followers"
      /busca|oportunidade|open to work|looking for/i.test(l); // "Em busca de nova oportunidade"

    if (!company) {
      const contactIdx = allLines.indexOf('Contact info');
      if (contactIdx >= 0) {
        for (let i = contactIdx + 1; i < Math.min(contactIdx + 8, allLines.length); i++) {
          const l = allLines[i];
          if (!JUNK_LINE(l)) {
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

  // ── Clean junk company text (runs in Node.js — NOT inside page.evaluate) ────
  // LinkedIn placeholder texts are stored as the company name when a profile has no
  // real experience entered. Detect them here and treat the person as "Not working".
  if (isLinkedInPlaceholder(result.company)) {
    result.company      = '';
    if (result.isCurrentRole !== true) result.isCurrentRole = false;
  }

  // ── /details/experience/ fallback ─────────────────────────────────────────────
  // LinkedIn hides the Experience section on the main profile page for non-connected
  // profiles. When no Experience data was extracted (title empty, isCurrentRole null),
  // navigate to the /details/experience/ sub-page which always renders the full section.
  if (!result.title && result.isCurrentRole === null) {
    try {
      const username = profileUrl.split('/in/')[1]?.replace(/\/.*$/, '');
      if (username) {
        await page.goto(`https://www.linkedin.com/in/${username}/details/experience/`, {
          waitUntil: 'domcontentloaded', timeout: 20000,
        });
        await page.waitForTimeout(3000);

        const expData = await page.evaluate(() => {
          const lines = (document.body.innerText || '')
            .split('\n').map(s => s.trim()).filter(Boolean);

          const YEAR_RE     = /\d{4}/;
          const PRESENT_RE  = /\bpresent\b|\batual\b|\bo momento\b|\bpresente\b|\batualmente\b/i;
          const MONTH_RE    = /^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|Fev|Abr|Mai|Ago|Set|Out|Dez)/i;
          const DURATION_RE = /\d+\s*(yr|mo|ano|mê|mes)/i;
          const SECTION_END = /^(Education|Educação|Skills|Habilidades|Languages|Idiomas|Certifications|Certificações|Recomendações|Interests|Volunteer|Projects)$/i;
          const EMPLOYMENT_TYPES = new Set([
            'Full-time','Part-time','Contract','Freelance','Internship','Self-employed','Seasonal',
            'Tempo integral','Meio período','Autônomo','Estágio','Temporário',
          ]);
          const WORK_MODES = new Set(['On-site','Remote','Hybrid','Presencial','Remoto','Híbrido','On site']);
          const JOB_TITLE_RE = /\b(gerente|diretor|director|manager|coordenador|supervisor|analista|analyst|engenheiro|engineer|head of|\bvp\b|vice.?president|\bceo\b|\bcoo\b|\bcfo\b|\bcto\b|presidente|president|especialista|specialist|líder|lider|\blead\b|chefe|responsável|responsavel|\bcoord\b)\b/i;

          const expIdx = lines.findIndex(l => l === 'Experience' || l === 'Experiência' || l === 'Cargo atual');
          if (expIdx < 0) return null;

          const expLines = [];
          for (let i = expIdx + 1; i < lines.length; i++) {
            if (SECTION_END.test(lines[i])) break;
            expLines.push(lines[i]);
          }

          const entries = [];
          for (let j = 0; j < expLines.length; j++) {
            const l = expLines[j];
            if (!l || l.length < 3 || YEAR_RE.test(l) || MONTH_RE.test(l) || DURATION_RE.test(l)) continue;
            const lastSeg = l.includes(' · ') ? l.split(' · ').pop().trim() : l.trim();
            if (WORK_MODES.has(lastSeg)) continue;
            if (!l.includes(' · ') && l.includes(', ') && /\b(Brazil|Brasil|São Paulo|Rio de Janeiro|Minas Gerais|Paraná|Santa Catarina|Goiás|Mato Grosso|Bahia|Pernambuco|Ceará|Espírito Santo)\b/i.test(l)) continue;

            let dateFound = false, isPresent = false, entryCompany = '', entryTitle = '', entryDate = '';
            const outerIsCompanyHeader = !JOB_TITLE_RE.test(l);

            for (let k = j + 1; k < Math.min(j + 15, expLines.length); k++) {
              const next = expLines[k];
              if (!next) continue;
              if (YEAR_RE.test(next) || MONTH_RE.test(next)) {
                dateFound = true;
                isPresent = PRESENT_RE.test(next);
                entryDate = next;
                break;
              }
              if (DURATION_RE.test(next) || EMPLOYMENT_TYPES.has(next) || /^\d/.test(next)) continue;
              const nextLastSeg = next.includes(' · ') ? next.split(' · ').pop().trim() : next.trim();
              if (WORK_MODES.has(nextLastSeg)) continue;
              const candidate = next.includes(' · ') ? next.split(' · ')[0].trim() : next;
              if (!candidate || DURATION_RE.test(candidate) || /^\d/.test(candidate)) continue;
              if (outerIsCompanyHeader) {
                if (!entryTitle)   entryTitle   = candidate;
                if (!entryCompany) entryCompany = l.includes(' · ') ? l.split(' · ')[0].trim() : l;
              } else {
                if (!entryCompany) entryCompany = candidate;
              }
            }

            const resolvedTitle   = outerIsCompanyHeader ? entryTitle : l;
            const resolvedCompany = outerIsCompanyHeader ? entryCompany : entryCompany;
            if (dateFound || resolvedTitle || resolvedCompany) {
              entries.push({ title: resolvedTitle, company: resolvedCompany, isPresent, date: entryDate });
            }
          }

          if (entries.length === 0) return null;
          const presentEntry = entries.find(e => e.isPresent);
          const anyDateFound = entries.some(e => e.date);
          const current = presentEntry || entries[0];
          const isCurrentRole = presentEntry ? true : anyDateFound ? false : null;
          return { title: current.title || '', company: current.company || '', isCurrentRole, roleDate: current.date || '' };
        });

        if (expData && (expData.title || expData.company)) {
          result.title         = expData.title;
          result.isCurrentRole = expData.isCurrentRole;
          result.roleDate      = expData.roleDate;
          // Only overwrite company if Experience gave us a real (non-junk) value
          if (expData.company && !isLinkedInPlaceholder(expData.company)) result.company = expData.company;
        }
      }
    } catch (e) {
      glog.warn(`[Fetch] details/experience fallback failed for ${profileUrl}: ${e.message}`);
    }
  }

  return result;
}
