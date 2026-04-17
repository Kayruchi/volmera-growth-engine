// growth-crawl.js — Job 1: LinkedIn Profile URL Crawler
// Playwright navigates LinkedIn search results, extracts profile URLs,
// deduplicates against Google Sheet, writes new ones with status "Scraped".
// Target: 50 unique URLs per run, max 5 pages.

import { getLinkedInPage } from './growth-browser.js';
import { getAllRows, appendRows } from './growth-sheets.js';
import { glog } from './growth-logger.js';

// ── DEFAULT ICP SEARCH URL ────────────────────────────────────────────────────
// 2nd+3rd connections, Brazil only, logistics/supply chain keywords
export const DEFAULT_SEARCH_URL =
  'https://www.linkedin.com/search/results/people/?' +
  'keywords=Gerente%20de%20Log%C3%ADstica%20OR%20Diretor%20de%20Log%C3%ADstica%20OR%20' +
  'Supply%20Chain%20Manager%20OR%20Terminal%20Manager%20OR%20Gerente%20de%20Opera%C3%A7%C3%B5es' +
  '&network=%5B%22S%22%2C%22O%22%5D' +
  '&geoUrn=%5B%22106057199%22%5D';

const TARGET_UNIQUE = 50;
const MAX_PAGES     = 5;

// ── IN-MEMORY PROGRESS (polled by dashboard every 2s) ────────────────────────
export const crawlProgress = {
  status:     'idle',   // idle | running | done | error
  page:       0,
  totalPages: MAX_PAGES,
  unique:     0,
  dupes:      0,
  currentUrl: null,
  error:      null,
  updatedAt:  null,
};

function setProgress(update) {
  Object.assign(crawlProgress, update, { updatedAt: new Date().toISOString() });
}

// ── MAIN EXPORT ───────────────────────────────────────────────────────────────

export async function runCrawl({ searchUrl = DEFAULT_SEARCH_URL } = {}) {
  setProgress({ status: 'running', page: 0, unique: 0, dupes: 0, currentUrl: searchUrl, error: null });
  glog.info(`[Crawl] Starting — target: ${TARGET_UNIQUE} unique, max ${MAX_PAGES} pages`);
  glog.info(`[Crawl] URL: ${searchUrl}`);

  let browser;
  const newUrls   = [];   // ordered list of new unique profile URLs
  const newUrlSet = new Set();
  let dupesFound  = 0;
  let pagesScraped = 0;
  let lastPageUrl = searchUrl;

  try {
    // ── Load existing URLs from sheet (dedup source) ──────────────────────────
    glog.info('[Crawl] Loading existing URLs from sheet for dedup...');
    const existingRows = await getAllRows();
    const existingSet  = new Set(
      existingRows.map(r => normalizeUrl(r.profileUrl)).filter(Boolean)
    );
    glog.info(`[Crawl] ${existingSet.size} existing URLs in sheet`);

    // ── Launch browser ────────────────────────────────────────────────────────
    const session = await getLinkedInPage();
    browser = session.browser;
    const page = session.page;

    // ── Crawl pages ───────────────────────────────────────────────────────────
    for (let pageNum = 1; pageNum <= MAX_PAGES; pageNum++) {
      if (newUrls.length >= TARGET_UNIQUE) break;

      const pageUrl = buildPageUrl(searchUrl, pageNum);
      lastPageUrl   = pageUrl;
      setProgress({ page: pageNum, currentUrl: pageUrl });
      glog.info(`[Crawl] Navigating to page ${pageNum}...`);

      await page.goto(pageUrl, { waitUntil: 'domcontentloaded', timeout: 35000 });

      // Wait for dynamic content + scroll to trigger lazy load
      await page.waitForTimeout(2500 + Math.random() * 1000);
      await page.keyboard.press('End');
      await page.waitForTimeout(1200);

      // ── Extract profile URLs from this page ───────────────────────────────
      const candidates = await extractProfileUrls(page);
      glog.info(`[Crawl] Page ${pageNum}: ${candidates.length} candidates`);

      if (candidates.length === 0) {
        glog.warn(`[Crawl] Page ${pageNum}: no results — stopping early`);
        break;
      }

      pagesScraped = pageNum;

      for (const url of candidates) {
        const norm = normalizeUrl(url);
        if (!norm) continue;

        if (existingSet.has(norm) || newUrlSet.has(norm)) {
          dupesFound++;
        } else {
          newUrls.push(norm);
          newUrlSet.add(norm);
          if (newUrls.length >= TARGET_UNIQUE) break;
        }
      }

      setProgress({ unique: newUrls.length, dupes: dupesFound });
      glog.info(`[Crawl] Running totals — unique: ${newUrls.length}, dupes: ${dupesFound}`);

      // Polite delay between pages (1.5–2.5s)
      if (pageNum < MAX_PAGES && newUrls.length < TARGET_UNIQUE) {
        await page.waitForTimeout(1500 + Math.random() * 1000);
      }
    }

    // ── Write to Google Sheet (batch) ─────────────────────────────────────────
    if (newUrls.length > 0) {
      glog.info(`[Crawl] Writing ${newUrls.length} URLs to sheet...`);
      await appendRows(newUrls.map(url => ({ profileUrl: url, status: 'Scraped' })));
      glog.info('[Crawl] Sheet updated');
    } else {
      glog.warn('[Crawl] No new unique URLs found — sheet not updated');
    }

    const result = {
      date:        formatDate(new Date()),
      unique:      newUrls.length,
      dupes:       dupesFound,
      pages:       pagesScraped,
      lastUrl:     lastPageUrl,
      completedAt: new Date().toISOString(),
    };

    setProgress({ status: 'done', unique: newUrls.length, dupes: dupesFound });
    glog.info(`[Crawl] Done — ${newUrls.length} unique, ${dupesFound} dupes, ${pagesScraped} pages`);
    return result;

  } catch (e) {
    setProgress({ status: 'error', error: e.message });
    glog.error('[Crawl] Failed', e);
    throw e;
  } finally {
    if (browser) {
      try { await browser.close(); } catch { /* ignore */ }
    }
  }
}

// ── HELPERS ───────────────────────────────────────────────────────────────────

/**
 * Extract profile URLs from current LinkedIn search results page.
 * Uses multiple selector strategies for resilience against LinkedIn DOM changes.
 */
async function extractProfileUrls(page) {
  return page.evaluate(() => {
    const results = [];
    const seen    = new Set();

    // Strategy 1: standard result list items
    const containers = Array.from(document.querySelectorAll(
      'li.reusable-search__result-container, ' +
      'li[class*="result-container"], ' +
      '.entity-result, ' +
      '[data-view-name="search-entity-result-universal-template"]'
    ));

    for (const container of containers) {
      // Find profile link
      const link = container.querySelector('a[href*="/in/"]');
      if (!link) continue;

      const href = link.getAttribute('href') || '';
      const match = href.match(/\/in\/([a-zA-Z0-9_%-]+)/);
      if (!match) continue;

      const username = decodeURIComponent(match[1]).toLowerCase();
      if (seen.has(username)) continue;

      // ── Check button state ────────────────────────────────────────────────
      // Collect all button/role=button elements and their labels
      const buttons  = Array.from(container.querySelectorAll('button, [role="button"]'));
      let hasPending = false;
      let hasAction  = false;

      for (const btn of buttons) {
        const label = (
          btn.getAttribute('aria-label') ||
          btn.innerText                  ||
          btn.textContent                || ''
        ).toLowerCase().trim();

        if (label.includes('pending'))                                         { hasPending = true; break; }
        if (label.includes('connect') || label.includes('follow') ||
            label.includes('message') || label.includes('withdraw'))           { hasAction = true; }
      }

      if (hasPending) continue;

      // Fallback: check container text if no buttons found
      if (!hasAction) {
        const text = (container.innerText || container.textContent || '').toLowerCase();
        if (text.includes('pending'))                                          continue;
        if (!text.includes('connect') && !text.includes('follow') &&
            !text.includes('message'))                                         continue;
      }

      seen.add(username);
      results.push('https://www.linkedin.com/in/' + match[1]);
    }

    // Strategy 2 fallback — if strategy 1 found nothing, try generic link scan
    if (results.length === 0) {
      const allLinks = Array.from(document.querySelectorAll('a[href*="/in/"]'));
      for (const link of allLinks) {
        const href  = link.getAttribute('href') || '';
        const match = href.match(/\/in\/([a-zA-Z0-9_%-]+)/);
        if (!match) continue;

        const username = decodeURIComponent(match[1]).toLowerCase();
        // Skip own profile, company pages, etc.
        if (username.length < 3) continue;
        if (seen.has(username)) continue;

        // Only include links in result list area (not sidebar/nav)
        const inNav = link.closest('nav, header, footer, [class*="sidebar"]');
        if (inNav) continue;

        seen.add(username);
        results.push('https://www.linkedin.com/in/' + match[1]);
      }
    }

    return results;
  });
}

/**
 * Append pagination start param to a LinkedIn search URL.
 * LinkedIn uses start=0 (page 1), start=10 (page 2), start=20 (page 3), etc.
 */
function buildPageUrl(baseUrl, pageNum) {
  try {
    const url = new URL(baseUrl);
    url.searchParams.set('start', String((pageNum - 1) * 10));
    return url.toString();
  } catch {
    // If URL parsing fails, append manually
    const sep = baseUrl.includes('?') ? '&' : '?';
    return `${baseUrl}${sep}start=${(pageNum - 1) * 10}`;
  }
}

/**
 * Normalize a LinkedIn profile URL to a clean canonical form.
 * Strips tracking params, query strings, and trailing slashes.
 */
function normalizeUrl(url) {
  if (!url) return null;
  const match = String(url).match(/linkedin\.com\/in\/([a-zA-Z0-9_%-]+)/);
  if (!match) return null;
  return `https://www.linkedin.com/in/${match[1]}`;
}

function formatDate(d) {
  const dd = String(d.getDate()).padStart(2, '0');
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const yy = d.getFullYear();
  return `${dd}-${mm}-${yy}`;
}
