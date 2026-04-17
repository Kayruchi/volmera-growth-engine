// growth-fetch.js — Job 2: Fetch & Enrich Profile Data
// For each Scraped profile: Brave Search → Claude ICP analysis → write back to sheet.
// Target: 50 profiles per run (configurable). No LinkedIn interaction — API only.

import { getRowsByStatus, batchUpdateRows } from './growth-sheets.js';
import { glog } from './growth-logger.js';

const BRAVE_URL   = 'https://api.search.brave.com/res/v1/web/search';
const CLAUDE_URL  = 'https://api.anthropic.com/v1/messages';
const CLAUDE_MODEL = 'claude-sonnet-4-6';

export const DEFAULT_FETCH_LIMIT = 50;

// ── IN-MEMORY PROGRESS ────────────────────────────────────────────────────────
export const fetchProgress = {
  status:    'idle',   // idle | running | done | error
  total:     0,
  done:      0,
  enriched:  0,
  rejected:  0,
  failed:    0,
  current:   null,    // profile URL currently being processed
  error:     null,
  updatedAt: null,
};

function setProgress(update) {
  Object.assign(fetchProgress, update, { updatedAt: new Date().toISOString() });
}

// ── MAIN EXPORT ───────────────────────────────────────────────────────────────

export async function runFetch({ limit = DEFAULT_FETCH_LIMIT } = {}) {
  setProgress({ status: 'running', total: 0, done: 0, enriched: 0, rejected: 0, failed: 0, current: null, error: null });
  glog.info(`[Fetch] Starting — limit: ${limit}`);

  const updates = [];

  try {
    // ── Load Scraped rows ─────────────────────────────────────────────────────
    const rows = await getRowsByStatus('Scraped');
    const batch = rows.slice(0, limit);
    setProgress({ total: batch.length });
    glog.info(`[Fetch] ${rows.length} Scraped rows found, processing ${batch.length}`);

    if (batch.length === 0) {
      glog.warn('[Fetch] No Scraped rows to enrich');
      setProgress({ status: 'done' });
      return { enriched: 0, rejected: 0, failed: 0, total: 0 };
    }

    // ── Process each profile ──────────────────────────────────────────────────
    for (const row of batch) {
      setProgress({ current: row.profileUrl, done: fetchProgress.done });

      try {
        const data = await enrichProfile(row.profileUrl);

        const newStatus = (data.icpScore >= 5) ? 'Enriched' : 'Rejected';

        updates.push({
          rowIndex: row.rowIndex,
          data: {
            name:          data.name,
            title:         data.title,
            company:       data.company,
            relevantProds: data.relevantProds,
            opEstimate:    data.opEstimate,
            icpReason:     data.icpReason,
            icpScore:      data.icpScore,
            status:        newStatus,
          },
        });

        if (newStatus === 'Enriched') {
          setProgress({ enriched: fetchProgress.enriched + 1 });
          glog.info(`[Fetch] Enriched ${row.profileUrl} — score: ${data.icpScore}, status: ${newStatus}`);
        } else {
          setProgress({ rejected: fetchProgress.rejected + 1 });
          glog.info(`[Fetch] Rejected ${row.profileUrl} — score: ${data.icpScore} (below 5)`);
        }

      } catch (e) {
        setProgress({ failed: fetchProgress.failed + 1 });
        glog.warn(`[Fetch] Failed to enrich ${row.profileUrl}: ${e.message}`);
        // Leave status as Scraped — will be retried next run
      }

      setProgress({ done: fetchProgress.done + 1 });

      // Polite delay between profiles (600–1000ms) to respect Brave rate limits
      if (fetchProgress.done < batch.length) {
        await sleep(600 + Math.random() * 400);
      }
    }

    // ── Batch write all updates ───────────────────────────────────────────────
    if (updates.length > 0) {
      glog.info(`[Fetch] Writing ${updates.length} enriched rows to sheet...`);
      await batchUpdateRows(updates);
      glog.info('[Fetch] Sheet updated');
    }

    const result = {
      total:     batch.length,
      enriched:  fetchProgress.enriched,
      rejected:  fetchProgress.rejected,
      failed:    fetchProgress.failed,
      completedAt: new Date().toISOString(),
    };

    setProgress({ status: 'done' });
    glog.info(`[Fetch] Done — enriched: ${result.enriched}, rejected: ${result.rejected}, failed: ${result.failed}`);
    return result;

  } catch (e) {
    setProgress({ status: 'error', error: e.message });
    glog.error('[Fetch] Failed', e);
    throw e;
  }
}

// ── ENRICH SINGLE PROFILE ─────────────────────────────────────────────────────

async function enrichProfile(profileUrl) {
  const username = extractUsername(profileUrl);

  // 1. Brave Search
  const searchResults = await braveSearch(
    `"${username}" linkedin Brazil logistics supply chain operations`
  );

  // 2. Claude ICP Analysis
  return await claudeAnalyze(profileUrl, username, searchResults);
}

// ── BRAVE SEARCH ──────────────────────────────────────────────────────────────

async function braveSearch(query) {
  const url = `${BRAVE_URL}?q=${encodeURIComponent(query)}&count=5&country=BR&lang=pt`;
  const res = await fetch(url, {
    headers: {
      'Accept': 'application/json',
      'Accept-Encoding': 'gzip',
      'X-Subscription-Token': process.env.BRAVE_SEARCH_API_KEY,
    },
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Brave Search error ${res.status}: ${body.slice(0, 200)}`);
  }

  const data = await res.json();
  const results = data.web?.results || [];

  // Return condensed text: title + description for each result
  return results.map(r => `${r.title || ''}: ${r.description || ''}`).join('\n').slice(0, 2000);
}

// ── CLAUDE ICP ANALYSIS ───────────────────────────────────────────────────────

const SYSTEM_PROMPT = `You are an ICP scoring assistant for Volmera — a Brazilian Yard Management System (YMS) SaaS.

Volmera's three products:
1. Volmera YMS — dock scheduling, truck slot booking, real-time yard visibility, detention cost reduction. Target: logistics managers, operations directors, terminal managers at agribusiness, manufacturers, 3PLs with high truck volume.
2. Volmera Freight Marketplace — solves empty return trips and backhaul inefficiency. Target: logistics/fleet managers.
3. Volmera Pallet Marketplace — connects pallet buyers with manufacturers. Target: procurement and supply chain managers.

ICP: Brazilian logistics/operations professionals at companies with significant physical goods movement. Ideal titles: Gerente de Logística, Diretor de Logística, Supply Chain Manager, Terminal Manager, Gerente de Operações, Head of Operations, COO at mid-to-large companies.

Score 1–10:
- 9–10: Perfect fit — title + company size + industry all match
- 7–8: Strong fit — right title or right industry
- 5–6: Possible fit — adjacent role or unclear company
- 3–4: Weak fit — tangential role or small company
- 1–2: Not a fit

You must respond with ONLY valid JSON, no explanation, no markdown.`;

async function claudeAnalyze(profileUrl, username, searchResults) {
  const userMsg = `LinkedIn profile: ${profileUrl}
Username: ${username}

Brave Search results:
${searchResults || '(no results found)'}

Extract available information and score this person's ICP fit for Volmera. If data is sparse, make a reasonable inference from the username and any available context.

Respond with this exact JSON structure:
{
  "name": "Full Name or empty string if unknown",
  "title": "Job title or empty string",
  "company": "Company name or empty string",
  "relevantProds": "Which Volmera products fit (YMS / Freight Marketplace / Pallet Marketplace) — pick the most relevant",
  "opEstimate": "Brief estimate of their operation scale: Small / Medium / Large / Unknown",
  "icpReason": "1–2 sentences explaining the ICP score",
  "icpScore": 7
}`;

  const res = await fetch(CLAUDE_URL, {
    method: 'POST',
    headers: {
      'x-api-key':          process.env.ANTHROPIC_API_KEY,
      'anthropic-version':  '2023-06-01',
      'content-type':       'application/json',
    },
    body: JSON.stringify({
      model:      CLAUDE_MODEL,
      max_tokens: 400,
      system:     SYSTEM_PROMPT,
      messages:   [{ role: 'user', content: userMsg }],
    }),
  });

  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Claude API error ${res.status}: ${err.slice(0, 200)}`);
  }

  const json = await res.json();
  const text = json.content[0].text.trim();

  // Parse JSON — strip any accidental markdown fencing
  const clean = text.replace(/^```json?\s*/i, '').replace(/\s*```$/, '').trim();
  const parsed = JSON.parse(clean);

  // Validate required fields
  return {
    name:          String(parsed.name          || '').slice(0, 100),
    title:         String(parsed.title         || '').slice(0, 100),
    company:       String(parsed.company       || '').slice(0, 100),
    relevantProds: String(parsed.relevantProds || '').slice(0, 200),
    opEstimate:    String(parsed.opEstimate    || 'Unknown').slice(0, 50),
    icpReason:     String(parsed.icpReason     || '').slice(0, 300),
    icpScore:      Math.min(10, Math.max(1, Number(parsed.icpScore) || 1)),
  };
}

// ── HELPERS ───────────────────────────────────────────────────────────────────

function extractUsername(profileUrl) {
  const match = String(profileUrl).match(/linkedin\.com\/in\/([^/?#]+)/);
  return match ? decodeURIComponent(match[1]) : profileUrl;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
