// growth-enrich.js — Job 3: Enrich Profile Data
// For each Fetched row: Brave Search research on name + company,
// Claude ICP scoring based on real operation signals.
// Status: Fetched → Enriched. No LinkedIn interaction.

import { getRowsByStatus, batchUpdateRows } from './growth-sheets.js';
import { glog } from './growth-logger.js';

const BRAVE_URL   = 'https://api.search.brave.com/res/v1/web/search';
const CLAUDE_URL  = 'https://api.anthropic.com/v1/messages';
const CLAUDE_MODEL = 'claude-sonnet-4-6';

export const DEFAULT_ENRICH_LIMIT = 50;

// ── IN-MEMORY PROGRESS ────────────────────────────────────────────────────────
export const enrichProgress = {
  status:    'idle',
  total:     0,
  done:      0,
  enriched:  0,
  failed:    0,
  current:   null,
  error:     null,
  updatedAt: null,
};

function setProgress(update) {
  Object.assign(enrichProgress, update, { updatedAt: new Date().toISOString() });
}

// ── MAIN EXPORT ───────────────────────────────────────────────────────────────

export async function runEnrich({ limit = DEFAULT_ENRICH_LIMIT } = {}) {
  setProgress({ status: 'running', total: 0, done: 0, enriched: 0, failed: 0, current: null, error: null });
  glog.info(`[Enrich] Starting — limit: ${limit}`);

  const updates = [];

  try {
    // ── Load Fetched rows ─────────────────────────────────────────────────────
    const rows = await getRowsByStatus('Fetched');
    const batch = rows.slice(0, limit);
    setProgress({ total: batch.length });
    glog.info(`[Enrich] ${rows.length} Fetched rows found, processing ${batch.length}`);

    if (batch.length === 0) {
      glog.warn('[Enrich] No Fetched rows — run Job 2 (Fetch) first');
      setProgress({ status: 'done' });
      return { total: 0, enriched: 0, failed: 0 };
    }

    for (const row of batch) {
      setProgress({ current: row.profileUrl, done: enrichProgress.done });

      try {
        const data = await enrichProfile(row);

        updates.push({
          rowIndex: row.rowIndex,
          data: {
            relevantProds: data.relevantProds,
            opEstimate:    data.opEstimate,
            icpReason:     data.icpReason,
            icpScore:      data.icpScore,
            status:        'Enriched',
          },
        });

        setProgress({ enriched: enrichProgress.enriched + 1 });
        glog.info(`[Enrich] ${row.name} @ ${row.company} — score: ${data.icpScore} | ${data.relevantProds}`);

      } catch (e) {
        setProgress({ failed: enrichProgress.failed + 1 });
        glog.warn(`[Enrich] Failed ${row.profileUrl}: ${e.message}`);
        // Leave as Fetched — retried on next run
      }

      setProgress({ done: enrichProgress.done + 1 });

      // Polite delay between Brave requests (700–1100ms)
      if (enrichProgress.done < batch.length) {
        await sleep(700 + Math.random() * 400);
      }
    }

    // ── Batch write ───────────────────────────────────────────────────────────
    if (updates.length > 0) {
      glog.info(`[Enrich] Writing ${updates.length} rows to sheet...`);
      await batchUpdateRows(updates);
      glog.info('[Enrich] Sheet updated');
    }

    const result = {
      total:       batch.length,
      enriched:    enrichProgress.enriched,
      failed:      enrichProgress.failed,
      completedAt: new Date().toISOString(),
    };

    setProgress({ status: 'done' });
    glog.info(`[Enrich] Done — enriched: ${result.enriched}, failed: ${result.failed}`);
    return result;

  } catch (e) {
    setProgress({ status: 'error', error: e.message });
    glog.error('[Enrich] Failed', e);
    throw e;
  }
}

// ── ENRICH ONE PROFILE ────────────────────────────────────────────────────────

export async function enrichProfile(row, onBraveDone) {
  // Build search query from real data scraped in Batch 2
  const query = [row.name, row.company, 'logística', 'Brasil']
    .filter(Boolean).join(' ');

  const searchResults = await braveSearch(query);
  onBraveDone?.();  // fires after Brave, before Claude — used by Batch 2 pipeline for live tracking
  return await claudeScore(row, searchResults);
}

// ── BRAVE SEARCH ──────────────────────────────────────────────────────────────

async function braveSearch(query) {
  const url = `${BRAVE_URL}?q=${encodeURIComponent(query)}&count=5&country=BR`;
  const res = await fetch(url, {
    headers: {
      'Accept': 'application/json',
      'Accept-Encoding': 'gzip',
      'X-Subscription-Token': process.env.BRAVE_SEARCH_API_KEY,
    },
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Brave ${res.status}: ${body.slice(0, 150)}`);
  }

  const data = await res.json();
  return (data.web?.results || [])
    .map(r => `${r.title || ''}: ${r.description || ''}`)
    .join('\n')
    .slice(0, 2500);
}

// ── CLAUDE ICP SCORING ────────────────────────────────────────────────────────

const SYSTEM_PROMPT = `You are an ICP scoring expert for Volmera — a Brazilian Yard Management System (YMS) SaaS for the logistics and agribusiness sector.

## Volmera Products
1. **Volmera YMS** — Yard Management System: dock scheduling, truck slot booking, real-time yard visibility, detention cost reduction. Automatic line-up: when a scheduled truck misses its slot, the system pulls the longest-waiting truck in the queue to fill the empty dock — eliminating idle dock time and cutting detention costs automatically.
2. **Volmera Freight Marketplace** — Eliminates empty return trips and backhaul inefficiency. Connects carriers and shippers for freight matching.
3. **Volmera Pallet Marketplace** — Connects pallet manufacturers directly with buyers. Reduces procurement friction.

## Ideal Customer Profile (ICP)
Companies with significant physical goods movement in Brazil — especially agribusiness, high-volume manufacturers, 3PL operators, terminal operators.

**YMS target operation baseline:** 3+ facilities (docks/terminals/warehouses) AND 40+ trucks per day. Below this threshold, a YMS adds limited value.

**Role seniority for scoring:**
- Decision maker (Director, VP, Head of, Gerente, COO, Operations Manager) = can buy or strongly influence purchase → higher score
- Influencer (Supervisor, Analyst, Coordinator, Specialist) = can influence but cannot decide alone → medium score
- Contributor/intern/individual role without team scope = unlikely to buy → lower score

## ICP Score (1–10)
- **9–10**: Perfect — decision maker at a large Brazilian logistics/agribusiness operation with clear YMS need (multiple facilities, high truck volume)
- **7–8**: Strong — decision maker at a mid-size operation, OR senior influencer at a large operation
- **5–6**: Possible — right industry but unclear scale, or right scale but non-decision role
- **3–4**: Weak — adjacent industry, small operation, or junior role
- **1–2**: Not a fit — software company, YMS competitor, unrelated industry, or intern/student

**Operation size inference rules (apply when search results are thin):**
- A "Diretor", "Gerente", "Head of", or "VP" at any logistics/agribusiness company → at minimum Medium
- Company name contains "Logística", "Transportes", "Frigorífico", "Agroindustrial", "Armazém", "Terminal", "Porto" → infer logistics/agribusiness, likely Medium+
- 3PL operators, cold chain, grain traders, fertiliser distributors → typically Large
- Small retail/service companies → Small
- When truly no signals exist, use "Small–Medium (inferred)" — never output just "Unknown"

**icpReason format:** Always return exactly 3 bullet points as a single string, each starting with "• ", separated by newline. Keep each bullet under 15 words. Cover: (1) role/seniority, (2) company/industry fit, (3) operation scale. Write entirely in English — translate job titles, never use Portuguese words.

**Role currency:** Cross-check the scraped title/company against the Brave search results. If the person has left the company or the role is clearly from the past, lower the score by 3–4 points and flag it in bullet 1.

You must respond with ONLY valid JSON, no explanation, no markdown fences.`;

async function claudeScore(row, searchResults) {
  // isCurrentRole: null = Experience section not parsed (unknown)
  //                true = Present date confirmed
  //                false = Experience section parsed, no Present found → past role
  const isPastRole = row.isCurrentRole === false;
  const isUnknown  = row.isCurrentRole === null || row.isCurrentRole === undefined;

  const displayTitle = isPastRole
    ? `${row.title} [FORMER ROLE — LinkedIn shows no current employment here]`
    : (row.title || '(unknown)');

  const roleDateLine = isPastRole
    ? `- Role status: PAST ROLE — date: ${row.roleDate || '(no Present on LinkedIn)'}`
    : isUnknown
      ? `- Role status: UNVERIFIED — Experience section did not load; use Brave results to confirm if still employed`
      : `- Role status: CURRENT — date: ${row.roleDate || 'Present'}`;

  const userMsg = `Profile to score:
- Name: ${row.name || '(unknown)'}
- Title: ${displayTitle}
- Company: ${row.company || '(unknown)'}
- LinkedIn: ${row.profileUrl}
${roleDateLine}
Brave Search results for "${row.name} ${row.company}":
${searchResults || '(no results — use title and company to infer)'}

Score this person's ICP fit for Volmera. Consider:
1. Their seniority level (decision maker / influencer / contributor)
2. Whether the scraped role is still their current position (check search results)
3. Their company's likely operation scale (number of facilities, truck volume) for YMS relevance
4. Industry fit (logistics, agribusiness, manufacturing, 3PL)
5. Which Volmera product fits best

Respond with this exact JSON:
{
  "relevantProds": "YMS / Freight Marketplace / Pallet Marketplace — or a combination e.g. YMS, Freight Marketplace",
  "opEstimate": "Large (10+ facilities, 200+ trucks/day) OR Medium (3-10 facilities, 40-200 trucks/day) OR Small (<3 facilities) — never Unknown, always infer",
  "icpReason": "• Role: ...\n• Industry: ...\n• Scale: ...",
  "icpScore": 7
}`;

  const res = await fetch(CLAUDE_URL, {
    method: 'POST',
    headers: {
      'x-api-key':         process.env.ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
      'content-type':      'application/json',
    },
    body: JSON.stringify({
      model:      CLAUDE_MODEL,
      max_tokens: 500,
      system:     SYSTEM_PROMPT,
      messages:   [{ role: 'user', content: userMsg }],
    }),
  });

  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Claude ${res.status}: ${err.slice(0, 150)}`);
  }

  const json = await res.json();
  const text = json.content[0].text.trim()
    .replace(/^```json?\s*/i, '').replace(/\s*```$/, '').trim();

  const parsed = JSON.parse(text);

  const rawScore = Math.min(10, Math.max(1, Number(parsed.icpScore) || 1));
  // Confirmed past role → hard cap at 5 (can't sell to someone with no current job)
  // Unverified (Experience section didn't load) → soft cap at 7 (benefit of doubt,
  // but prevents 9/10 on profiles we can't confirm are still active)
  const icpScore = isPastRole  ? Math.min(rawScore, 5)
                 : isUnknown   ? Math.min(rawScore, 7)
                 : rawScore;

  return {
    relevantProds: String(parsed.relevantProds || '').slice(0, 200),
    opEstimate:    String(parsed.opEstimate    || 'Unknown').slice(0, 100),
    icpReason:     String(parsed.icpReason     || '').slice(0, 600),
    icpScore,
  };
}

// ── HELPERS ───────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
