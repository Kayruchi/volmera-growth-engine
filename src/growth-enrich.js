// growth-enrich.js — Job 3: Enrich Profile Data
// Primary enrichment job. For each Fetched row: Brave Search queries (person + company),
// Claude ICP scoring based on combined real operation signals.
// Status: Fetched → Enriched. No LinkedIn interaction.

import { getRowsByStatus, batchUpdateRows, appendBlacklist, deleteContactRow } from './growth-sheets.js';
import { glog } from './growth-logger.js';

const BRAVE_URL    = 'https://api.search.brave.com/res/v1/web/search';
const CLAUDE_URL   = 'https://api.anthropic.com/v1/messages';
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

  const updates     = [];  // rows to write back as Enriched
  const toBlacklist = [];  // rows flagged by Claude as blacklist candidates

  try {
    // ── Load Fetched rows ─────────────────────────────────────────────────────
    const rows = await getRowsByStatus('Fetched');
    const batch = rows.slice(0, limit);
    setProgress({ total: batch.length });
    glog.info(`[Enrich] ${rows.length} Fetched rows found, processing ${batch.length}`);

    if (batch.length === 0) {
      glog.warn('[Enrich] No Fetched rows — run Batch 2 (Fetch) first');
      setProgress({ status: 'done' });
      return { total: 0, enriched: 0, failed: 0 };
    }

    for (const row of batch) {
      setProgress({ current: row.profileUrl, done: enrichProgress.done });

      try {
        const data = await enrichProfile(row);

        if (data.shouldBlacklist) {
          // Claude flagged this company — queue for blacklist processing
          toBlacklist.push({ row, reason: data.blacklistReason });
          glog.info(`[Enrich] BLACKLIST — ${row.name} @ ${row.company} | reason: ${data.blacklistReason}`);
        } else {
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
        }

      } catch (e) {
        setProgress({ failed: enrichProgress.failed + 1 });
        glog.error(`[Enrich] Failed ${row.profileUrl}: ${e.message}`);
        // Leave as Fetched — retried on next run
      }

      setProgress({ done: enrichProgress.done + 1 });

      // Polite delay between Brave requests (700–1100ms)
      if (enrichProgress.done < batch.length) {
        await sleep(700 + Math.random() * 400);
      }
    }

    // ── Write enriched rows ───────────────────────────────────────────────────
    if (updates.length > 0) {
      glog.info(`[Enrich] Writing ${updates.length} enriched rows to sheet...`);
      await batchUpdateRows(updates);
      glog.info('[Enrich] Sheet updated');
    }

    // ── Process blacklisted rows ──────────────────────────────────────────────
    // Append to Blacklist first, then delete from Contacts in reverse rowIndex
    // order so earlier indices are not shifted by prior deletions.
    if (toBlacklist.length > 0) {
      glog.info(`[Enrich] Moving ${toBlacklist.length} blacklisted contact(s) out of Contacts...`);
      for (const { row, reason } of toBlacklist) {
        await appendBlacklist({ name: row.name, profileUrl: row.profileUrl, reason });
        glog.info(`[Enrich] Appended to Blacklist — ${row.name} | ${reason}`);
      }
      // Delete in reverse rowIndex order to avoid index drift
      const sorted = [...toBlacklist].sort((a, b) => b.row.rowIndex - a.row.rowIndex);
      for (const { row } of sorted) {
        await deleteContactRow(row.rowIndex);
        glog.info(`[Enrich] Deleted from Contacts — ${row.name} (row ${row.rowIndex})`);
      }
    }

    const result = {
      total:        batch.length,
      enriched:     enrichProgress.enriched,
      blacklisted:  toBlacklist.length,
      failed:       enrichProgress.failed,
      completedAt:  new Date().toISOString(),
    };

    setProgress({ status: 'done' });
    glog.info(`[Enrich] Done — enriched: ${result.enriched}, blacklisted: ${result.blacklisted}, failed: ${result.failed}`);
    return result;

  } catch (e) {
    setProgress({ status: 'error', error: e.message });
    glog.error('[Enrich] Failed', e);
    throw e;
  }
}

// ── ENRICH ONE PROFILE ────────────────────────────────────────────────────────

export async function enrichProfile(row) {
  const name    = row.name    || '';
  // "Not working" is written to the sheet when isCurrentRole===false — strip it here
  // so Brave searches use the real last company name, not a display label.
  const company = (row.company === 'Not working' ? '' : row.company) || '';

  // ── Round 1: 4 parallel searches ─────────────────────────────────────────
  // Person context + 3 company-specific angles to find concrete scale signals.
  // Company searches are skipped if no company is known (between jobs / not loaded).
  const [personResults, generalResults, fleetResults, scaleResults] = await Promise.all([
    braveSearch(`${name} ${company} Brasil`.trim()),
    company ? braveSearch(`"${company}" logística operações Brasil`) : Promise.resolve(''),
    company ? braveSearch(`"${company}" frota caminhões instalações armazéns`) : Promise.resolve(''),
    company ? braveSearch(`"${company}" faturamento funcionários filiais receita`) : Promise.resolve(''),
  ]);

  // Combine company research results
  let companyResearch = [generalResults, fleetResults, scaleResults].filter(Boolean).join('\n').slice(0, 4000);

  // ── Round 2: targeted follow-up if Round 1 has no concrete scale numbers ──
  // Looks for any numeric signal: truck count, facility count, revenue figure, headcount.
  const hasConcreteNumbers = /\b\d{2,}[\s+]*(caminhões|trucks|veículos|vehicles|instalações|facilities|filiais|armazéns|funcionários|employees|milhões|bilhões|million|billion)\b/i.test(companyResearch);

  if (!hasConcreteNumbers && company) {
    // Try news, annual reports, company website
    const [newsResults, siteResults] = await Promise.all([
      braveSearch(`"${company}" notícia OR relatório anual operação logística Brasil`),
      braveSearch(`"${company}" site oficial sobre empresa frota capacidade`),
    ]);
    companyResearch += '\n' + [newsResults, siteResults].filter(Boolean).join('\n');
    companyResearch = companyResearch.slice(0, 5000);
  }

  return await claudeScore(row, personResults, companyResearch);
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
    .slice(0, 2000);
}

// ── CLAUDE ICP SCORING ────────────────────────────────────────────────────────

const SYSTEM_PROMPT = `You are an ICP scoring expert for Volmera — a Brazilian Yard Management System (YMS) SaaS.

## Volmera Products
1. **Volmera YMS** — Yard Management System: dock scheduling, truck slot booking, real-time yard visibility, detention cost reduction. Automatic line-up: when a scheduled truck misses its slot, the system pulls the longest-waiting truck in the queue to fill the empty dock — eliminating idle dock time and cutting detention costs automatically.
2. **Volmera Freight Marketplace** — Eliminates empty return trips and backhaul inefficiency. Connects carriers and shippers for freight matching.
3. **Volmera Pallet Marketplace** — Connects pallet manufacturers directly with buyers. Reduces procurement friction.

## Ideal Customer Profile (ICP)
Any Brazilian company with significant physical goods movement. Target industries include (but are not limited to):
- **Agribusiness & food:** grain traders, soy/corn exporters, sugar mills, fertiliser distributors, food producers, slaughterhouses (frigoríficos), cold chain operators
- **Logistics & transport:** 3PL operators, freight carriers, terminal operators, port logistics, distribution centres, last-mile operators
- **Manufacturing & industry:** high-volume manufacturers, automotive parts, chemical, steel, consumer goods with large warehousing operations
- **Retail & distribution:** large-scale retail distribution, wholesalers with multiple distribution centres
- **Energy & construction:** companies with large equipment/material yards requiring dock and slot management

**YMS target operation baseline:** 3+ facilities (docks/terminals/warehouses) AND 40+ trucks per day. Below this threshold, a YMS adds limited value.

**Role seniority for scoring:**
- Decision maker (Director, VP, Head of, Gerente, COO, Operations Manager, Supply Chain Manager, Logistics Director) = can buy or strongly influence purchase → higher score
- Influencer (Supervisor, Analyst, Coordinator, Specialist, Process Engineer) = can influence but cannot decide alone → medium score
- Contributor/intern/individual role without team scope = unlikely to buy → lower score

## ICP Score (1–10)
- **9–10**: Perfect — decision maker at a large Brazilian operation (logistics/agribusiness/manufacturing/3PL) with clear YMS need (multiple facilities, high truck volume)
- **7–8**: Strong — decision maker at a mid-size operation, OR senior influencer at a large operation
- **5–6**: Possible — right industry but unclear scale, or right scale but non-decision role
- **3–4**: Weak — adjacent or small-scale industry, or junior role
- **1–2**: Not a fit — software company, YMS competitor, unrelated service industry, or intern/student

**Operation size inference rules (apply when search results are thin):**
- A "Diretor", "Gerente", "Head of", or "VP" at any logistics/agribusiness/manufacturing company → at minimum Medium
- Company name contains "Logística", "Transportes", "Frigorífico", "Agroindustrial", "Armazém", "Terminal", "Porto", "Distribuidora", "Indústria", "Alimentos", "Grãos" → infer industrial/logistics, likely Medium+
- 3PL operators, cold chain, grain traders, fertiliser distributors, slaughterhouses → typically Large
- Small retail/service companies → Small
- When truly no signals exist, use "Small–Medium (inferred)" — never output just "Unknown"

**icpReason format:** Always return exactly 3 bullet points as a single string, each starting with "• ", separated by newline. Keep each bullet under 15 words. Cover: (1) role/seniority, (2) company/industry fit, (3) facility count and daily truck volume. Write entirely in English — translate job titles and company types, never use Portuguese words in your output.

## Blacklist Detection
You must also determine if this contact should be blacklisted. Set shouldBlacklist to true if the company is:
- A software development house, IT services firm, or tech consultancy
- A company that provides Yard Management System (YMS) software or services (our direct competitors)
- A company that provides WMS, TMS, or ERP software to logistics/transport companies (software vendors)
- Examples of blacklist types: TOTVS, SAP Brasil, Senior Sistemas, GoRamp, C3 Solutions, any SaaS/software company targeting logistics

Do NOT blacklist: actual logistics operators, agribusiness companies, manufacturers, 3PLs, distributors — even if they use software. Only blacklist companies whose core business IS software/tech.

If shouldBlacklist is true, set blacklistReason to a short phrase: "YMS competitor", "logistics software vendor", "software development house", or similar.

**Role currency:** You receive a person search and a company research block (multiple search angles). Cross-check the scraped title/company against both. If the person has left the company or the role is clearly from the past, lower the score by 3–4 points and flag it in bullet 1. Use the company research to assess operation scale independently.

**opEstimate — STRICT FORMAT, NO EXCEPTIONS:**
Two numbers only — nothing else allowed in this field:
1. How many facilities (docks, terminals, warehouses, distribution centres) in Brazil?
2. What is the estimated daily truck volume across those facilities?

Output format — exactly this, no deviations:
- Confirmed data:  "196 facilities, ~500 trucks/day"
- Estimated data:  "~5 facilities, ~80 trucks/day"
- Inferred data:   "~3 facilities, ~40 trucks/day"

FORBIDDEN in opEstimate (put these in icpReason bullet 3 instead):
- Source citations: no "(source: confirmed)", no "— 96 DCs, 100 branches", no parentheses
- Revenue, CNPJ count, geographic coverage, LinkedIn followers
- Any text beyond the two numbers and "facilities" / "trucks/day"
- Never vague — always output specific numbers even when estimated

You must respond with ONLY valid JSON, no explanation, no markdown fences. Always include shouldBlacklist and blacklistReason.`;

async function claudeScore(row, personSearchResults, companySearchResults) {
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
      ? `- Role status: UNVERIFIED — Experience section did not load; use search results to confirm if still employed`
      : `- Role status: CURRENT — date: ${row.roleDate || 'Present'}`;

  const userMsg = `Profile to score:
- Name: ${row.name || '(unknown)'}
- Title: ${displayTitle}
- Company: ${row.company || '(unknown)'}
- LinkedIn: ${row.profileUrl}
${roleDateLine}

--- PERSON SEARCH ("${row.name} ${row.company}") ---
${personSearchResults || '(no results)'}

--- COMPANY RESEARCH ("${row.company}" — fleet, facilities, financials, news) ---
${companySearchResults || '(no results)'}

Score this person's ICP fit for Volmera. Instructions:
1. Role: assess seniority — decision maker, influencer, or contributor?
2. Currency: is the scraped role still current? Check person search for evidence.
3. Scale: from the company research, find how many physical facilities (docks, terminals, warehouses, DCs) this company runs in Brazil AND what the daily truck volume is. These are the ONLY two numbers needed for opEstimate.
4. Industry: logistics, agribusiness, manufacturing, 3PL, cold chain, distribution?
5. Product fit: which Volmera product matches this company's core pain?

Respond with this exact JSON:
{
  "relevantProds": "YMS / Freight Marketplace / Pallet Marketplace — or a combination",
  "opEstimate": "X facilities, ~Y trucks/day (source: confirmed/estimated/inferred)",
  "icpReason": "• Role: ...\\n• Industry: ...\\n• Scale: X facilities, ~Y trucks/day",
  "icpScore": 7,
  "shouldBlacklist": false,
  "blacklistReason": ""
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
  const icpScore = isPastRole ? Math.min(rawScore, 5)
                 : isUnknown  ? Math.min(rawScore, 7)
                 : rawScore;

  return {
    relevantProds:   String(parsed.relevantProds   || '').slice(0, 200),
    opEstimate:      String(parsed.opEstimate       || 'Unknown').slice(0, 150),
    icpReason:       String(parsed.icpReason        || '').slice(0, 600),
    icpScore,
    shouldBlacklist: parsed.shouldBlacklist === true,
    blacklistReason: String(parsed.blacklistReason  || '').slice(0, 100),
  };
}

// ── HELPERS ───────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
