// growth-connections.js — Batch 6: Network check + Marketing messages
//
// Step A (runConnectionsCheck): Opens LinkedIn connections page via Chrome extension,
//   scrapes new connections since last check, matches against Pending rows by
//   first name + company-in-headline + title-in-headline, flips status → Ready.
//
// Step B (generatePendingMessages): Claude generates personalized English messages
//   for each Ready person, stored in data-growth/pending-messages.json.
//
// Step C (approveAndSendMessage): Translates to local language, queues Chrome extension
//   to open profile → click Message → fill → send, updates sheet: Engaged.

import { getAllRows, batchUpdateRows, today } from './growth-sheets.js';
import { enqueueConnectionsScrape, enqueueMessage, awaitExtensionResult } from './growth-extension-queue.js';
import { glog } from './growth-logger.js';
import { writeFileSync, readFileSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname         = path.dirname(fileURLToPath(import.meta.url));
const PENDING_MSGS_FILE = path.join(__dirname, '..', 'data-growth', 'pending-messages.json');

const CLAUDE_URL      = 'https://api.anthropic.com/v1/messages';
const CLAUDE_MODEL    = 'claude-sonnet-4-6';
const CALENDAR_LINK   = 'https://calendar.app.google/B6KijgsHYc9CufEn9';

// ── PROGRESS STATE ────────────────────────────────────────────────────────────

export const connectionsProgress = {
  status:    'idle',   // idle | running | done | error
  found:     0,
  matched:   0,
  updatedAt: null,
};

function setProgress(update) {
  Object.assign(connectionsProgress, update, { updatedAt: new Date().toISOString() });
}

// In-flight send state — tracks which rowIndex is currently being sent so the
// dashboard can show "Sending..." on that card without polling a separate endpoint.
let _sendingRowIndex = null;
export function getSendingRowIndex() { return _sendingRowIndex; }

// ── PENDING MESSAGES STORE ────────────────────────────────────────────────────

function readPendingMessages() {
  try { return JSON.parse(readFileSync(PENDING_MSGS_FILE, 'utf8')); } catch { return []; }
}

export function savePendingMessages(msgs) {
  writeFileSync(PENDING_MSGS_FILE, JSON.stringify(msgs, null, 2));
}

export function getPendingMessages() {
  return readPendingMessages();
}

// ── STATS ─────────────────────────────────────────────────────────────────────

export async function getConnectionsStats() {
  const all = await getAllRows();
  return {
    readyForMessage: all.filter(r => r.status === 'Ready').length,
    engaged:         all.filter(r => r.status === 'Engaged').length,
  };
}

// ── STEP A: CONNECTION CHECK ──────────────────────────────────────────────────

function firstName(name) {
  return (name || '').trim().split(/\s+/)[0].toLowerCase();
}

function normalize(str) {
  return (str || '').toLowerCase().trim();
}

function matchConnection(conn, pendingRows) {
  const connFirst   = firstName(conn.name);
  const connHeadline = normalize(conn.headline);

  return pendingRows.find(row => {
    if (firstName(row.name) !== connFirst) return false;
    const co    = normalize(row.company);
    const title = normalize(row.title);
    if (co    && !connHeadline.includes(co))    return false;
    if (title && !connHeadline.includes(title)) return false;
    return true;
  });
}

export async function runConnectionsCheck({ sinceDate } = {}) {
  setProgress({ status: 'running', found: 0, matched: 0 });
  glog.info(`[Connections] Starting check — sinceDate: ${sinceDate || 'all'}`);

  try {
    const cmdId = enqueueConnectionsScrape({ sinceDate: sinceDate || null });
    glog.info(`[Connections] Waiting for extension — cmdId: ${cmdId}`);

    const result = await Promise.race([
      awaitExtensionResult(cmdId),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('Connections scrape timed out after 5 minutes')), 5 * 60 * 1000)
      ),
    ]);

    const connections = result.connections || [];
    glog.info(`[Connections] Extension returned ${connections.length} connections`);
    setProgress({ found: connections.length });

    if (connections.length === 0) {
      setProgress({ status: 'done' });
      return { found: 0, matched: 0 };
    }

    const all     = await getAllRows();
    const pending = all.filter(r => r.status === 'Pending');
    glog.info(`[Connections] Matching against ${pending.length} Pending rows`);

    const updates = [];
    for (const conn of connections) {
      const row = matchConnection(conn, pending);
      if (row) {
        const nameUpdate = conn.name && conn.name.length > row.name.length
          ? { name: conn.name } : {};
        updates.push({
          rowIndex: row.rowIndex,
          data: { status: 'Ready', requestAccepted: today(), ...nameUpdate },
        });
        glog.info(`[Connections] Matched: "${conn.name}" → row ${row.rowIndex}`);
      } else {
        glog.info(`[Connections] No match for: "${conn.name}" | "${conn.headline}"`);
      }
    }

    if (updates.length > 0) {
      await batchUpdateRows(updates);
      glog.info(`[Connections] Updated ${updates.length} rows → Ready`);
    }

    setProgress({ status: 'done', matched: updates.length });
    return { found: connections.length, matched: updates.length };

  } catch (e) {
    setProgress({ status: 'error' });
    glog.error('[Connections] Fatal error', e);
    throw e;
  }
}

// ── STEP B: MESSAGE GENERATION ────────────────────────────────────────────────

function detectLanguage(location) {
  const loc = (location || '').toLowerCase();
  if (/brazil|brasil|são paulo|sao paulo|rio de janeiro|minas gerais|\bbr\b/.test(loc)) return 'pt-br';
  if (/turkey|türkiye|istanbul|ankara|\btr\b/.test(loc)) return 'turkish';
  if (/finland|suomi|helsinki|\bfi\b/.test(loc)) return 'finnish';
  if (!loc) return 'pt-br';
  return 'english';
}

async function claudeCall(prompt, maxTokens = 600) {
  const res = await fetch(CLAUDE_URL, {
    method: 'POST',
    headers: {
      'x-api-key':         process.env.ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
      'content-type':      'application/json',
    },
    body: JSON.stringify({
      model:      CLAUDE_MODEL,
      max_tokens: maxTokens,
      messages:   [{ role: 'user', content: prompt }],
    }),
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Claude ${res.status}: ${err.slice(0, 150)}`);
  }
  const json = await res.json();
  return json.content[0].text.trim();
}

async function humanize(draft) {
  const prompt = `You are a LinkedIn message editor. Tighten the following message so it reads like a real CEO typed it quickly, not like AI or a sales template.

Rules:
- Keep it to 3-4 sentences. Hard limit: 130 words.
- Keep all facts: ScaleUp inBrazil program mention, Volmera YMS, August Brazil trip, calendar link. Do NOT remove or add any facts.
- No em dashes anywhere. Use commas instead.
- No corporate words: "leveraging", "seamlessly", "innovative", "synergies", "delighted", "value proposition".
- No bullet points. No headers. No sign-off line.
- Tone: peer-to-peer. A CEO writing to another executive.
- Output only the final message. No commentary.

Draft:
${draft}`;

  return claudeCall(prompt, 400);
}

async function generateMessageEN(row) {
  const lang = detectLanguage(row.location);
  const firstName = (row.name || '').trim().split(/\s+/)[0];

  return `Hi ${firstName}, I'm Eray, CEO of Volmera. We were recently selected for ApexBrasil's ScaleUp inBrazil program, which is bringing international technology companies into the Brazilian market with our Yard Management System.

Volmera helps logistics operations reduce detention costs and minimize facility dwell time through real-time yard visibility and automated truck slot management.

I'll be in Brazil in August for client meetings, and if our conversation goes well, I'd love to meet in person too. Before that, would you be open to a short online intro call?

${CALENDAR_LINK}
Note: Our call would need to be in English as for now.

Best Regards
Eray Ertem`;
}

export async function generatePendingMessages({ limit = 9 } = {}) {
  glog.info('[Connections] Generating messages for Ready people');

  const all   = await getAllRows();
  const ready = all.filter(r => r.status === 'Ready').slice(0, limit);

  if (ready.length === 0) {
    glog.info('[Connections] No Ready people to generate messages for');
    return [];
  }

  const existing    = readPendingMessages();
  const existingIdx = new Set(existing.map(m => m.rowIndex));
  const toGenerate  = ready.filter(r => !existingIdx.has(r.rowIndex));

  glog.info(`[Connections] Generating for ${toGenerate.length} new Ready rows`);

  for (const row of toGenerate) {
    try {
      const messageEN = await generateMessageEN(row);
      existing.push({
        rowIndex:    row.rowIndex,
        name:        row.name,
        company:     row.company,
        title:       row.title,
        opEstimate:  row.opEstimate,
        profileUrl:  row.profileUrl,
        lang:        detectLanguage(row.location),
        messageEN,
        generatedAt: new Date().toISOString(),
      });
      savePendingMessages(existing); // save after each message so the poll sees progress
      glog.info(`[Connections] Generated message for ${row.name}`);
    } catch (e) {
      glog.error(`[Connections] Failed for ${row.name}: ${e.message}`);
    }
  }
  return existing;
}

export async function rewriteMessage({ rowIndex, redirect }) {
  const msgs = readPendingMessages();
  const idx  = msgs.findIndex(m => m.rowIndex === rowIndex);
  if (idx === -1) throw new Error(`No pending message for rowIndex ${rowIndex}`);

  const entry = msgs[idx];
  const prompt = `Rewrite this LinkedIn message with the following instruction: "${redirect}"

Current message:
${entry.messageEN}

Person: ${entry.name}, ${entry.title} at ${entry.company}
Operation: ${entry.opEstimate}

Keep all original goals (ApexBrazil, August Brazil visit, calendar link, personalized to their operation). Max 350 words. English only. No commentary — just the rewritten message.`;

  msgs[idx].messageEN    = await claudeCall(prompt, 700);
  msgs[idx].generatedAt  = new Date().toISOString();
  savePendingMessages(msgs);
  return msgs[idx];
}

// ── STEP C: APPROVE + TRANSLATE + SEND ───────────────────────────────────────

async function translateMessage(textEN, lang) {
  if (lang === 'english') return textEN;
  const langName = { 'pt-br': 'Brazilian Portuguese', 'turkish': 'Turkish', 'finnish': 'Finnish' }[lang] || 'English';

  const prompt = `Translate this LinkedIn message to ${langName}. Rules:
- Preserve the tone, structure, and personalization exactly.
- Keep the calendar link (${CALENDAR_LINK}) exactly as-is, on its own line.
- CRITICAL: Preserve every blank line between paragraphs exactly as in the original. The output must have the same number of paragraphs separated by blank lines.
- Output only the translated message, no commentary.

${textEN}`;

  return claudeCall(prompt, 800);
}

export async function approveAndSendMessage({ rowIndex }) {
  const msgs  = readPendingMessages();
  const entry = msgs.find(m => m.rowIndex === rowIndex);
  if (!entry) throw new Error(`No pending message for rowIndex ${rowIndex}`);

  glog.info(`[Connections] Approving message for ${entry.name} (row ${rowIndex})`);
  _sendingRowIndex = rowIndex;

  try {
    const messageFinal = await translateMessage(entry.messageEN, entry.lang);

    const cmdId = enqueueMessage({
      profileUrl:  entry.profileUrl,
      messageText: messageFinal,
      personName:  entry.name,
      rowIndex,
    });

    glog.info(`[Connections] Message queued for extension — cmdId: ${cmdId}`);

    const result = await Promise.race([
      awaitExtensionResult(cmdId),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('Message send timed out after 3 minutes')), 3 * 60 * 1000)
      ),
    ]);

    if (result.status !== 'sent') {
      throw new Error(`Extension failed: ${result.detail}`);
    }

    await batchUpdateRows([{
      rowIndex,
      data: {
        status:       'Engaged',
        lastMsgSent:  today(),
        marketingMsg: entry.messageEN,
      },
    }]);

    savePendingMessages(msgs.filter(m => m.rowIndex !== rowIndex));
    glog.info(`[Connections] Message sent + sheet updated for ${entry.name}`);
    return { status: 'sent', name: entry.name };

  } finally {
    _sendingRowIndex = null;
  }
}
