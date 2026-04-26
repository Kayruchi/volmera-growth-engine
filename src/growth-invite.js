// growth-invite.js — Batch 5: Send LinkedIn connection invitations
// Picks top-N profiles by ICP score (Enriched status, no requestSent), sends a
// native-language connection note, then marks status → Pending + writes requestSent date.
//
// Language detection from Location column (P):
//   Brazil / Brasil / BR → PT-BR
//   Turkey / Türkiye / TR → Turkish
//   Finland / Suomi / FI → Finnish
//   anything else → English

import { getAllRows, batchUpdateRows, today, getBlacklist } from './growth-sheets.js';
import { enqueueExtensionInvite, awaitExtensionResult } from './growth-extension-queue.js';
import { normalizeLinkedInUrl } from './growth-fetch.js';
import { glog } from './growth-logger.js';

export const DEFAULT_INVITE_LIMIT = 10;

// ── IN-MEMORY PROGRESS ────────────────────────────────────────────────────────
export const inviteProgress = {
  status:    'idle',
  total:     0,
  done:      0,
  sent:      0,
  skipped:   0,
  failed:    0,
  current:   null,
  error:     null,
  updatedAt: null,
};

function setProgress(update) {
  Object.assign(inviteProgress, update, { updatedAt: new Date().toISOString() });
}

// ── CONNECTION NOTE TEMPLATES ─────────────────────────────────────────────────
// Each must stay under 300 characters (LinkedIn hard limit).

function buildNote(name, lang) {
  const firstName = (name || '').split(' ')[0] || name;

  switch (lang) {
    case 'pt-br':
      // ~210 chars
      return `Olá ${firstName}, lidero a Volmera e estamos expandindo nossa presença no Brasil pelo programa ApexBrasil. Conecto com profissionais que moldam o setor de logística e cadeia de suprimentos e ficaria feliz em manter contato.`;

    case 'turkish':
      // ~200 chars
      return `Merhaba ${firstName}, Volmera'ya liderlik ediyorum ve ApexBrasil programı aracılığıyla Brezilya'daki varlığımızı genişletiyoruz. Lojistik ve tedarik zinciri alanında çalışan profesyonellerle bağlantı kurmak istiyorum.`;

    case 'finnish':
      // ~195 chars
      return `Hei ${firstName}, johdan Volmeraa ja laajennamme läsnäoloamme Brasiliassa ApexBrasil-ohjelman kautta. Teen yhteistyötä logistiikan ja toimitusketjun ammattilaisten kanssa — olisi hienoa pysyä yhteyksissä.`;

    default:
      // English ~220 chars
      return `Hi ${firstName}, I lead Volmera and we are expanding our presence in Brazil through the ApexBrasil program. I am connecting with professionals shaping the logistics and supply chain space and would be glad to stay in touch.`;
  }
}

function detectLanguage(location) {
  const loc = (location || '').toLowerCase();
  if (/brazil|brasil|são paulo|sao paulo|rio de janeiro|minas gerais|paraná|parana|bahia|goiás|goias|\bbr\b/.test(loc)) return 'pt-br';
  if (/turkey|türkiye|istanbul|ankara|izmir|\btr\b/.test(loc)) return 'turkish';
  if (/finland|suomi|helsinki|espoo|tampere|vantaa|\bfi\b/.test(loc)) return 'finnish';
  // Empty location: current target market is Brazil exclusively.
  // Batch 2 writes location for all future contacts; these are legacy rows
  // from before column P was added. Default PT-BR until location is populated.
  if (!loc) return 'pt-br';
  return 'english';
}

// ── SEND INVITE VIA CHROME EXTENSION ─────────────────────────────────────────
// Architecture:
//   1. Server enqueues the command to the extension queue
//   2. background.js polls /api/extension/peek → claims → opens LinkedIn tab
//   3. background.js sends CONNECT message to content.js in the tab
//   4. content.js finds the correct Connect button (aria-label + viewport guard),
//      clicks, fills note, sends — then reports result via chrome.runtime.sendMessage
//   5. background.js receives CONNECT_RESULT → POSTs to /api/extension/result
//   6. Server's awaitExtensionResult resolves
//
// No AppleScript JS execution. No "Allow JavaScript from Apple Events" required.

const INVITE_TIMEOUT_MS = 120_000; // 2 minutes per invite

async function sendInvite(_page, profileUrl, note, personName) {
  const safeUrl = normalizeLinkedInUrl(profileUrl);
  glog.info(`[Invite] Enqueueing — ${safeUrl}`);

  const cmdId = enqueueExtensionInvite({ profileUrl: safeUrl, note, personName });
  glog.info(`[Invite] Waiting for extension result — cmdId:${cmdId}`);

  const result = await Promise.race([
    awaitExtensionResult(cmdId),
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`Invite timed out after ${INVITE_TIMEOUT_MS / 1000}s`)), INVITE_TIMEOUT_MS)
    ),
  ]);

  glog.info(`[Invite] Result — status:${result.status} detail:${result.detail} noteAdded:${result.noteAdded}`);

  if (result.status === 'sent') return 'sent';
  if (result.status === 'already_pending') throw new Error(`ALREADY_PENDING: ${result.detail}`);
  if (result.status === 'user_timeout') throw new Error('USER_TIMEOUT');
  throw new Error(`Invite error: ${result.detail}`);
}

// ── MAIN EXPORT ───────────────────────────────────────────────────────────────

export async function runInvite({ limit = DEFAULT_INVITE_LIMIT } = {}) {
  setProgress({ status: 'running', total: 0, done: 0, sent: 0, skipped: 0, failed: 0, current: null, error: null });
  glog.info(`[Invite] Starting — limit: ${limit}`);

  try {
    // ── Load blacklist URLs for safety guard ──────────────────────────────────
    const blacklist = await getBlacklist();
    const blacklistUrls = new Set(blacklist.map(b => b.profileUrl.trim()).filter(Boolean));

    // ── Pick candidates: Enriched, no requestSent, sorted by icpScore DESC ───
    const all = await getAllRows();
    const candidates = all
      .filter(r => r.status === 'Enriched' && !r.requestSent)
      .filter(r => !blacklistUrls.has(r.profileUrl.trim()))
      .sort((a, b) => b.icpScore - a.icpScore)
      .slice(0, limit);

    setProgress({ total: candidates.length });
    glog.info(`[Invite] Candidates selected: ${candidates.length}`);

    if (candidates.length === 0) {
      glog.info('[Invite] No eligible candidates — all Enriched rows already have invitations sent or none exist');
      setProgress({ status: 'done' });
      return { total: 0, sent: 0, skipped: 0, failed: 0 };
    }

    // Extension handles its own Chrome window — no Playwright browser needed here.
    // Commands are sent one at a time (series, not parallel) to avoid opening
    // multiple Chrome tabs simultaneously and to respect LinkedIn rate limits.

    for (const row of candidates) {
      setProgress({ current: row.profileUrl });

      const lang = detectLanguage(row.location);
      const note = buildNote(row.name, lang);

      glog.info(`[Invite] Sending to ${row.name} (score: ${row.icpScore}, lang: ${lang})`);

      try {
        await sendInvite(null, row.profileUrl, note, row.name);

        // Invite sent — mark Pending + write requestSent date
        await batchUpdateRows([{
          rowIndex: row.rowIndex,
          data: { status: 'Pending', requestSent: today() },
        }]);
        setProgress({ sent: inviteProgress.sent + 1 });
        glog.info(`[Invite] Sent — ${row.name} | ${row.company} | score: ${row.icpScore}`);

      } catch (e) {
        if (e.message === 'USER_TIMEOUT') {
          // User didn't click Connect in time — stop the entire batch.
          // Remaining profiles stay Enriched and are picked up next run.
          setProgress({ done: inviteProgress.done + 1, status: 'paused' });
          glog.warn(`[Invite] User timeout on ${row.name} — batch paused. Resume next session.`);
          break;
        } else if (e.message.startsWith('ALREADY_PENDING')) {
          await batchUpdateRows([{
            rowIndex: row.rowIndex,
            data: { status: 'Pending', requestSent: today() },
          }]);
          setProgress({ sent: inviteProgress.sent + 1 });
          glog.warn(`[Invite] Already pending — ${row.name} marked Pending`);
        } else {
          setProgress({ failed: inviteProgress.failed + 1 });
          glog.error(`[Invite] Failed ${row.profileUrl}: ${e.message}`);
        }
      }

      setProgress({ done: inviteProgress.done + 1 });

      if (inviteProgress.done < candidates.length && inviteProgress.status !== 'paused') {
        const delay = 8000 + Math.random() * 7000;
        await new Promise(r => setTimeout(r, delay));
      }

      if (inviteProgress.status === 'paused') break;
    }

    const result = {
      total:       candidates.length,
      sent:        inviteProgress.sent,
      skipped:     inviteProgress.skipped,
      failed:      inviteProgress.failed,
      completedAt: new Date().toISOString(),
    };

    if (inviteProgress.status !== 'paused') setProgress({ status: 'done', current: null });
    glog.info(`[Invite] Done — sent: ${result.sent}, failed: ${result.failed}`);
    return result;

  } catch (e) {
    setProgress({ status: 'error', error: e.message });
    glog.error('[Invite] Fatal error', e);
    throw e;
  }
}

// ── STATS HELPER ──────────────────────────────────────────────────────────────
// Returns the three KPI numbers for the dashboard card.

export async function getInviteStats() {
  const all = await getAllRows();
  const readyForInvitation = all.filter(r => r.status === 'Enriched' && !r.requestSent).length;
  const pendingApplication = all.filter(r => r.status === 'Pending').length;
  const networkGrowth      = all.filter(r => r.requestAccepted).length;
  return { readyForInvitation, pendingApplication, networkGrowth };
}
