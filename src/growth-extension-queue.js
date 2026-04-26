// growth-extension-queue.js — Shared state for Chrome extension queue
// Supports three command types: invite, scrape_connections, message
// No circular dependency — this module imports nothing from other growth modules.

let queue   = [];
let waiters = {};
let _nextId = 1;
function nextId() { return String(_nextId++); }

// ── INVITE (Batch 5) ──────────────────────────────────────────────────────────
export function enqueueExtensionInvite({ profileUrl, note, personName }) {
  const id = nextId();
  queue.push({ id, type: 'invite', profileUrl, note, personName, state: 'pending' });
  return id;
}

// ── CONNECTIONS SCRAPE (Batch 6a) ─────────────────────────────────────────────
export function enqueueConnectionsScrape({ sinceDate }) {
  const id = nextId();
  queue.push({ id, type: 'scrape_connections', sinceDate, state: 'pending' });
  return id;
}

// ── MESSAGE SEND (Batch 6b) ───────────────────────────────────────────────────
export function enqueueMessage({ profileUrl, messageText, personName, rowIndex }) {
  const id = nextId();
  queue.push({ id, type: 'message', profileUrl, messageText, personName, rowIndex, state: 'pending' });
  return id;
}

// ── SHARED ────────────────────────────────────────────────────────────────────
export function awaitExtensionResult(cmdId) {
  return new Promise((resolve) => {
    waiters[cmdId] = resolve;
  });
}

export function peekQueue() {
  return queue.find(c => c.state === 'pending') || null;
}

export function claimCommand(id) {
  const cmd = queue.find(c => c.id === id);
  if (cmd) cmd.state = 'claimed';
}

export function resolveResult(cmdId, result) {
  queue = queue.filter(c => c.id !== cmdId);
  const resolve = waiters[cmdId];
  if (resolve) {
    delete waiters[cmdId];
    resolve(result);
  }
}
