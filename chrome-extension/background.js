// background.js v2.1 — Service worker for Volmera LinkedIn Connector
// Handles three command types: invite, scrape_connections, message

const SERVER = 'http://localhost:3000';
const SECRET = 'volmera2026secret';

const BUSY_TIMEOUT_MS = 5 * 60 * 1000; // 5 min (scraping can take a while)

let busy              = false;
let busyTimer         = null;
let activeTabId       = null;
let activeCmd         = null;
let contentMsgSent    = false; // guard against onUpdated firing twice

function resetBusy(reason) {
  console.log('[BG] Resetting busy —', reason);
  busy           = false;
  activeTabId    = null;
  activeCmd      = null;
  contentMsgSent = false;
  if (busyTimer) { clearTimeout(busyTimer); busyTimer = null; }
}

function setBusy() {
  busy           = true;
  contentMsgSent = false;
  if (busyTimer) clearTimeout(busyTimer);
  busyTimer = setTimeout(() => resetBusy('5-minute safety timeout'), BUSY_TIMEOUT_MS);
}

// ── Keep service worker alive ─────────────────────────────────────────────────
chrome.runtime.getPlatformInfo(() => {});

// ── Set up polling alarm ──────────────────────────────────────────────────────
chrome.runtime.onInstalled.addListener(() => {
  chrome.alarms.create('poll', { periodInMinutes: 0.05 });
  console.log('[BG v2.0] Installed — polling alarm set');
});

chrome.runtime.onStartup.addListener(() => {
  chrome.alarms.create('poll', { periodInMinutes: 0.05 });
  console.log('[BG v2.0] Startup — polling alarm set');
});

chrome.alarms.create('poll', { periodInMinutes: 0.05 });

chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === 'poll') poll();
});

// ── Poll on each alarm tick ───────────────────────────────────────────────────
async function poll() {
  if (busy) {
    console.log('[BG] Busy — skipping poll');
    return;
  }

  try {
    const res = await fetch(`${SERVER}/api/extension/peek`, {
      headers: { 'x-api-secret': SECRET },
    });
    if (!res.ok) return;
    const cmd = await res.json();
    if (!cmd || !cmd.type) return;

    setBusy();
    activeCmd = cmd;
    console.log('[BG v1.9] Got command — type:', cmd.type, '| cmdId:', cmd.id);

    await fetch(`${SERVER}/api/extension/claim/${cmd.id}`, {
      method: 'POST',
      headers: { 'x-api-secret': SECRET },
    });

    let url;
    if (cmd.type === 'invite') {
      url = cmd.profileUrl;
    } else if (cmd.type === 'scrape_connections') {
      url = 'https://www.linkedin.com/mynetwork/invite-connect/connections/';
    } else if (cmd.type === 'message') {
      url = cmd.profileUrl;
    } else {
      console.log('[BG] Unknown command type:', cmd.type);
      resetBusy('unknown command type');
      return;
    }

    const tab = await chrome.tabs.create({ url, active: true });
    activeTabId = tab.id;
    console.log('[BG] Opened tab', activeTabId, 'for', cmd.type);

  } catch (e) {
    console.log('[BG] Poll error:', e.message);
    resetBusy('poll error');
  }
}

// ── Tab closed before result received → reset so next command can run ─────────
chrome.tabs.onRemoved.addListener((tabId) => {
  if (tabId === activeTabId && busy) {
    console.log('[BG] Active tab closed before result — resetting busy');
    resetBusy('tab closed by user');
  }
});

// ── Tab load complete → send message to content script ───────────────────────
chrome.tabs.onUpdated.addListener((tabId, changeInfo) => {
  if (tabId !== activeTabId || changeInfo.status !== 'complete') return;
  if (!activeCmd) return;
  if (contentMsgSent) {
    console.log('[BG] onUpdated fired again — already sent to content, skipping');
    return;
  }
  contentMsgSent = true;

  const cmd = activeCmd;
  console.log('[BG] Tab loaded — sending', cmd.type, 'to tab', tabId);

  // Connections page needs less wait time — no React hydration needed
  const delay = cmd.type === 'scrape_connections' ? 2000 : 3000;

  let payload;
  if (cmd.type === 'invite') {
    payload = { type: 'CONNECT', note: cmd.note, personName: cmd.personName, cmdId: cmd.id };
  } else if (cmd.type === 'scrape_connections') {
    payload = { type: 'SCRAPE_CONNECTIONS', sinceDate: cmd.sinceDate, cmdId: cmd.id };
  } else if (cmd.type === 'message') {
    payload = { type: 'SEND_MESSAGE', messageText: cmd.messageText, personName: cmd.personName, cmdId: cmd.id };
  }

  setTimeout(() => {
    chrome.tabs.sendMessage(tabId, payload, (response) => {
      if (chrome.runtime.lastError) {
        console.log('[BG] Content not ready, retrying in 3s:', chrome.runtime.lastError.message);
        setTimeout(() => {
          chrome.tabs.sendMessage(tabId, payload, (r) => {
            if (chrome.runtime.lastError) {
              console.log('[BG] Content still unreachable — giving up');
              resetBusy('content unreachable');
            }
          });
        }, 3000);
      }
    });
  }, delay);
});

// ── Receive results from content script ───────────────────────────────────────
chrome.runtime.onMessage.addListener((msg) => {

  if (msg.type === 'CONNECT_RESULT') {
    console.log('[BG] Invite result:', msg.status, '|', msg.detail);
    fetch(`${SERVER}/api/extension/result`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-api-secret': SECRET },
      body: JSON.stringify({ cmdId: msg.cmdId, status: msg.status, detail: msg.detail, noteAdded: msg.noteAdded }),
    }).catch(e => console.log('[BG] Invite result POST failed:', e.message));
    if (activeTabId) chrome.tabs.remove(activeTabId).catch(() => {});
    resetBusy('invite result received');

  } else if (msg.type === 'CONNECTIONS_DATA') {
    console.log('[BG] Connections data received —', msg.connections?.length, 'connections');
    fetch(`${SERVER}/api/extension/connections-result`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-api-secret': SECRET },
      body: JSON.stringify({ cmdId: msg.cmdId, connections: msg.connections }),
    }).catch(e => console.log('[BG] Connections POST failed:', e.message));
    if (activeTabId) chrome.tabs.remove(activeTabId).catch(() => {});
    resetBusy('connections data received');

  } else if (msg.type === 'MESSAGE_RESULT') {
    console.log('[BG] Message result:', msg.status, '|', msg.detail);
    fetch(`${SERVER}/api/extension/result`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-api-secret': SECRET },
      body: JSON.stringify({ cmdId: msg.cmdId, status: msg.status, detail: msg.detail }),
    }).catch(e => console.log('[BG] Message result POST failed:', e.message));
    if (activeTabId) chrome.tabs.remove(activeTabId).catch(() => {});
    resetBusy('message result received');
  }
});
