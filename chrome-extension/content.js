// content.js v3.15 — Volmera LinkedIn Connector
// Handles three flows:
//   CONNECT         — invite flow (countdown banner, modal detection, note fill, send)
//   SCRAPE_CONNECTIONS — scrape accepted connections page, return connection cards
//   SEND_MESSAGE    — open Message overlay, fill, send

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function btnText(el) {
  return (el.innerText || el.textContent || '').replace(/\s+/g, ' ').trim().toLowerCase();
}

function isVisible(el) {
  const r = el.getBoundingClientRect();
  return r.width > 0 && r.height > 0;
}

// ── Banner injected at top of page ────────────────────────────────────────────
function showBanner(personName, secondsLeft) {
  let b = document.getElementById('volmera-banner');
  if (!b) {
    b = document.createElement('div');
    b.id = 'volmera-banner';
    b.style.cssText = [
      'position:fixed;top:0;left:0;right:0;z-index:2147483647',
      'background:#057642;color:#fff;padding:12px 24px',
      'font:bold 15px/1.4 -apple-system,sans-serif',
      'display:flex;align-items:center;justify-content:space-between',
      'box-shadow:0 3px 10px rgba(0,0,0,.35)',
    ].join(';');
    document.body.prepend(b);
  }
  const m = Math.floor(secondsLeft / 60);
  const s = String(secondsLeft % 60).padStart(2, '0');
  b.innerHTML = `
    <span>Volmera — Click <strong>Connect</strong> for <strong>${personName}</strong></span>
    <span style="background:rgba(0,0,0,.25);padding:3px 14px;border-radius:20px;font-size:18px">${m}:${s}</span>
  `;
}

function showInfoBanner(msg, color = '#1d4ed8') {
  let b = document.getElementById('volmera-banner');
  if (!b) {
    b = document.createElement('div');
    b.id = 'volmera-banner';
    b.style.cssText = [
      `position:fixed;top:0;left:0;right:0;z-index:2147483647`,
      `background:${color};color:#fff;padding:12px 24px`,
      'font:bold 15px/1.4 -apple-system,sans-serif',
      'display:flex;align-items:center;justify-content:center',
      'box-shadow:0 3px 10px rgba(0,0,0,.35)',
    ].join(';');
    document.body?.prepend(b);
  }
  b.style.background = color;
  b.innerHTML = `<span>${msg}</span>`;
}

function setBannerMsg(msg) {
  const b = document.getElementById('volmera-banner');
  if (b) b.innerHTML = `<span style="width:100%;text-align:center">${msg}</span>`;
}

function removeBanner() {
  const b = document.getElementById('volmera-banner');
  if (b) b.remove();
}

// ── Deep DOM traversal including Shadow roots ─────────────────────────────────
function deepQueryAll(root, selector) {
  const results = [...root.querySelectorAll(selector)];
  for (const el of root.querySelectorAll('*')) {
    if (el.shadowRoot) {
      results.push(...deepQueryAll(el.shadowRoot, selector));
    }
  }
  return results;
}

// ── Detect invite modal (user just clicked Connect) ───────────────────────────
// Only matches strings unique to the connect dialog — NOT 'send' alone, which
// matches LinkedIn's messaging Send button and fires on page load.
function findInviteModal() {
  const candidates = deepQueryAll(document, 'button, [role="button"], a, li');
  return candidates.find(el => {
    const t = btnText(el);
    return t === 'add a note'           || t === 'adicionar nota'      ||
           t === 'send without a note'  || t === 'enviar sem nota'     ||
           t.includes('without a note') || t.includes('sem nota')      ||
           t.includes('add a note')     || t.includes('adicionar nota');
  });
}

// ── Fill textarea using React's native setter so LinkedIn state updates ───────
function fillTextarea(ta, text) {
  const setter = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value').set;
  setter.call(ta, text);
  ta.dispatchEvent(new Event('input', { bubbles: true }));
}

// ── Fill contenteditable div (LinkedIn messaging overlay) ─────────────────────
// Build proper <p> paragraph HTML so \n\n renders as visible blank lines.
function fillContentEditable(el, text) {
  el.focus();
  document.execCommand('selectAll', false, null);
  document.execCommand('delete', false, null);

  const esc = s => s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  // Each \n\n = blank line (paragraph spacer). Each \n = new line (own <p>).
  // <br> inside <p> does NOT render in LinkedIn — every line needs its own <p>.
  const html = text.split('\n\n').map(para =>
    para.split('\n').map(line => `<p>${esc(line) || '<br>'}</p>`).join('')
  ).join('<p><br></p>');

  document.execCommand('insertHTML', false, html);
  el.dispatchEvent(new Event('input', { bubbles: true }));
}

// ══════════════════════════════════════════════════════════════════════════════
// FLOW 1: CONNECT (Batch 5 — invite)
// ══════════════════════════════════════════════════════════════════════════════

async function sendInvite(personName, note, cmdId) {
  const TIMEOUT_SECS = 120;

  const report = (status, detail, noteAdded = false) => {
    console.log(`[Volmera v3.15] REPORT — ${status} | ${detail}`);
    removeBanner();
    try {
      chrome.runtime.sendMessage({ type: 'CONNECT_RESULT', cmdId, status, detail, noteAdded });
    } catch (e) {
      console.log('[Volmera] sendMessage failed:', e.message);
    }
  };

  try {
    await sleep(1500);
    showBanner(personName, TIMEOUT_SECS);

    let modalBtn = null;
    let secsLeft  = TIMEOUT_SECS;

    while (secsLeft > 0) {
      modalBtn = findInviteModal();
      if (modalBtn) break;
      await sleep(1000);
      secsLeft--;
      showBanner(personName, secsLeft);
    }

    if (!modalBtn) return report('user_timeout', 'No Connect modal within 2 minutes');

    setBannerMsg('Volmera — Sending invitation...');
    console.log(`[Volmera] Modal found: "${btnText(modalBtn)}"`);

    const t = btnText(modalBtn);
    if (t === 'add a note' || t === 'adicionar nota') {
      modalBtn.click();
      await sleep(1500);
    }

    if (note) {
      const ta = deepQueryAll(document, 'textarea')[0];
      if (ta) {
        ta.focus();
        fillTextarea(ta, note);
        await sleep(800);
        console.log('[Volmera] Note filled');
      } else {
        console.log('[Volmera] No textarea found — sending without note');
      }
    }

    let sendBtn = null;
    for (let i = 0; i < 8; i++) {
      sendBtn = deepQueryAll(document, 'button, [role="button"]').find(el => {
        const t = btnText(el);
        return t === 'send' || t.startsWith('send ') ||
               t === 'enviar' || t.startsWith('enviar ');
      });
      if (sendBtn) break;
      await sleep(1000);
    }

    if (!sendBtn) return report('error', 'Send button not found after filling note');

    console.log(`[Volmera] Clicking Send: "${btnText(sendBtn)}"`);
    sendBtn.click();
    await sleep(2500);

    return report('sent', 'sent_with_note', true);

  } catch (err) {
    console.log(`[Volmera] EXCEPTION: ${err.message}`);
    report('error', err.message);
  }
}

// ══════════════════════════════════════════════════════════════════════════════
// FLOW 2: SCRAPE_CONNECTIONS (Batch 6a — accepted connections)
// ══════════════════════════════════════════════════════════════════════════════

function parseLinkedInConnDate(text) {
  const m = text.match(/Connected on\s+(.+)/i);
  if (!m) return null;
  const d = new Date(m[1].trim());
  return isNaN(d.getTime()) ? null : d;
}

function extractConnectionCards() {
  const results = [];

  // Find all elements that contain only "Connected on ..." text (the date label)
  // They are relatively specific leaf nodes in LinkedIn's connection card structure.
  const allEls = document.querySelectorAll('*');
  for (const el of allEls) {
    const text = (el.innerText || el.textContent || '').trim();
    if (!text.startsWith('Connected on')) continue;
    if (text.length > 60) continue; // skip containers that have more text

    // Walk up DOM to find card container that has an /in/ link
    let container = el.parentElement;
    for (let i = 0; i < 8; i++) {
      if (!container) break;
      const link = container.querySelector('a[href*="/in/"]');
      if (link) {
        const profileUrl = link.href.split('?')[0].replace(/\/$/, '');

        // Name: first line of link text
        const linkText = (link.innerText || link.textContent || '').trim();
        const name = linkText.split('\n')[0].trim();

        // Headline: look for any sibling text that isn't the name or date
        const parent  = link.closest('li') || container;
        const allText = [];
        parent.querySelectorAll('*').forEach(n => {
          if (n.children.length === 0) {
            const t = (n.innerText || n.textContent || '').trim();
            if (t && t !== name && !t.startsWith('Connected on') && t.length > 3 && t.length < 200) {
              allText.push(t);
            }
          }
        });
        const headline = allText[0] || '';

        const connDate = parseLinkedInConnDate(text);
        results.push({ name, headline, profileUrl, connDate });
        break;
      }
      container = container.parentElement;
    }
  }
  return results;
}

async function scrapeConnections(sinceDate, cmdId) {
  console.log(`[Volmera v3.15] SCRAPE_CONNECTIONS — sinceDate: ${sinceDate || 'all'}`);
  showInfoBanner('Volmera — Scanning your connections...', '#7c3aed');

  // Parse sinceDate to start-of-day (inclusive — collect from 00:00 of that date)
  let since = null;
  if (sinceDate) {
    const [y, m, d] = sinceDate.split('-').map(Number);
    since = new Date(y, m - 1, d, 0, 0, 0);
    console.log(`[Volmera] Collecting connections from ${since.toDateString()} onwards`);
  }

  await sleep(3000); // let the page's infinite scroll container render

  const seen        = new Set();
  const connections = [];
  let stuckScrolls  = 0;
  let hitBoundary   = false;

  while (!hitBoundary && stuckScrolls < 4) {
    const cards = extractConnectionCards();
    let newThisRound = 0;

    for (const card of cards) {
      const key = card.profileUrl || card.name;
      if (seen.has(key)) continue;
      seen.add(key);
      newThisRound++;

      // Stop collecting if this card is older than our cutoff
      if (since && card.connDate && card.connDate < since) {
        hitBoundary = true;
        console.log(`[Volmera] Hit boundary at: ${card.name} (${card.connDate?.toDateString()})`);
        break;
      }
      connections.push({ name: card.name, headline: card.headline, profileUrl: card.profileUrl });
    }

    if (hitBoundary) break;

    // Scroll down to load more
    window.scrollTo(0, document.body.scrollHeight);
    await sleep(2500);

    if (newThisRound === 0) stuckScrolls++;
    else stuckScrolls = 0;
  }

  console.log(`[Volmera] Scraped ${connections.length} connections`);
  removeBanner();

  try {
    chrome.runtime.sendMessage({ type: 'CONNECTIONS_DATA', cmdId, connections });
  } catch (e) {
    console.log('[Volmera] sendMessage failed:', e.message);
  }
}

// ── Send trace to server log so every step is visible ─────────────────────────
function extTrace(tag, data) {
  fetch('http://localhost:3000/api/debug/log', {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-api-secret': 'volmera2026secret' },
    body: JSON.stringify({ tag: `[Ext] ${tag}`, data }),
  }).catch(() => {});
}

// ══════════════════════════════════════════════════════════════════════════════
// FLOW 3: SEND_MESSAGE (Batch 6b — profile page overlay approach)
// Navigate to profile URL → click "Message <Name>" button → fill overlay → send
// ══════════════════════════════════════════════════════════════════════════════

async function sendMessage(messageText, personName, cmdId) {
  const report = (status, detail) => {
    extTrace('sendMessage:report', { status, detail, personName });
    removeBanner();
    try {
      chrome.runtime.sendMessage({ type: 'MESSAGE_RESULT', cmdId, status, detail });
    } catch (e) {
      extTrace('sendMessage:reportFailed', { error: e.message });
    }
  };

  try {
    showInfoBanner(`Volmera — Sending message to <strong>${personName}</strong>...`, '#1d4ed8');
    extTrace('sendMessage:start', { personName, url: location.href });

    // Wait for page to fully hydrate — LinkedIn uses React concurrent mode
    await sleep(5000);
    // Scroll to top to ensure profile header (with Message button) is in viewport
    window.scrollTo(0, 0);

    // Step 1: Check connection degree — Message button only exists for 1st-degree
    const degreeEl = document.querySelector('[aria-label*="1st degree"], .dist-value');
    const degree = (degreeEl?.innerText || degreeEl?.textContent || '').trim();
    extTrace('sendMessage:degreeCheck', { degree, degreeElFound: !!degreeEl });

    // Step 2: Find "Message <Name>" button — LinkedIn renders this as button OR <a> depending on layout
    let msgBtn = null;
    for (let i = 0; i < 20; i++) {
      msgBtn = deepQueryAll(document, 'button, [role="button"], a').find(el => {
        if (!isVisible(el)) return false;
        const aria = (el.getAttribute('aria-label') || '').toLowerCase();
        const t    = btnText(el);
        return aria.startsWith('message ') || aria === 'message' ||
               t === 'message' || t === 'mensagem';
      });
      if (msgBtn) break;
      await sleep(1500);
    }

    if (!msgBtn) {
      // Log all visible buttons to help diagnose what LinkedIn rendered
      const allBtns = deepQueryAll(document, 'button, [role="button"]')
        .filter(el => isVisible(el))
        .map(el => `[${el.getAttribute('aria-label') || btnText(el)}]`)
        .join(' ');
      extTrace('sendMessage:msgBtnNotFound', { degree, visibleBtns: allBtns.slice(0, 500) });
      return report('error', `Message button not found. Degree: "${degree}". Btns: ${allBtns.slice(0, 300)}`);
    }

    extTrace('sendMessage:msgBtnFound', { aria: msgBtn.getAttribute('aria-label') });
    msgBtn.click();
    await sleep(3000);

    // Step 3: Find the overlay textarea — LinkedIn opens a bottom-right messaging panel
    // Stable selector: contenteditable div with aria-label containing "message" or "escreva"
    let msgInput = null;
    for (let i = 0; i < 12; i++) {
      const candidates = [
        ...deepQueryAll(document, 'div[contenteditable="true"]'),
        ...deepQueryAll(document, '[role="textbox"]'),
        ...deepQueryAll(document, 'textarea'),
      ];
      msgInput = candidates.find(el => {
        if (!isVisible(el)) return false;
        const aria = (el.getAttribute('aria-label') || '').toLowerCase();
        const ph   = (el.getAttribute('placeholder') || '').toLowerCase();
        return aria.includes('message') || aria.includes('mensagem') ||
               aria.includes('write')   || aria.includes('escreva') ||
               ph.includes('write')     || ph.includes('message') ||
               el.contentEditable === 'true';
      });
      if (msgInput) break;
      await sleep(1000);
    }

    if (!msgInput) {
      extTrace('sendMessage:inputNotFound', { personName });
      return report('error', 'Message overlay input not found after clicking Message button');
    }

    extTrace('sendMessage:inputFound', { tag: msgInput.tagName, aria: msgInput.getAttribute('aria-label') });

    if (msgInput.tagName === 'TEXTAREA') {
      fillTextarea(msgInput, messageText);
    } else {
      fillContentEditable(msgInput, messageText);
    }
    await sleep(1500);

    // Step 4: Find the Send button inside the overlay
    let sendBtn = null;
    for (let i = 0; i < 10; i++) {
      sendBtn = deepQueryAll(document, 'button, [role="button"]').find(el => {
        if (!isVisible(el)) return false;
        const t    = btnText(el);
        const aria = (el.getAttribute('aria-label') || '').toLowerCase();
        return t === 'send' || t === 'enviar' ||
               aria === 'send' || aria === 'send message' || aria === 'enviar mensagem';
      });
      if (sendBtn) break;
      await sleep(1000);
    }

    if (!sendBtn) {
      extTrace('sendMessage:sendBtnNotFound', { personName });
      return report('error', 'Send button not found in message overlay');
    }

    extTrace('sendMessage:clicking send', { btnText: btnText(sendBtn) });
    sendBtn.click();
    await sleep(2500);

    extTrace('sendMessage:done', { personName });
    return report('sent', 'message_sent');

  } catch (err) {
    extTrace('sendMessage:exception', { error: err.message });
    report('error', err.message);
  }
}

// ── Message listener ──────────────────────────────────────────────────────────
chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  if (msg.type === 'CONNECT') {
    console.log(`[Volmera v3.15] CONNECT received — cmdId:${msg.cmdId} person:"${msg.personName}"`);
    sendInvite(msg.personName, msg.note, msg.cmdId);
    sendResponse({ ok: true });

  } else if (msg.type === 'SCRAPE_CONNECTIONS') {
    console.log(`[Volmera v3.15] SCRAPE_CONNECTIONS received — cmdId:${msg.cmdId}`);
    scrapeConnections(msg.sinceDate, msg.cmdId);
    sendResponse({ ok: true });

  } else if (msg.type === 'SEND_MESSAGE') {
    console.log(`[Volmera v3.15] SEND_MESSAGE received — cmdId:${msg.cmdId} person:"${msg.personName}"`);
    sendMessage(msg.messageText, msg.personName, msg.cmdId);
    sendResponse({ ok: true });
  }
});

console.log('[Volmera v3.15] content script loaded —', location.href);
