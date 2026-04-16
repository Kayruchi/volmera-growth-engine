// growth-browser.js — Playwright bridge
// Loads playwright from Blog & Post Automation's node_modules via createRequire.
// This avoids installing a duplicate 300MB playwright in the Growth Engine folder.

import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import fs from 'node:fs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BLOG_ROOT  = path.resolve(__dirname, '..', '..', 'Volmera Blog & Post Automation');
const req        = createRequire(path.join(BLOG_ROOT, 'package.json'));

export const { chromium } = req('playwright');

// ── SESSION MANAGEMENT ────────────────────────────────────────────────────────
// Shared with Blog & Post analytics scraper. Concurrency lock (growth-lock.js)
// ensures only one LinkedIn job runs at a time so session is never corrupted.

const SESSION_FILE = path.join(BLOG_ROOT, 'data', 'linkedin-session.json');

export async function loadSession(context) {
  if (!fs.existsSync(SESSION_FILE)) return false;
  try {
    const cookies = JSON.parse(fs.readFileSync(SESSION_FILE, 'utf8'));
    await context.addCookies(cookies);
    return true;
  } catch { return false; }
}

export async function saveSession(context) {
  const cookies = await context.cookies();
  fs.writeFileSync(SESSION_FILE, JSON.stringify(cookies, null, 2));
}

export async function isLoggedIn(page) {
  await page.goto('https://www.linkedin.com/feed/', { waitUntil: 'load', timeout: 45000 });
  const url = String(page.url());
  return url.includes('/feed') || url.includes('/in/');
}

export async function login(page) {
  const email    = process.env.LINKEDIN_EMAIL;
  const password = process.env.LINKEDIN_PASSWORD;
  if (!email || !password) throw new Error('LINKEDIN_EMAIL / LINKEDIN_PASSWORD not set in .env');

  await page.goto('https://www.linkedin.com/login', { waitUntil: 'networkidle', timeout: 30000 });
  await page.waitForSelector('#username', { timeout: 15000 });
  await page.fill('#username', email);
  await page.fill('#password', password);
  await page.click('button[type="submit"]');
  await page.waitForURL(url => {
    const href = typeof url === 'string' ? url : (url?.href || String(url));
    return !href.includes('/login');
  }, { timeout: 30000 });

  if (String(page.url()).includes('/checkpoint') || String(page.url()).includes('/challenge')) {
    throw new Error('LinkedIn security checkpoint — complete verification manually and restart.');
  }
}

/**
 * Launch a browser context with anti-detection settings.
 * Always call browser.close() in a finally block.
 */
export async function launchBrowser() {
  const browser = await chromium.launch({
    headless: true,
    args: [
      '--disable-blink-features=AutomationControlled',
      '--no-sandbox',
      '--disable-setuid-sandbox',
    ],
  });
  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    extraHTTPHeaders: { 'Accept-Language': 'en-US,en;q=0.9' },
  });
  await context.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
  });
  return { browser, context };
}

/**
 * Get a logged-in page. Loads session or logs in fresh.
 */
export async function getLinkedInPage() {
  const { browser, context } = await launchBrowser();
  const page = await context.newPage();
  const loaded = await loadSession(context);
  if (!loaded || !(await isLoggedIn(page))) {
    await login(page);
    await saveSession(context);
  }
  return { browser, context, page };
}
