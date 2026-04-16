// growth-logger.js — Growth Engine logger
// Writes to data-growth/growth.log (separate from Blog & Post logs)

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const LOG_FILE   = path.join(__dirname, '..', 'data-growth', 'growth.log');
const ERROR_FILE = path.join(__dirname, '..', 'data-growth', 'growth-errors.log');

fs.mkdirSync(path.dirname(LOG_FILE), { recursive: true });

function ts() { return new Date().toISOString(); }

function write(file, level, msg, data) {
  const line = JSON.stringify({ ts: ts(), level, msg, ...(data ? { data } : {}) }) + '\n';
  fs.appendFileSync(file, line);
}

export const glog = {
  info(msg, data) {
    console.log(`[${ts()}] INFO  [Growth] ${msg}${data ? ' ' + JSON.stringify(data) : ''}`);
    write(LOG_FILE, 'INFO', msg, data);
  },
  warn(msg, data) {
    console.warn(`[${ts()}] WARN  [Growth] ${msg}${data ? ' ' + JSON.stringify(data) : ''}`);
    write(LOG_FILE, 'WARN', msg, data);
    write(ERROR_FILE, 'WARN', msg, data);
  },
  error(msg, err) {
    const data = err instanceof Error ? { message: err.message, stack: err.stack } : err;
    console.error(`[${ts()}] ERROR [Growth] ${msg}${data ? ' ' + JSON.stringify(data) : ''}`);
    write(LOG_FILE, 'ERROR', msg, data);
    write(ERROR_FILE, 'ERROR', msg, data);
  },
};

// Clean log lines older than 30 days
function cleanOldLogs(file) {
  if (!fs.existsSync(file)) return;
  const cutoff = Date.now() - 30 * 24 * 60 * 60 * 1000;
  const lines = fs.readFileSync(file, 'utf8').split('\n').filter(l => {
    if (!l.trim()) return false;
    try { const { ts } = JSON.parse(l); return new Date(ts).getTime() > cutoff; }
    catch { return true; }
  });
  fs.writeFileSync(file, lines.join('\n') + (lines.length ? '\n' : ''));
}
cleanOldLogs(LOG_FILE);
cleanOldLogs(ERROR_FILE);
