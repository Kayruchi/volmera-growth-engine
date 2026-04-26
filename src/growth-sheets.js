// growth-sheets.js — Google Sheets read/write layer for Volmera Growth Engine
// Sheet: 1tbGNSHsUZihsR4vciqV5dXlIf4Iy4nzlZS2-8DkRBIg | Growth tab gid: 1961568046
//
// Column map (0-indexed, row 1 = headers, data starts row 2):
//  A(0): Profile URL      B(1): Name           C(2): Title
//  D(3): Company          E(4): Relevant Products  F(5): Operation Estimate
//  G(6): ICP Reason       H(7): ICP Score      I(8): Request Sent
//  J(9): Request Accepted K(10): Last Message Sent
//  L(11): Marketing Messaging EN   M(12): Follow up Message EN
//  N(13): Status          O(14): Last Pulse     P(15): Location
//
// Changes tab gid: 48533851
//  A: Profile URL  B: Name  C: Old Title  D: Old Company  E: Detected On  F: Congrats Sent
//
// Blacklist tab:
//  A: Name  B: Profile URL  C: Reason  D: Date Added  E: Batch 4 Check

import { google } from 'googleapis';
import { glog } from './growth-logger.js';

const SPREADSHEET_ID = process.env.GROWTH_SHEET_ID || '1tbGNSHsUZihsR4vciqV5dXlIf4Iy4nzlZS2-8DkRBIg';
const GROWTH_GID     = '1961568046';
const BLACKLIST_TAB  = 'Blacklist';
const CHANGES_TAB    = 'Changes';

// Column indices (0-based)
export const COL = {
  PROFILE_URL:      0,
  NAME:             1,
  TITLE:            2,
  COMPANY:          3,
  RELEVANT_PRODS:   4,
  OP_ESTIMATE:      5,
  ICP_REASON:       6,
  ICP_SCORE:        7,
  REQUEST_SENT:     8,
  REQUEST_ACCEPTED: 9,
  LAST_MSG_SENT:    10,
  MARKETING_MSG:    11,
  FOLLOWUP_MSG:     12,
  STATUS:           13,
  LAST_PULSE:       14,
  LOCATION:         15,
};

let _sheets          = null;
let _sheetName       = null;   // Contacts tab name, discovered on first use
let _blacklistSheetId = null;  // Blacklist tab numeric sheetId, discovered on first use

function getAuth() {
  const auth = new google.auth.OAuth2(
    process.env.GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET,
    GOOGLE_REDIRECT_URI
  );
  auth.setCredentials({ refresh_token: process.env.GOOGLE_REFRESH_TOKEN });
  return auth;
}

// OAuth redirect URI — always localhost, never ngrok/production
const GOOGLE_REDIRECT_URI = 'http://localhost:3000/oauth/google/callback';

function getSheetsClient() {
  if (!_sheets) {
    _sheets = google.sheets({ version: 'v4', auth: getAuth() });
  }
  return _sheets;
}

async function getSheetName() {
  if (_sheetName) return _sheetName;
  const client = getSheetsClient();
  const meta = await client.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  const sheet = meta.data.sheets.find(s => String(s.properties.sheetId) === GROWTH_GID);
  if (!sheet) throw new Error(`Sheet with gid ${GROWTH_GID} not found in spreadsheet`);
  _sheetName = sheet.properties.title;
  glog.info(`[Sheets] Growth tab name resolved: "${_sheetName}"`);
  return _sheetName;
}

async function getBlacklistSheetId() {
  if (_blacklistSheetId !== null) return _blacklistSheetId;
  const client = getSheetsClient();
  const meta = await client.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  const sheet = meta.data.sheets.find(s => s.properties.title === BLACKLIST_TAB);
  if (!sheet) throw new Error(`Blacklist tab not found in spreadsheet`);
  _blacklistSheetId = sheet.properties.sheetId;
  return _blacklistSheetId;
}

// Deletes a single row by 1-based rowIndex from any sheet (by numeric sheetId)
async function deleteSheetRow(sheetId, rowIndex1Based) {
  const client = getSheetsClient();
  await client.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      requests: [{
        deleteDimension: {
          range: {
            sheetId,
            dimension: 'ROWS',
            startIndex: rowIndex1Based - 1,  // 0-based inclusive
            endIndex:   rowIndex1Based,       // 0-based exclusive
          },
        },
      }],
    },
  });
}

// ── READ ──────────────────────────────────────────────────────────────────────

/** Returns all data rows (row 2 onwards) as objects with rowIndex (1-based sheet row). */
export async function getAllRows() {
  const name = await getSheetName();
  const client = getSheetsClient();
  const res = await client.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${name}!A:P`,
  });
  const rows = res.data.values || [];
  // Skip header row (index 0 = row 1)
  return rows.slice(1).map((row, i) => rowToObj(row, i + 2));
}

/** Returns rows matching one or more status values. */
export async function getRowsByStatus(...statuses) {
  const all = await getAllRows();
  return all.filter(r => statuses.includes(r.status));
}

/**
 * Returns all rows in the Blacklist tab as objects.
 * Columns: A=Name, B=Profile URL, C=Reason, D=Date Added, E=Batch 4 Check
 */
export async function getBlacklist() {
  const client = getSheetsClient();
  try {
    const res = await client.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: `${BLACKLIST_TAB}!A:E`,
    });
    const rows = res.data.values || [];
    return rows.slice(1)
      .map((row, i) => ({
        rowIndex:    i + 2,
        name:        row[0] || '',
        profileUrl:  row[1] || '',
        reason:      row[2] || '',
        dateAdded:   row[3] || '',
        batch4Check: row[4] || '',
      }))
      .filter(r => r.name || r.profileUrl);
  } catch (e) {
    glog.error('[Sheets] Blacklist fetch failed:', e.message);
    return [];
  }
}

/** Append a person to the Blacklist tab. */
export async function appendBlacklist({ name, profileUrl, reason }) {
  const client = getSheetsClient();
  await client.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${BLACKLIST_TAB}!A:E`,
    valueInputOption: 'RAW',
    requestBody: { values: [[name, profileUrl, reason, today(), '']] },
  });
}

/** Update the Batch 4 Check date (col E) for a blacklist row. */
export async function updateBlacklistCheck(rowIndex, date) {
  const client = getSheetsClient();
  await client.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `${BLACKLIST_TAB}!E${rowIndex}`,
    valueInputOption: 'RAW',
    requestBody: { values: [[date]] },
  });
}

/** Delete a row from the Blacklist tab by its 1-based rowIndex. */
export async function deleteBlacklistRow(rowIndex) {
  glog.info(`[Sheets] DELETE Blacklist row ${rowIndex}`);
  const sheetId = await getBlacklistSheetId();
  await deleteSheetRow(sheetId, rowIndex);
  glog.info(`[Sheets] DELETE Blacklist row ${rowIndex} — confirmed`);
}

/** Delete a row from the Contacts tab by its 1-based rowIndex. */
export async function deleteContactRow(rowIndex) {
  glog.info(`[Sheets] DELETE Contacts row ${rowIndex}`);
  await deleteSheetRow(Number(GROWTH_GID), rowIndex);
  glog.info(`[Sheets] DELETE Contacts row ${rowIndex} — confirmed`);
}

/** Count of each status across all rows. */
export async function getStatusCounts() {
  const all = await getAllRows();
  const counts = {};
  for (const r of all) {
    const s = r.status || 'unknown';
    counts[s] = (counts[s] || 0) + 1;
  }
  return counts;
}

// ── WRITE ─────────────────────────────────────────────────────────────────────

/** Append multiple rows in a single API call. rows = array of data objects. */
export async function appendRows(rows) {
  if (!rows || rows.length === 0) return;
  const name = await getSheetName();
  const client = getSheetsClient();
  const values = rows.map(objToRow);
  await client.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${name}!A:P`,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values },
  });
}

/** Append a new row. data is a partial object — missing fields left blank. */
export async function appendRow(data) {
  const name = await getSheetName();
  const client = getSheetsClient();
  const row = objToRow(data);
  await client.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${name}!A:P`,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: [row] },
  });
}

/** Update specific columns in a row by its 1-based sheet rowIndex. */
export async function updateRow(rowIndex, data) {
  const name = await getSheetName();
  const client = getSheetsClient();

  // Build individual cell updates for only the provided fields
  const updates = [];
  for (const [key, colIdx] of Object.entries(COL)) {
    const fieldName = colKeyToField(key);
    if (data[fieldName] !== undefined) {
      const colLetter = String.fromCharCode(65 + colIdx); // A=65
      updates.push({
        range: `${name}!${colLetter}${rowIndex}`,
        values: [[data[fieldName]]],
      });
    }
  }

  if (updates.length === 0) return;

  await client.spreadsheets.values.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      valueInputOption: 'USER_ENTERED',
      data: updates,
    },
  });
}

/** Batch update multiple rows. updates = [{ rowIndex, data }, ...] */
export async function batchUpdateRows(updates) {
  const name = await getSheetName();
  const client = getSheetsClient();

  const batchData = [];
  for (const { rowIndex, data } of updates) {
    for (const [key, colIdx] of Object.entries(COL)) {
      const fieldName = colKeyToField(key);
      if (data[fieldName] !== undefined) {
        const colLetter = String.fromCharCode(65 + colIdx);
        batchData.push({
          range: `${name}!${colLetter}${rowIndex}`,
          values: [[data[fieldName]]],
        });
      }
    }
  }

  if (batchData.length === 0) return;

  await client.spreadsheets.values.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: { valueInputOption: 'RAW', data: batchData },
  });
}

// ── DEDUP ─────────────────────────────────────────────────────────────────────

/** Given a list of URLs, returns a Set of those already in the sheet. */
export async function findExistingUrls(urls) {
  const all = await getAllRows();
  const existing = new Set(all.map(r => r.profileUrl?.trim()).filter(Boolean));
  return new Set(urls.filter(u => existing.has(u.trim())));
}

// ── MESSAGE HISTORY ───────────────────────────────────────────────────────────

/**
 * Append a message to the Marketing Messaging EN column thread.
 * direction: 'sent' | 'received'
 */
export async function appendMarketingMessage(rowIndex, direction, senderLabel, textEN) {
  const all = await getAllRows();
  const row = all.find(r => r.rowIndex === rowIndex);
  const existing = row?.marketingMsg || '';
  const date = today();
  const entry = `[${date}] ${direction === 'sent' ? `Volmera → ${senderLabel}` : `${senderLabel} → Volmera`}: ${textEN}`;
  const updated = existing ? existing + '\n' + entry : entry;
  await updateRow(rowIndex, { marketingMsg: updated });
}

// ── HELPERS ───────────────────────────────────────────────────────────────────

/** Returns today's date as dd-mm-yyyy */
export function today() {
  const d = new Date();
  const dd = String(d.getDate()).padStart(2, '0');
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const yyyy = d.getFullYear();
  return `${dd}-${mm}-${yyyy}`;
}

function rowToObj(row, rowIndex) {
  return {
    rowIndex,
    profileUrl:      row[COL.PROFILE_URL]      || '',
    name:            row[COL.NAME]             || '',
    title:           row[COL.TITLE]            || '',
    company:         row[COL.COMPANY]          || '',
    relevantProds:   row[COL.RELEVANT_PRODS]   || '',
    opEstimate:      row[COL.OP_ESTIMATE]       || '',
    icpReason:       row[COL.ICP_REASON]       || '',
    icpScore:        Number(row[COL.ICP_SCORE]) || 0,
    requestSent:     row[COL.REQUEST_SENT]     || '',
    requestAccepted: row[COL.REQUEST_ACCEPTED] || '',
    lastMsgSent:     row[COL.LAST_MSG_SENT]    || '',
    marketingMsg:    row[COL.MARKETING_MSG]    || '',
    followupMsg:     row[COL.FOLLOWUP_MSG]     || '',
    status:          row[COL.STATUS]           || '',
    lastPulse:       row[COL.LAST_PULSE]       || '',
    location:        row[COL.LOCATION]         || '',
  };
}

function objToRow(data) {
  const row = new Array(16).fill('');
  const map = {
    profileUrl:      COL.PROFILE_URL,
    name:            COL.NAME,
    title:           COL.TITLE,
    company:         COL.COMPANY,
    relevantProds:   COL.RELEVANT_PRODS,
    opEstimate:      COL.OP_ESTIMATE,
    icpReason:       COL.ICP_REASON,
    icpScore:        COL.ICP_SCORE,
    requestSent:     COL.REQUEST_SENT,
    requestAccepted: COL.REQUEST_ACCEPTED,
    lastMsgSent:     COL.LAST_MSG_SENT,
    marketingMsg:    COL.MARKETING_MSG,
    followupMsg:     COL.FOLLOWUP_MSG,
    status:          COL.STATUS,
    lastPulse:       COL.LAST_PULSE,
    location:        COL.LOCATION,
  };
  for (const [field, colIdx] of Object.entries(map)) {
    if (data[field] !== undefined) row[colIdx] = String(data[field]);
  }
  return row;
}

// Maps COL key (e.g. "PROFILE_URL") → field name (e.g. "profileUrl")
const _colKeyToField = {
  PROFILE_URL:      'profileUrl',
  NAME:             'name',
  TITLE:            'title',
  COMPANY:          'company',
  RELEVANT_PRODS:   'relevantProds',
  OP_ESTIMATE:      'opEstimate',
  ICP_REASON:       'icpReason',
  ICP_SCORE:        'icpScore',
  REQUEST_SENT:     'requestSent',
  REQUEST_ACCEPTED: 'requestAccepted',
  LAST_MSG_SENT:    'lastMsgSent',
  MARKETING_MSG:    'marketingMsg',
  FOLLOWUP_MSG:     'followupMsg',
  STATUS:           'status',
  LAST_PULSE:       'lastPulse',
  LOCATION:         'location',
};
function colKeyToField(key) { return _colKeyToField[key] || key.toLowerCase(); }

// ── CHANGES TAB ───────────────────────────────────────────────────────────────
// Columns: A=Profile URL, B=Name, C=Old Title, D=Old Company, E=Detected On, F=Congrats Sent

/** Append a detected job change to the Changes tab. */
export async function appendChange({ profileUrl, name, oldTitle, oldCompany }) {
  const client = getSheetsClient();
  const detectedOn = today();
  await client.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${CHANGES_TAB}!A:F`,
    valueInputOption: 'RAW',
    requestBody: { values: [[profileUrl, name, oldTitle, oldCompany, detectedOn, '']] },
  });
}
