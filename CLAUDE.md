# Volmera Growth Engine — Claude Code Brief

## What this is
Program 2 of the Volmera AI Platform. LinkedIn outreach automation for Volmera YMS sales into the Brazilian logistics market.

## Where code lives
- Source files: `src/growth-*.js` (this folder)
- Runtime data: `data-growth/`
- Served by: Blog & Post Automation server (port 3000) — server.js imports growth-routes.js from this folder

## Key rule
**Never touch Blog & Post Automation code** except these two lines in server.js:
1. The growth router import
2. The growth lock integration on the LinkedIn scraper

## Full spec
See memory file: `project_growth_engine_brief.md`

## Architecture
- Playwright: loaded via createRequire from Blog & Post node_modules (see growth-browser.js)
- googleapis: installed in THIS folder's node_modules
- All credentials: process.env (same server process as Blog & Post)
- Google Sheet: ID `1tbGNSHsUZihsR4vciqV5dXlIf4Iy4nzlZS2-8DkRBIg`, tab gid `1961568046`
- LinkedIn session: shared with Blog & Post at `../Volmera Blog & Post Automation/data/linkedin-session.json`

## Jobs (never mix, never auto-chain)
1. Crawl → status: Scraped
2. Fetch → status: Fetched
3. Enrich → status: Enriched
4. Pulse Check → runs on: Pending, Followup, Engaged, Lead
5. Invite → status: Enriched → Pending → Ready
6. Marketing Message → status: Ready → Engaged
7. Follow-up → status: Engaged → Followup → Dead (7d / 14d)

## Status map (Google Sheet column N)
Scraped | Fetched | Enriched | Pending | Ready | Engaged | Followup | Lead | Dead | Success
