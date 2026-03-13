# Nomolo — Session Notes

## 2026-03-12: Web UI + Local Mac Collectors + Identity Snapshot

### What we built
- **Full web UI** (`web/`) — FastAPI server with gamified welcome journey
- **Local Mac scanner** (`web/local_scanner.py`) — preview-only scan of 9 macOS data sources
- **Local Mac collectors** (`collectors/local_mac.py`) — vault-writing collectors for Contacts, Calendar, iMessage, Notes, Mail, Photos, Safari, Chrome Bookmarks
- **Identity snapshot** — "magic moment" with top 5 leaderboard + stats after collection
- **3-phase knowledge graph** — hero nodes (visit counts) > category clusters > local Mac sources (found/locked)
- **FDA permission guide** — polished modal with macOS deep link + Terminal restart handling
- **Journey state persistence** — auto-resume after FDA-triggered Terminal restart
- **Collection speed KPI** — tracks and displays URLs/sec as viral metric
- **Test suite** (`tests/test_web_journey.py`) — 146 end-to-end tests, all passing

### Architecture decisions
- **Local-first**: all data stays on user's machine, zero external API calls for scanning/collection
- **Two-step collection**: quick glimpse (seconds) then background deep collection
- **Free tier = scanning + collection**: no LLM tokens needed. AI features (RAG chat, insights) = paid tier
- **Expert Mode planned**: Google API credentials path for technical users
- **Default path**: local Mac scanning via Full Disk Access (one toggle vs 10+ steps for Google OAuth)

### Files created/modified
- `web/server.py` — FastAPI server, all API endpoints
- `web/local_scanner.py` — macOS database preview scanner
- `web/static/js/app.js` — journey UI, graph animation, leaderboard, FDA guide
- `web/static/css/style.css` — all styles
- `web/templates/welcome.html` — welcome journey template
- `web/templates/base.html`, `dashboard.html`, etc. — other templates
- `collectors/local_mac.py` — local Mac vault collectors + identity snapshot
- `tests/test_web_journey.py` — 146 E2E tests

### What's next
- Polish dashboard with source badges (Email, Contacts, etc.)
- Expert Mode for Google API credentials (JSON import flow)
- Email signup flow (growth hook)
- Vercel marketing site
- Knowledge graph on dashboard
- Background deep scan after FDA
