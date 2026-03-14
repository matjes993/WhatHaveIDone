# Nomolo — Session Notes

## 2026-03-13 (session 2): Life Map + Aliases + Automaton Powers + Architecture

### What we built
- **Life Map** (replaces Sources/Raid Targets page) — vertical territory cards with inline expand, loot emoji indicators, progress bars, impact scores, category badges (Live/API/Import), relative timestamps, treasure summary bar
- **Aliases page** ("Many Faces") — discovers user's identities across platforms by scanning Gmail/Contacts/Messages vault data. Groups by type (emails, names, orgs) with villain icons and usage counts
- **Automaton Power Levels** — 4-level permission system in Settings (Cabin Boy/First Mate/Quartermaster/Captain). Saves to config.yaml, UI shows radio card grid
- **Loot type fix** — new `star_chart` (⭐) type for bookmarks, `waypoint` (📍) reassigned to locations/maps. 14 total loot types
- **Nav updates** — "Raid Targets" → "Life Map"/"Data Map", added "Many Faces"/"Identities" between Life Map and Loot Log

### Architecture decisions
- **One repo, clean separation** — decided against splitting into 3 repos (overhead > benefit at current scale). Instead: clean one-way dependency rule: `core/` ← `web/`, `core/` ← `mcp_server.py`, but `core/` never imports from `web/`
- **Two-terminal workflow** — parallel Claude sessions using git worktrees: `Nomolo-core/` on `core/*` branches, `Nomolo-web/` on `web/*` branches
- **Knowledge lifecycle layer** (planned) — `core/knowledge/` with linker, insights, normalizer, forgetter, graph modules. Sits between raw collection and query
- **Entrance Door API** (parked, v2.0+) — concept for LLM-guarded query gateway to let others safely query your knowledge core. Needs knowledge layer first

### Files created
- `core/aliases.py` — alias extraction from vault data with caching
- `web/templates/aliases.html` — aliases page template with inline CSS

### Files modified
- `web/rpg.py` — star_chart loot type, waypoint reassigned, jargon updates
- `web/server.py` — Life Map computed fields, `/aliases` + `/api/aliases` routes, automaton_power_level
- `web/templates/base.html` — nav: Life Map + Many Faces links
- `web/templates/sources.html` — complete rewrite as territory cards
- `web/templates/settings.html` — Automaton Powers section
- `web/static/css/style.css` — ~400 lines of Life Map styles
- `web/static/js/app.js` — saveAutomatonPower() function

### What's next
- Set up git worktrees for parallel core/web development
- Build `core/knowledge/` layer (linker, insights, normalizer, forgetter)
- Improve collector integration into `core/collectors/`
- File upload UI for import-based collectors

---

## 2026-03-13: Records Browser + Sources Page + New Collectors + UX Overhaul

### What we built
- **Records browser** (`/records`) — full-text search + paginated browsing of all 40k+ vault records, with source filtering, sort controls, and record detail modal
- **Sources page** (`/sources`) — shows connected vs available sources, grouped by category (Mac local, Google API, import-based), with click-to-collect actions
- **3-screen welcome journey** — stripped the bloated 5-screen journey (graph, leaderboard, stat pills, expansion cards, gateway, fun fact quiz) down to a minimal 3-screen flow: hook → work → done
- **Open vault folder** — button to reveal vault directory in Finder
- **WhatsApp collector** (`collectors/whatsapp.py`) — parses WhatsApp chat text exports
- **Telegram collector** (`collectors/telegram.py`) — parses Telegram Desktop JSON exports
- **Slack collector** (`collectors/slack.py`) — parses Slack workspace exports (channel JSON files)
- **Navigation cleanup** — sidebar trimmed from 5 links (Dashboard, Scanner, Quests, Achievements, Timeline) to 3 (Dashboard, Records, Sources)
- **Datetime bug fix** — `_extract_date()` in `web/game.py` mixed offset-naive and offset-aware datetimes, crashing the dashboard and causing an infinite redirect loop
- **Init flag** — returning users with data go straight to dashboard or records, never loop back to welcome

### Architecture decisions
- **No re-scan on return**: `_has_any_data()` check prevents welcome flow for initialized users. Dashboard failure redirects to `/records` instead of welcome
- **Records API reads JSONL directly**: paginated endpoint streams through vault entries with sort/filter. Search uses existing hybrid BM25+vector engine
- **Sources catalog is server-side**: 24 sources defined in the `/sources` route, matched against vault contents to show connected/available state
- **Import collectors use same vault pattern**: `flush_entries` + `processed_ids.txt` for dedup, matching Gmail/Contacts/Calendar pattern

### Files created
- `web/templates/records.html` — records browser template
- `web/templates/sources.html` — sources management template
- `collectors/whatsapp.py` — WhatsApp chat export parser
- `collectors/telegram.py` — Telegram JSON export parser
- `collectors/slack.py` — Slack workspace export parser

### Files modified
- `web/server.py` — added `/records`, `/sources`, `/api/records`, `/api/open-vault-folder` endpoints + init redirect fix
- `web/templates/base.html` — sidebar nav trimmed to Dashboard/Records/Sources
- `web/templates/welcome.html` — simplified 3-screen journey + expert modal restored
- `web/static/js/app.js` — records browser JS, source collection, vault folder opener
- `web/static/css/style.css` — records browser + sources page + expert modal styles
- `web/game.py` — datetime normalization fix in `_extract_date()`
- `collectors/local_mac.py` — top 3 leaderboard (was top 5)

### What's next
- File upload UI for import-based collectors (WhatsApp, Telegram, Slack, etc.)
- AI-powered insights layer (RAG chat over vault data)
- Email signup flow (growth hook)
- Marketing site

---

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
- ~~Polish dashboard with source badges~~ Done (Sources page)
- ~~Expert Mode for Google API credentials~~ Done (Expert modal)
- Email signup flow (growth hook)
- Vercel marketing site
