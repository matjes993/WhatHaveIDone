# Nomolo — Learnings

## 2026-03-13 (session 2)

- **Don't split repos prematurely**: OpenClaw has 22 repos (many contributors, independent lifecycles). CLI-Anything has 1 repo (one team, full control). Nomolo is closer to CLI-Anything — one person + AI. Separate repos add coordination tax (dependency versioning, cross-repo PRs, interface freezing) without benefit until you actually have independent teams.
- **Clean module boundaries > repo boundaries**: The one-way dependency rule (`core/` ← `web/`, `core/` ← `mcp`, never reverse) gives the same isolation as separate repos without the overhead. Git worktrees enable parallel work on the same repo.
- **The missing layer is "thinking"**: collect → store → display skips the most valuable step. A knowledge lifecycle layer (linking, enriching, normalizing, forgetting) is what turns raw data into something worth querying.
- **Forgetting is as important as remembering**: Users need to selectively delete data with cascade awareness — if you forget a person, insights referencing them need updating. A "forgetting log" (you deleted something, but you know *that* you deleted it) preserves intentionality.
- **LLM-as-gatekeeper is clever but not deterministic**: Using an LLM to judge whether an external query is safe sounds elegant, but prompt injection can bypass it. Hard technical limits (field-level access, rate limits, token budgets) must be the real enforcement layer, with LLM as a convenience layer on top.

## 2026-03-13

- **Simplicity beats features**: A 5-screen journey with graphs, leaderboards, stat pills, expansion cards, fun fact quizzes, and badges was "complete shit." A 3-screen flow (hook → work → done) with a breathing orb and a big number is what users actually want. When in doubt, remove.
- **Datetime mixing crashes Python**: `datetime.fromisoformat()` returns naive datetimes for strings without timezone, but `datetime.fromtimestamp(val, tz=timezone.utc)` returns aware ones. Mixing them in `min()`/`max()` throws "can't compare offset-naive and offset-aware datetimes." Always normalize to UTC-aware.
- **Dashboard fallback loops are deadly**: If `/` catches dashboard errors by falling back to `welcome.html`, and the welcome page saves state that causes auto-resume back to `/`, you get an infinite loop. Never fall back to welcome for initialized users — redirect to a safe page like `/records` instead.
- **Zero-record UX**: If data was already collected, the collection API returns 0 new records. Showing "0 records saved" is confusing. Fall back to vault totals for the display number.
- **Records pagination over JSONL is slow at scale**: Reading 40k+ entries through `read_all_entries()` for pagination loads everything into memory. Works for now but will need cursor-based pagination or an index (FTS5 is already there) for 100k+ vaults.

## 2026-03-12

- **macOS Full Disk Access kills Terminal**: When a user toggles FDA for Terminal in System Settings, macOS terminates the Terminal process. Must persist journey state to disk and auto-resume on restart.
- **macOS deep links work**: `x-apple.systempreferences:com.apple.preference.security?Privacy_AllFiles` opens System Settings directly to the right panel. Massive UX improvement over "navigate to System Settings > Privacy & Security > Full Disk Access".
- **Photos.sqlite schema varies by macOS version**: `ZORIGINALFILENAME` doesn't exist in all versions. Use defensive queries, fall back gracefully.
- **iMessage dates changed format**: After ~2020, iMessage uses nanoseconds since 2001-01-01 instead of seconds. Must detect and handle both.
- **Core Foundation epoch is 2001-01-01**: macOS stores dates as seconds since 2001-01-01, not Unix epoch. Must offset by `datetime(2001, 1, 1)`.
- **Chrome timestamps use WebKit epoch**: Microseconds since 1601-01-01. Different from both Unix and Core Foundation.
- **Local scanning is the growth engine**: Zero friction (one FDA toggle) vs Google OAuth (10+ steps). Start with local, offer Google as "Expert Mode".
- **The leaderboard is the viral moment**: Showing someone their top 5 sites with visit counts is immediately personal and screenshot-worthy. More impactful than generic stats.
- **Collection speed is a viral KPI**: "3,680 URLs archived in 1.0s (3,643/sec)" is impressive and shareable.
- **Never gate scanning behind payment**: Scanning and collection must be free. Monetize the AI layer (RAG, insights, marketplace) on top.
