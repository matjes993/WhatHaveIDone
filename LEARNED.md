# Nomolo — Learnings

## 2026-03-14 (session 3, part 3)

- **Prefer richest entity on identifier lookup**: When the same email/phone appears on multiple entities (e.g., a bare person from email parsing AND a full contact record), deterministic `fetchone()` may pick the wrong one. Always `ORDER BY length(properties) DESC` to get the most complete entity. This is a general principle: when deduplication hasn't run yet, disambiguate by richness.
- **Overlap detection needs both path and extension matching**: A file at `Library/Mail/V10/msg.emlx` should be skipped by the deep scanner because Mail.app already has a dedicated collector. Matching only by path misses files in unexpected locations; matching only by extension produces false positives. The two-layer approach (path pattern OR extension) catches both cases with minimal false positives.
- **Skip system directories early, not late**: Checking SKIP_DIRS in `os.walk` via `dirs[:]` in-place modification is both correct and performant — it prunes entire subtrees. Doing the check later (per-file) wastes time walking directories that produce no results.
- **Query accuracy benchmarks should test the full pipeline**: Testing individual components (adapter, resolver, store) catches unit-level bugs. But accuracy of end-to-end queries (find person by email, count relationships, check entity resolution) reveals integration issues like identifier attachment, merge ordering, and property inheritance that unit tests miss.

## 2026-03-14 (session 3, continued)

- **Adapters are the right abstraction layer**: One adapter per source, each yielding CanonicalRecords. Adding a new source = one function, zero changes to graph builder or entity resolution. The adapter pattern makes 11 sources feel like one.
- **Default limits bite in tests**: `find_entities(limit=100)` silently truncates results. Always use `count_entities()` for assertions about totals. This applies to any paginated API — never assume the default page size covers everything.
- **Field name mismatches between generators and adapters are inevitable**: The Slack generator used `"sender"` but the adapter expected `"user"`. When you control both ends, pick one convention and stick to it. When you don't, try multiple field names with fallbacks.
- **Integration tests catch what unit tests miss**: All 117 unit tests passed, but the integration test revealed adapter bugs, field mismatches, and query limit issues that only surface when real data flows through the full pipeline.

## 2026-03-14 (session 3)

- **Three pillars, not two**: core (storage) and web (display) aren't enough. The agent layer (LLM reasoning) is the actual product — it's what makes raw data useful. Keep it architecturally separate so the core stays deterministic and testable without LLM dependencies.
- **JSONL vault as single source of truth**: Derived indexes (SQLite graph, ChromaDB vectors) should always be rebuildable from raw vault data. This makes the system portable — copy one folder, everything regenerates on any machine.
- **Entity resolution is a 3-tier problem**: Start with cheap deterministic matching (email/phone exact match catches 80%). Use probabilistic matching (Jaro-Winkler) for ambiguous names. Save expensive LLM resolution for the agent layer, where it's metered and optional.
- **Bitemporal modeling prevents data loss**: Tracking both "when was this true in reality" and "when did we learn this" means you never lose historical context. People change jobs, get married, move cities — the graph should reflect the full timeline, not just the current snapshot.
- **Append-only annotations are worth the complexity**: Letting scrolls (plugins) only ADD data, never modify, means a bad scroll can never corrupt your vault. Uninstall = remove annotations, original data untouched. The read-time merge of original + annotations adds complexity but the safety guarantee is worth it.
- **Community plugins need two safety tiers**: Requiring agent review for every plugin kills contribution momentum. Safe scrolls (pure computation, no I/O) can run without review. Power scrolls (network, dependencies) need review. This balances openness with safety.
- **External identifiers bridge private and public knowledge**: ISBN, UPC, DOI, IMDB IDs let you connect personal data (your books, purchases, movies) to public metadata without sharing anything. Design the identifiers table early — it's cheap to store and enables massive enrichment later.
- **Hypotheses are the graph's curiosity engine**: Instead of only storing confirmed facts, track suspected connections and data gaps. This feeds gamification (quests to resolve mysteries) and makes the system actively smarter over time.
- **Machine adaptivity should be automatic**: Don't force users to choose embedding models. Detect hardware, pick the right tier, let power users override. Re-embedding on upgrade is a one-time cost, not a migration nightmare, because the JSONL vault is the source of truth.

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
