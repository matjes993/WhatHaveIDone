# Nomolo — Learnings

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
