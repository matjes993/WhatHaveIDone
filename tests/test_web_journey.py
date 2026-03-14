"""
NOMOLO Web Journey — End-to-End Test Suite

Tests the complete user journey across all pages and API endpoints.
Runs against a live local server. No external dependencies beyond stdlib.

Usage:
    python tests/test_web_journey.py
    python tests/test_web_journey.py --verbose
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = os.environ.get("NOMOLO_TEST_URL", "http://localhost:3000")
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VAULT_ROOT = os.path.join(PROJECT_ROOT, "vaults")
VERBOSE = "--verbose" in sys.argv or "-v" in sys.argv

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_passed = 0
_failed = 0
_errors = []


def _get(path, expect_status=200):
    """GET request, return (status, body)."""
    url = BASE_URL + path
    try:
        req = urllib.request.Request(url)
        resp = urllib.request.urlopen(req, timeout=30)
        body = resp.read().decode("utf-8")
        return resp.status, body
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8") if e.fp else ""
        return e.code, body
    except Exception as e:
        return 0, str(e)


def _post(path, data=None, content_type=None):
    """POST request, return (status, body)."""
    url = BASE_URL + path
    try:
        post_data = data if data else b""
        req = urllib.request.Request(url, data=post_data, method="POST")
        if content_type:
            req.add_header("Content-Type", content_type)
        resp = urllib.request.urlopen(req, timeout=60)
        body = resp.read().decode("utf-8")
        return resp.status, body
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8") if e.fp else ""
        return e.code, body
    except Exception as e:
        return 0, str(e)


def _json_get(path):
    """GET JSON endpoint, return parsed dict."""
    status, body = _get(path)
    if status != 200:
        return {"_error": f"HTTP {status}", "_body": body[:200]}
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return {"_error": "Invalid JSON", "_body": body[:200]}


def _json_post(path, data=None, content_type=None):
    """POST JSON endpoint, return parsed dict."""
    status, body = _post(path, data=data, content_type=content_type)
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return {"_error": f"HTTP {status}", "_body": body[:200]}


def check(name, condition, detail=""):
    """Assert a test condition."""
    global _passed, _failed
    if condition:
        _passed += 1
        if VERBOSE:
            print(f"  PASS  {name}")
    else:
        _failed += 1
        msg = f"  FAIL  {name}"
        if detail:
            msg += f" — {detail}"
        print(msg)
        _errors.append(name)


def section(title):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


# ===========================================================================
# TEST SUITES
# ===========================================================================

def test_server_health():
    """Verify the server is running and serves basic pages."""
    section("1. Server Health")

    status, body = _get("/")
    check("Root page loads (200)", status == 200)
    check("Root page has HTML", "<html" in body.lower())

    status, _ = _get("/static/css/style.css")
    check("CSS static file loads", status == 200)

    status, _ = _get("/static/js/app.js")
    check("JS static file loads", status == 200)

    status, _ = _get("/nonexistent-page")
    check("404 for unknown routes", status == 404)


def test_base_template():
    """Verify shared base template elements are present on all pages."""
    section("2. Base Template (Shared Elements)")

    status, body = _get("/")
    check("Base: has trust badge", 'trust-badge' in body)
    check("Base: has LOCAL ONLY text", 'LOCAL ONLY' in body)
    check("Base: has jargon toggle", 'id="jargon-toggle"' in body)
    check("Base: has sidebar", 'id="sidebar"' in body)
    check("Base: has toast container", 'id="toast-container"' in body)
    check("Base: has NomoloBridge init", 'NomoloBridge.init()' in body)

    # Sidebar navigation links
    check("Nav: SCUMM Bar / Dashboard", 'href="/"' in body)
    check("Nav: Automaton / AI Chat", 'href="/automaton"' in body)
    check("Nav: Life Map / Data Map", 'href="/sources"' in body)
    check("Nav: Many Faces / Identities", 'href="/aliases"' in body)
    check("Nav: Loot Log / Records", 'href="/records"' in body)
    check("Nav: Ship's Helm / Settings", 'href="/settings"' in body)

    # Jargon system
    check("Base: jargon data-rpg attrs", 'data-rpg=' in body)
    check("Base: jargon data-real attrs", 'data-real=' in body)

    # Pirate proverbs
    check("Base: has proverb container", 'id="pirate-proverb"' in body)
    check("Base: has version", 'v0.1.0' in body)


def test_intro_cinematic():
    """Test the intro cinematic page."""
    section("3. Intro Cinematic")

    status, body = _get("/intro")
    check("Intro page loads", status == 200)
    check("Intro: has skip button", 'id="intro-skip"' in body)
    check("Intro: has 6 scenes", body.count('data-scene=') == 6)
    check("Intro: scene 0 — Flatcloud", 'data flows like water' in body)
    check("Intro: scene 1 — Armada", 'Merchant Lords' in body)
    check("Intro: scene 2 — Your Data", 'locked away' in body)
    check("Intro: scene 3 — Awakening", 'small ship set sail' in body)
    check("Intro: scene 4 — NOMOLO reveal", 'intro__title' in body and 'NOMOLO' in body)
    check("Intro: scene 5 — The Call", 'intro__typewriter' in body)
    check("Intro: has CTA", 'intro__cta' in body)
    check("Intro: sidebar hidden", '.sidebar' in body.lower() and 'display: none' in body.lower())
    check("Intro: calls NomoloBridge.initIntro()", 'NomoloBridge.initIntro()' in body)
    check("Intro: has ocean animation", 'intro__ocean' in body)
    check("Intro: has ship animation", 'intro__ship' in body)
    check("Intro: has floating items", 'intro__float-item' in body)


def test_welcome_page():
    """Test the welcome/onboarding journey page."""
    section("4. Welcome Page (Onboarding Journey)")

    status, body = _get("/welcome")
    check("Welcome page loads", status == 200)

    # Step 1: Hook
    check("Has journey container", 'id="journey"' in body)
    check("Has Begin button", 'id="begin-btn"' in body)
    check("Has Nomolo logo", 'NOMOLO' in body)
    check("Has Set Sail CTA", 'Set Sail' in body)
    check("Has trust text", 'never leaves this ship' in body.lower())

    # Step 2: Working
    check("Has work step", 'id="step-work"' in body)
    check("Has work breathing animation", 'journey__work-breath' in body)

    # Step 3: Done
    check("Has done step", 'id="step-done"' in body)
    check("Has done number counter", 'id="done-number"' in body)
    check("Has SCUMM Bar CTA", 'Enter the SCUMM Bar' in body)

    # FDA Modal
    check("Has FDA modal", 'id="fda-modal"' in body)
    check("FDA: 4 steps", body.count('class="fda-step"') == 4)
    check("FDA: macOS deep link", 'x-apple.systempreferences' in body)
    check("FDA: trust text", 'never modifies' in body.lower())

    # Expert Modal
    check("Has Expert modal", 'id="expert-modal"' in body)
    check("Expert: Phase 1 upload", 'id="expert-phase-upload"' in body)
    check("Expert: Phase 2 auth", 'id="expert-phase-auth"' in body)
    check("Expert: Phase 3 collect", 'id="expert-phase-collect"' in body)
    check("Expert: credential upload zone", 'id="expert-upload-zone"' in body)
    check("Expert: Gmail row", "data-source=\"gmail\"" in body)
    check("Expert: Contacts row", "data-source=\"contacts-google\"" in body)
    check("Expert: Calendar row", "data-source=\"calendar\"" in body)

    # Nerd Mode
    check("Has Nerd toggle", 'id="nerd-toggle"' in body)
    check("Has Nerd panel", 'id="nerd-panel"' in body)
    check("Nerd: show-me-the-matrix", 'show-me-the-matrix' in body)

    # Sidebar hidden
    check("Sidebar hidden on welcome", '.sidebar { display: none' in body or 'sidebar { display: none' in body.lower())

    # NomoloBridge wired
    check("Begin calls NomoloBridge", 'NomoloBridge.beginJourney()' in body)
    check("Journey resume check", 'NomoloBridge.checkJourneyResume()' in body)

    # Starfield
    check("Has starfield", 'id="starfield"' in body)
    check("Has 40 stars", body.count('class="star star--') >= 40)


def test_dashboard_page():
    """Test the RPG dashboard (SCUMM Bar)."""
    section("5. Dashboard / SCUMM Bar")

    status, body = _get("/")
    check("Root page loads", status == 200)

    has_vault = os.path.isdir(VAULT_ROOT) and any(
        os.path.isdir(os.path.join(VAULT_ROOT, d)) for d in os.listdir(VAULT_ROOT)
        if not d.startswith('.')
    ) if os.path.isdir(VAULT_ROOT) else False

    if has_vault:
        # RPG Dashboard elements
        check("Has RPG header", 'rpg__header' in body)
        check("Has level badge", 'rpg__level-badge' in body)
        check("Has pirate title", 'rpg__title' in body)
        check("Has power number", 'rpg__power-number' in body)
        check("Has share button", 'rpg__share-btn' in body)
        check("Has memory state", 'rpg__memory-state' in body)
        check("Has level progress bar", 'rpg__level-bar' in body)

        # Character Stats
        check("Has stats card", 'rpg__card--stats' in body)
        for stat in ['STR', 'WIS', 'DEX', 'INT', 'CHA', 'END']:
            check(f"Has stat: {stat}", stat in body)
        check("Has total power", 'Total Plunder Power' in body or 'Data Coverage' in body)

        # Digital Serotonin (Lobster Principle)
        check("Has serotonin", 'rpg__serotonin' in body)
        check("Has lobster emoji", '🦞' in body or '&#x1F99E;' in body)

        # Loot Inventory
        check("Has inventory card", 'rpg__card--inventory' in body)

        # Power-Ups
        check("Has power-ups card", 'rpg__card--powerups' in body)

        # Memory Tavern
        check("Has tavern card", 'rpg__card--tavern' in body)
        check("Tavern: challenge button", 'Challenge the Bartender' in body or 'Start Quiz' in body)

        # The Armada (Villain Progress)
        check("Has villains card", 'rpg__card--villains' in body)

        # The One (Final Boss)
        check("Has The One section", 'rpg__the-one' in body)

        # Verb Bar (SCUMM-style)
        check("Has verb bar", 'verb-bar' in body)
        check("Verb: Scan", 'data-verb="scan"' in body)
        check("Verb: Collect/Plunder", 'data-verb="collect"' in body)
        check("Verb: Search", 'data-verb="search"' in body)
        check("Verb: Explore", 'data-verb="explore"' in body)

        # Intro replay
        check("Has replay intro button", 'id="replay-intro-btn"' in body)

        # Jargon pairs
        check("Dashboard jargon: SCUMM Bar", 'data-rpg="SCUMM Bar"' in body)
        check("Dashboard jargon: Character Stats", 'data-rpg="Character Stats"' in body)
        check("Dashboard jargon: Loot Inventory", 'data-rpg="Loot Inventory"' in body)
        check("Dashboard jargon: Power-Ups", 'data-rpg="Power-Ups"' in body)
        check("Dashboard jargon: Memory Tavern", 'data-rpg="Memory Tavern"' in body)
        check("Dashboard jargon: The Armada", 'data-rpg="The Armada"' in body)
    else:
        check("Shows welcome/intro (no data)", "journey" in body or "intro" in body or "Begin" in body or "NOMOLO" in body)


def test_sources_page():
    """Test the Life Map / Sources page."""
    section("6. Life Map / Sources")

    status, body = _get("/sources")
    check("Sources page loads", status == 200)

    # Header
    check("Has Life Map title", 'Life Map' in body)
    check("Has territory count", 'territories charted' in body)
    check("Has Loot Everything button", 'Loot Everything' in body or 'Collect All' in body)
    check("Has Scan Horizon button", 'Scan Horizon' in body or 'Rescan' in body)

    # Treasure summary bar
    check("Has treasure bar", 'lifemap-treasure-bar' in body)

    # Territory cards
    check("Has territory container", 'id="lifemap-territories"' in body)
    check("Has territory cards", 'class="territory' in body)
    check("Territory: has row (clickable)", 'territory__row' in body)
    check("Territory: has expand detail", 'territory__detail' in body)
    check("Territory: has progress bar", 'territory__progress' in body)
    check("Territory: has badges", 'territory__badge' in body)
    check("Territory: has CTA buttons", 'territory__cta' in body)
    check("Territory: has impact score", 'territory__impact' in body)

    # Jargon pairs
    check("Sources jargon: raid button", "Raid" in body)

    # Keyboard accessibility
    check("Territory rows have tabindex", 'tabindex="0"' in body)
    check("Territory rows have role=button", 'role="button"' in body)

    # Footer
    check("Has open vault button", 'Open the treasure chest' in body or 'Open in Finder' in body)

    # Island data for JS
    check("Has island data JSON", 'id="island-data"' in body)

    # Relative time rendering
    check("Has relative time elements", 'data-raid-time' in body or 'renderRelativeTimes' in body)


def test_records_page():
    """Test the Loot Log / Records page."""
    section("7. Loot Log / Records")

    status, body = _get("/records")
    check("Records page loads", status == 200)

    # Header
    check("Has Yer Plunder / Records title", 'Yer Plunder' in body or 'Records' in body)
    check("Has record count", 'id="records-total"' in body)

    # Search
    check("Has search input", 'id="records-search"' in body)
    check("Search: pirate placeholder", 'Scan the horizon' in body or 'Search' in body)

    # Sort
    check("Has sort dropdown", 'id="records-sort"' in body)
    check("Sort: newest", 'value="newest"' in body)
    check("Sort: oldest", 'value="oldest"' in body)
    check("Sort: relevance", 'value="relevance"' in body)

    # Filters
    check("Has filter buttons", 'id="records-filters"' in body)
    check("Has All Plunder filter", 'All Plunder' in body or 'All Records' in body)

    # Records list
    check("Has records list", 'id="records-list"' in body)
    check("Has loading state", 'id="records-loading"' in body)

    # Pagination
    check("Has pagination", 'id="records-pagination"' in body)
    check("Has prev button", 'id="records-prev"' in body)
    check("Has next button", 'id="records-next"' in body)

    # Record detail modal
    check("Has record detail modal", 'id="record-detail"' in body)
    check("Detail: has backdrop", 'plunder-detail__backdrop' in body)

    # Footer
    check("Has open vault button", 'plunder-footer__btn' in body)

    # JS init
    check("Records: NomoloBridge.initRecords()", 'NomoloBridge.initRecords()' in body)


def test_aliases_page():
    """Test the Many Faces / Aliases page."""
    section("8. Many Faces / Aliases")

    status, body = _get("/aliases")
    check("Aliases page loads", status == 200)

    # Header
    check("Has Many Faces / Identities title", 'Many Faces' in body or 'Your Identities' in body)
    check("Has known faces count", 'known faces' in body or 'known identities' in body)

    # Primary Identity Card
    check("Has primary identity card", 'aliases-page__primary-card' in body)
    check("Has CAPTAIN / PRIMARY badge", 'CAPTAIN' in body or 'PRIMARY' in body)

    # Check for section structure or empty state
    has_sections = 'aliases-page__section' in body
    has_empty = 'aliases-page__empty' in body
    check("Has alias sections or empty state", has_sections or has_empty)
    if has_sections:
        check("Has Signal Flags / Emails / Names", 'Signal Flags' in body or 'Email Addresses' in body or 'Known Aliases' in body or 'Names' in body)
    if has_empty:
        check("Empty: has CTA to sources", 'href="/sources"' in body)

    # Footer
    check("Has footer text", 'aliases-page__footer' in body)

    # Jargon
    check("Aliases jargon: faces", 'data-rpg="Many Faces"' in body)


def test_automaton_page():
    """Test the Automaton / AI Chat page."""
    section("9. The Automaton / AI Chat")

    status, body = _get("/automaton")
    check("Automaton page loads", status == 200)

    # Header
    check("Has automaton icon", '&#x1F916;' in body or '🤖' in body)
    check("Has title", 'The Automaton' in body or 'AI Assistant' in body)

    # Sidebar
    check("Has chat sidebar", 'id="automaton-sidebar"' in body)
    check("Has new chat button", 'NomoloBridge.newChat()' in body)
    check("Has search chats input", 'id="automaton-search"' in body)
    check("Has pinned section", 'id="automaton-pinned-section"' in body)
    check("Has chat list", 'id="automaton-chat-list"' in body)
    check("Has add folder button", 'NomoloBridge.createFolder()' in body)

    # Main chat area
    check("Has messages container", 'id="automaton-messages"' in body)
    check("Has welcome message", 'id="automaton-welcome"' in body)
    check("Has chat input form", 'id="automaton-query"' in body)
    check("Has send button", 'id="automaton-send"' in body)

    # Context menus
    check("Has chat context menu", 'id="automaton-ctx-menu"' in body)
    check("Has folder context menu", 'id="automaton-folder-ctx-menu"' in body)
    check("Context: rename", "chatCtxAction('rename')" in body)
    check("Context: pin", "chatCtxAction('pin')" in body)
    check("Context: delete", "chatCtxAction('delete')" in body)
    check("Context: move to folder", "chatCtxAction('folder')" in body)

    # Jargon
    check("Automaton jargon: New Transmission", 'data-rpg="New Transmission"' in body)
    check("Automaton jargon: Ship\'s Log", "data-rpg=\"Ship's Log\"" in body or 'Ship&#39;s Log' in body or "Ship's Log" in body)

    # JS init
    check("Automaton: NomoloBridge.initChat()", 'NomoloBridge.initChat()' in body)


def test_achievements_page():
    """Test the Power-Up Gallery / Achievements page."""
    section("10. Power-Up Gallery / Achievements")

    status, body = _get("/achievements")
    check("Achievements page loads", status == 200)

    # Header
    check("Has Power-Up Gallery title", 'Power-Up Gallery' in body or 'Achievement Showcase' in body)
    check("Has unlock count", 'unlocked' in body)

    # Score widget
    check("Has score widget", 'score-widget' in body)
    check("Score: has SVG ring", 'score-widget__ring' in body)

    # Category filters
    check("Has filter buttons", 'achievement-filter' in body)
    check("Filter: All", 'data-category="all"' in body)
    check("Filter: collection/Plundering", 'data-category="collection"' in body)
    check("Filter: quality", 'data-category="quality"' in body)
    check("Filter: exploration/Charting", 'data-category="exploration"' in body)
    check("Filter: dedication/Sea Legs", 'data-category="dedication"' in body)

    # Achievement grid
    check("Has achievement grid", 'id="achievement-grid"' in body)
    check("Has achievement cards", 'achievement-card' in body)
    check("Cards: have SVG icons", 'achievement-card__icon' in body)
    check("Cards: have names", 'achievement-card__name' in body)
    check("Cards: have descriptions", 'achievement-card__desc' in body)
    check("Cards: have status", 'achievement-card__status' in body)

    # Locked/unlocked states
    check("Has locked state class", 'achievement-card--locked' in body)

    # JS
    check("Has filter function", 'filterAchievements(' in body)
    check("Has detail function", 'showAchievementDetail(' in body)


def test_settings_page():
    """Test the Ship's Helm / Settings page."""
    section("11. Ship's Helm / Settings")

    status, body = _get("/settings")
    check("Settings page loads", status == 200)

    # Header
    check("Has Ship's Helm title", "Ship's Helm" in body or "Ship&#x27;s Helm" in body or "Settings" in body)

    # Captain's Identity
    check("Has Captain's Identity section", "Captain's Identity" in body or "Captain&#x27;s Identity" in body or "Your Identity" in body)
    check("Has name input", 'id="setting-user-name"' in body)

    # Treasure Hold / Data Storage
    check("Has Treasure Hold section", 'Treasure Hold' in body or 'Data Storage' in body)
    check("Has vault location", 'Vault Location' in body)
    check("Has total records", 'Total Booty' in body or 'Total Records' in body)
    check("Has source count", 'Islands Plundered' in body or 'Connected Sources' in body)

    # Raiding Orders / Collection
    check("Has Raiding Orders section", 'Raiding Orders' in body or 'Collection' in body)
    check("Has auto-scan toggle", 'id="setting-auto-scan"' in body)
    check("Has rescan button", 'Scan the Horizon' in body or 'Rescan' in body)
    check("Has reset journey", 'Restart the voyage' in body or 'Reset welcome journey' in body)

    # Google Integration
    check("Has Omniscient Eye section", 'The Omniscient Eye' in body or 'Google Integration' in body)
    check("Has Google creds status", 'id="google-creds-status"' in body)

    # LLM API Keys
    check("Has Arcane Scrolls section", 'Arcane Scrolls' in body or 'LLM API Keys' in body)
    check("Has LLM token status", 'id="llm-token-status"' in body)
    check("Has LLM provider dropdown", 'id="llm-provider"' in body)
    check("Has LLM token input", 'id="llm-token"' in body)
    check("LLM: 12 providers", body.count('<option value=') >= 12)

    # Automaton Powers
    check("Has Automaton Powers section", 'Automaton Powers' in body or 'AI Permissions' in body)
    check("Has 4 power levels", body.count('name="automaton_power"') == 4)
    check("Power: Cabin Boy", 'Cabin Boy' in body or 'Read Only' in body)
    check("Power: First Mate", 'First Mate' in body or 'Organize' in body)
    check("Power: Quartermaster", 'Quartermaster' in body or 'Collect' in body)
    check("Power: Captain", 'Captain' in body or 'Full Access' in body)

    # Danger Zone
    check("Has Here Be Dragons section", 'Here Be Dragons' in body or 'Danger Zone' in body)
    check("Has scuttle button", 'Scuttle' in body or 'Clear' in body)

    # Version footer
    check("Has version footer", 'v0.1.0' in body)


def test_api_rpg():
    """Test the RPG data API endpoint."""
    section("12. RPG API")

    data = _json_get("/api/rpg")
    check("RPG API responds", "_error" not in data)

    if "_error" in data:
        return

    check("Has level data", "level" in data)
    check("Has stats", "stats" in data)
    check("Has inventory", "inventory" in data)
    check("Has villains", "villains" in data)
    check("Has serotonin", "serotonin" in data)
    check("Has memory", "memory" in data)
    check("Has the_one", "the_one" in data)
    check("Has power_ups", "power_ups" in data)
    check("Has total_records", "total_records" in data)

    # Level structure
    level = data.get("level", {})
    check("Level has title", "title" in level)
    check("Level has level number", "level" in level)
    check("Level has progress_pct", "progress_pct" in level)
    check("Level has flavor_text", "flavor_text" in level)

    # Stats structure
    stats = data.get("stats", {})
    for stat in ["STR", "WIS", "DEX", "INT", "CHA", "END"]:
        check(f"Stats has {stat}", stat in stats)
    check("Stats has total_power", "total_power" in stats)

    # Serotonin
    sero = data.get("serotonin", {})
    check("Serotonin has level", "level" in sero)
    check("Serotonin has state", "state" in sero)

    # The One
    the_one = data.get("the_one", {})
    check("The One has status", "status" in the_one)
    check("The One status valid", the_one.get("status") in ("locked", "preparing", "ready"))


def test_api_records():
    """Test the records browse/search API."""
    section("13. Records API")

    data = _json_get("/api/records?page=1&per_page=5")
    check("Records API responds", "_error" not in data)

    if "_error" in data:
        return

    check("Has records list", "records" in data)
    check("Has total count", "total" in data)
    check("Has page info", "page" in data)
    check("Has per_page info", "per_page" in data)

    records = data.get("records", [])
    if records:
        first = records[0]
        check("Record has title", "title" in first)
        check("Record has source", "source" in first)
        check("Record has date", "date" in first)


def test_api_aliases():
    """Test the aliases API endpoint."""
    section("14. Aliases API")

    data = _json_get("/api/aliases")
    check("Aliases API responds", "_error" not in data)

    if "_error" in data:
        return

    check("Has primary_name", "primary_name" in data)
    check("Has aliases list or structured aliases",
          "aliases" in data or "email_aliases" in data)


def test_api_chat():
    """Test the chat/Automaton API endpoints."""
    section("15. Automaton Chat API")

    # Chat status
    data = _json_get("/api/chat/status")
    check("Chat status responds", "_error" not in data)

    # List chats
    data = _json_get("/api/chats")
    check("List chats responds", "_error" not in data)
    check("Chats is a list", isinstance(data, list) or "chats" in data)

    # Create a chat
    payload = json.dumps({"title": "Test Transmission"}).encode()
    result = _json_post("/api/chats", data=payload, content_type="application/json")
    check("Create chat responds", "_error" not in result or "id" in result)

    chat_id = result.get("id") or result.get("chat_id")
    if chat_id:
        # Load the chat
        chat_data = _json_get(f"/api/chats/{chat_id}")
        check("Load chat responds", "_error" not in chat_data)
        check("Chat has id", "id" in chat_data or "chat_id" in chat_data)


def test_api_settings():
    """Test the settings API endpoints."""
    section("16. Settings API")

    # LLM token status (GET)
    data = _json_get("/api/settings/llm-token")
    check("LLM token status responds", "_error" not in data)
    check("Has provider or configured flag", "provider" in data or "configured" in data)

    # Credentials status
    data = _json_get("/api/credentials/status")
    check("Credentials status responds", "_error" not in data)
    check("Has credentials flag", "credentials" in data)


def test_api_game_endpoints():
    """Test game-related API endpoints."""
    section("17. Game API Endpoints")

    # Fun facts
    data = _json_get("/api/fun-facts")
    check("Fun facts responds", "_error" not in data or data.get("questions") is not None)

    # Progress
    data = _json_get("/api/progress")
    check("Progress responds", "_error" not in data)

    # Mini-game
    data = _json_get("/api/mini-game")
    check("Mini-game responds", "_error" not in data)

    # Share card
    data = _json_get("/api/share-card")
    check("Share card responds", "_error" not in data)

    # Characters
    data = _json_get("/api/characters")
    check("Characters responds", "_error" not in data)

    # Memory dialogue
    data = _json_get("/api/memory-dialogue")
    check("Memory dialogue responds", "_error" not in data)

    # Dialogue characters
    data = _json_get("/api/dialogue/characters")
    check("Dialogue characters responds", "_error" not in data)


def test_api_vault_stats():
    """Test the vault stats endpoint."""
    section("18. Vault Stats API")

    data = _json_get("/api/vault/stats")
    check("Vault stats returns", "_error" not in data)

    vaults = data.get("vaults", {})
    total = data.get("total_records", 0)
    check("Has vaults dict", isinstance(vaults, dict))
    check("Total records >= 0", total >= 0, f"got {total}")


def test_chrome_analysis_api():
    """Test the Chrome history analysis endpoint."""
    section("19. Chrome Analysis API")

    data = _json_get("/api/chrome-analysis")
    check("Chrome analysis responds", "success" in data or "_error" in data)

    if data.get("success"):
        platforms = data.get("platforms", [])
        check("Found platforms", len(platforms) > 0, f"got {len(platforms)}")
        stats = data.get("stats", {})
        check("Has stats", len(stats) > 0)


def test_browser_collection():
    """Test real browser-chrome collection end-to-end."""
    section("20. Browser Collection (Real Data)")

    result = _json_post("/api/collect/browser-chrome")
    check("Collection starts", result.get("status") == "started",
          result.get("_error", ""))
    task_id = result.get("task_id")
    check("Returns task_id", task_id is not None)

    if not task_id:
        print("    (Skipping — no task_id)")
        return

    # Poll until done
    final_status = None
    for i in range(15):
        time.sleep(2)
        status_data = _json_get(f"/api/collect/browser-chrome/status?task_id={task_id}")
        st = status_data.get("status")
        if VERBOSE:
            print(f"    Poll {i+1}: {st} — {status_data.get('message', '')[:60]}")
        if st in ("completed", "error"):
            final_status = status_data
            break

    check("Collection completes", final_status is not None and
          final_status.get("status") == "completed",
          f"got: {final_status}")


def test_google_sources():
    """Test that Google sources correctly report setup needed."""
    section("21. Google Sources (Setup Flow)")

    for source in ["gmail", "contacts-google", "calendar"]:
        result = _json_post(f"/api/collect/{source}")
        task_id = result.get("task_id")
        if not task_id:
            check(f"{source}: starts", False, str(result))
            continue

        time.sleep(2)
        status = _json_get(f"/api/collect/{source}/status?task_id={task_id}")
        st = status.get("status")

        creds_exist = os.path.exists(os.path.join(PROJECT_ROOT, "credentials.json"))
        if not creds_exist:
            check(f"{source}: needs_setup (no creds)", st == "needs_setup", f"got status={st}")
        else:
            check(f"{source}: needs_auth or runs",
                  st in ("needs_auth", "running", "completed"), f"got status={st}")


def test_file_sources():
    """Test that file-based sources report instructions needed."""
    section("22. File-Based Sources")

    for source in ["contacts-linkedin", "youtube", "music-spotify",
                    "shopping-amazon", "finance-paypal"]:
        result = _json_post(f"/api/collect/{source}")
        task_id = result.get("task_id")
        if not task_id:
            check(f"{source}: starts", False, str(result))
            continue

        time.sleep(1)
        status = _json_get(f"/api/collect/{source}/status?task_id={task_id}")
        st = status.get("status")

        check(f"{source}: needs_file", st == "needs_file", f"got status={st}")


def test_static_assets_integrity():
    """Verify CSS and JS files contain expected content."""
    section("23. Static Assets Integrity")

    # CSS
    status, css = _get("/static/css/style.css")
    check("CSS loads", status == 200)
    check("CSS has journey styles", ".journey" in css)
    check("CSS has dashboard/rpg styles", ".rpg__" in css or ".dashboard" in css)
    check("CSS has plunder page styles", ".plunder-" in css)
    check("CSS has lifemap styles", ".lifemap-" in css)
    check("CSS has automaton styles", ".automaton" in css)
    check("CSS has settings styles", ".settings-" in css)
    check("CSS has achievement styles", ".achievement-" in css)
    check("CSS has intro styles", ".intro" in css)
    check("CSS has verb-bar styles", ".verb-bar" in css)
    check("CSS has nerd-toggle styles", ".nerd-toggle" in css)

    # JS
    status, js = _get("/static/js/app.js")
    check("JS loads", status == 200)
    check("JS has NomoloBridge", "NomoloBridge" in js)
    check("JS has beginJourney", "beginJourney" in js)
    check("JS has toggleNerdMode", "toggleNerdMode" in js)
    check("JS has initRecords", "initRecords" in js)
    check("JS has initChat", "initChat" in js)
    check("JS has initJargonToggle", "initJargonToggle" in js)
    check("JS has escapeHtml", "escapeHtml" in js)
    check("JS has toggleJargon", "toggleJargon" in js)
    check("JS has filterRecords", "filterRecords" in js)
    check("JS has openVaultFolder", "openVaultFolder" in js)


def test_easter_egg_routes():
    """Test easter egg and bonus routes."""
    section("24. Easter Eggs & Bonus Routes")

    # Grog page
    status, body = _get("/grog")
    check("Grog page loads", status == 200)

    # Rubber chicken
    data = _json_get("/api/rubber-chicken")
    check("Rubber chicken responds", "_error" not in data)


def test_idempotent_collection():
    """Test that running collection twice doesn't duplicate data."""
    section("25. Idempotent Collection")

    stats1 = _json_get("/api/vault/stats")
    count1 = stats1.get("vaults", {}).get("Browser", {}).get("records", 0)

    if count1 == 0:
        check("Have initial count (skip if no data)", True)
        return

    result = _json_post("/api/collect/browser-chrome")
    task_id = result.get("task_id")

    if task_id:
        for _ in range(15):
            time.sleep(2)
            status = _json_get(f"/api/collect/browser-chrome/status?task_id={task_id}")
            if status.get("status") in ("completed", "error"):
                break

    stats2 = _json_get("/api/vault/stats")
    count2 = stats2.get("vaults", {}).get("Browser", {}).get("records", 0)
    check("Collection is idempotent (no duplicates)",
          count2 == count1, f"before={count1}, after={count2}")


def test_concurrent_collections():
    """Test that multiple collections don't interfere."""
    section("26. Concurrent Safety")

    r1 = _json_post("/api/collect/browser-chrome")
    r2 = _json_post("/api/collect/gmail")

    check("Both collections start", r1.get("task_id") and r2.get("task_id"))
    check("Different task IDs", r1.get("task_id") != r2.get("task_id"))

    if r1.get("task_id"):
        for _ in range(15):
            time.sleep(2)
            s = _json_get(f"/api/collect/browser-chrome/status?task_id={r1['task_id']}")
            if s.get("status") in ("completed", "error"):
                check("Browser completes under concurrency",
                      s.get("status") == "completed")
                break


def test_jargon_consistency():
    """Verify all pages have consistent jargon toggle support."""
    section("27. Jargon / Brand Consistency")

    pages = [
        ("/", "Dashboard"),
        ("/sources", "Life Map"),
        ("/records", "Loot Log"),
        ("/aliases", "Many Faces"),
        ("/automaton", "Automaton"),
        ("/settings", "Settings"),
        ("/achievements", "Achievements"),
        ("/welcome", "Welcome"),
    ]

    for path, name in pages:
        status, body = _get(path)
        if status != 200:
            check(f"{name}: page loads", False, f"got {status}")
            continue

        # Every page should have jargon toggle in base template
        check(f"{name}: has jargon toggle", 'id="jargon-toggle"' in body)
        # Every page should have data-rpg attributes
        check(f"{name}: has data-rpg attributes", 'data-rpg=' in body)
        # Every page should have data-real attributes
        check(f"{name}: has data-real attributes", 'data-real=' in body)


def test_accessibility():
    """Basic accessibility checks across pages."""
    section("28. Accessibility")

    status, body = _get("/")
    check("Has lang attribute", 'lang="en"' in body)
    check("Has viewport meta", 'name="viewport"' in body)
    check("Has charset meta", 'charset=' in body.lower())

    # Sources page accessibility
    status, body = _get("/sources")
    if status == 200:
        check("Sources: territory rows have role=button", 'role="button"' in body)
        check("Sources: territory rows have tabindex", 'tabindex="0"' in body)
        check("Sources: territory rows have aria-label", 'aria-label=' in body)


# ===========================================================================
# Runner
# ===========================================================================

def main():
    global _passed, _failed

    print("=" * 60)
    print("  NOMOLO Web Journey — End-to-End Tests")
    print(f"  Target: {BASE_URL}")
    print("=" * 60)

    # Check server is up
    try:
        status, _ = _get("/")
        if status != 200:
            print(f"\n  ERROR: Server returned {status}. Is it running?")
            print(f"  Start with: python nomolo.py web --port 3000 --no-open")
            sys.exit(1)
    except Exception as e:
        print(f"\n  ERROR: Cannot reach server at {BASE_URL}")
        print(f"  {e}")
        print(f"  Start with: python nomolo.py web --port 3000 --no-open")
        sys.exit(1)

    # Run all test suites
    test_server_health()
    test_base_template()
    test_intro_cinematic()
    test_welcome_page()
    test_dashboard_page()
    test_sources_page()
    test_records_page()
    test_aliases_page()
    test_automaton_page()
    test_achievements_page()
    test_settings_page()
    test_api_rpg()
    test_api_records()
    test_api_aliases()
    test_api_chat()
    test_api_settings()
    test_api_game_endpoints()
    test_api_vault_stats()
    test_chrome_analysis_api()
    test_browser_collection()
    test_google_sources()
    test_file_sources()
    test_static_assets_integrity()
    test_easter_egg_routes()
    test_idempotent_collection()
    test_concurrent_collections()
    test_jargon_consistency()
    test_accessibility()

    # Summary
    total = _passed + _failed
    print(f"\n{'=' * 60}")
    if _failed == 0:
        print(f"  ALL {total} TESTS PASSED")
    else:
        print(f"  {_passed}/{total} passed, {_failed} FAILED")
        print(f"\n  Failed tests:")
        for err in _errors:
            print(f"    - {err}")
    print(f"{'=' * 60}\n")

    sys.exit(0 if _failed == 0 else 1)


if __name__ == "__main__":
    main()
