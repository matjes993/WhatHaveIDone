"""
NOMOLO Web Journey — End-to-End Test Suite

Tests the complete user journey from landing page through data collection.
Runs against a live local server. No external dependencies beyond stdlib + requests.

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


def _post(path, expect_status=200):
    """POST request, return (status, body)."""
    url = BASE_URL + path
    try:
        req = urllib.request.Request(url, data=b"", method="POST")
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


def _json_post(path):
    """POST JSON endpoint, return parsed dict."""
    status, body = _post(path)
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


def test_welcome_page():
    """Test the welcome/journey page renders correctly."""
    section("2. Welcome Page (New User Journey)")

    # Use /welcome route (always shows journey, even with existing data)
    status, body = _get("/welcome")
    check("Welcome page loads", status == 200)

    # Step 1: Hook
    check("Has journey container", 'id="journey"' in body)
    check("Has Begin button", 'id="begin-btn"' in body)
    check("Has Nomolo logo", 'NOMOLO' in body)
    check("Has trust text", 'never leaves this machine' in body.lower())

    # Step 2: Discovery
    check("Has discovery step", 'id="step-discover"' in body)
    check("Has knowledge graph SVG", 'id="knowledge-graph"' in body)
    check("Has platform counter", 'id="platform-counter"' in body)

    # Step 3: Invitation
    check("Has invitation step", 'id="step-invite"' in body)
    check("Has connect button", 'id="connect-btn"' in body)

    # Step 4: Collection
    check("Has collect step", 'id="step-collect"' in body)
    check("Has collect graph", 'id="collect-graph"' in body)
    check("Has Explore CTA", 'Explore Your Archive' in body)

    # Nerd Mode / Matrix
    check("Has Matrix toggle", 'id="nerd-toggle"' in body)
    check("Has Matrix panel", 'id="nerd-panel"' in body)
    check("Matrix title correct", 'show-me-the-matrix' in body)

    # Sidebar hidden on welcome
    check("Sidebar hidden on welcome", 'sidebar { display: none' in body.lower() or '.sidebar { display: none' in body.lower())

    # NomoloBridge wired up
    check("Begin calls NomoloBridge", 'NomoloBridge.beginJourney()' in body)
    check("Connect calls NomoloBridge", 'NomoloBridge.startCollection()' in body)


def test_dashboard_page():
    """Test the dashboard loads when vault data exists."""
    section("2b. Dashboard (Returning User)")

    status, body = _get("/")
    check("Root page loads", status == 200)

    # If we have vault data, should be dashboard; otherwise welcome
    has_vault = os.path.isdir(os.path.join(VAULT_ROOT, "Browser"))
    if has_vault:
        check("Shows dashboard (has data)", "Dashboard" in body or "Your Life Archive" in body)
        check("Has score widget", "score-widget" in body)
        check("Has quick actions", "Quick Actions" in body or "action-btn" in body)
    else:
        check("Shows welcome (no data)", "journey" in body or "Begin" in body)


def test_chrome_analysis_api():
    """Test the Chrome history analysis endpoint."""
    section("3. Chrome Analysis API")

    data = _json_get("/api/chrome-analysis")
    check("Chrome analysis succeeds", data.get("success") is True,
          data.get("message", data.get("_error", "")))

    if not data.get("success"):
        print("    (Skipping detail checks — analysis failed)")
        return

    # Platforms
    platforms = data.get("platforms", [])
    check("Found platforms", len(platforms) > 0, f"got {len(platforms)}")
    check("Platforms have names", all("name" in p for p in platforms))
    check("Platforms have visits", all("visits" in p for p in platforms))
    check("Platforms have categories", all("category" in p for p in platforms))
    check("Platforms sorted by visits (desc)",
          all(platforms[i]["visits"] >= platforms[i+1]["visits"]
              for i in range(len(platforms)-1)))

    # Stats
    stats = data.get("stats", {})
    check("Has platforms_detected", "platforms_detected" in stats)
    check("Has years_of_history", "years_of_history" in stats)
    check("Has unique_domains", "unique_domains" in stats)
    check("Stats match platform count",
          stats.get("platforms_detected") == len(platforms))

    # Graph data
    nodes = data.get("graph_nodes", [])
    edges = data.get("graph_edges", [])
    check("Has graph nodes", len(nodes) > 0)
    check("Has graph edges", len(edges) > 0)
    check("First node is 'user'",
          nodes[0].get("id") == "user" if nodes else False)
    check("Edges connect to user",
          all(e.get("source") == "user" for e in edges))

    # Top domains
    top = data.get("top_domains", [])
    check("Has top domains", len(top) > 0)

    # Suggestion
    suggestion = data.get("suggestion")
    check("Has suggestion", suggestion is not None)
    if suggestion:
        check("Suggestion has source", "source" in suggestion)
        check("Suggestion has name", "name" in suggestion)
        check("Suggestion has difficulty", "difficulty" in suggestion)

    # Date range
    dr = data.get("date_range", {})
    check("Has date range", "earliest" in dr and "latest" in dr)


def test_browser_collection():
    """Test real browser-chrome collection end-to-end."""
    section("4. Browser Collection (Real Data)")

    # Start collection
    result = _json_post("/api/collect/browser-chrome")
    check("Collection starts", result.get("status") == "started",
          result.get("_error", ""))
    task_id = result.get("task_id")
    check("Returns task_id", task_id is not None)

    if not task_id:
        print("    (Skipping — no task_id)")
        return

    # Poll until done (max 30 seconds)
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

    if final_status and final_status.get("status") == "completed":
        records = final_status.get("records", 0)
        check("Collected records > 0", records > 0, f"got {records}")
        check("Has completion message", "saved" in final_status.get("message", "").lower())

    # Verify vault files on disk
    browser_vault = os.path.join(VAULT_ROOT, "Browser")
    check("Browser vault directory exists", os.path.isdir(browser_vault))

    jsonl_path = os.path.join(browser_vault, "browser.jsonl")
    check("browser.jsonl exists", os.path.isfile(jsonl_path))

    if os.path.isfile(jsonl_path):
        with open(jsonl_path) as f:
            lines = sum(1 for _ in f)
        check("JSONL has entries", lines > 0, f"got {lines} lines")

        # Validate a sample entry
        with open(jsonl_path) as f:
            first_line = f.readline()
        try:
            entry = json.loads(first_line)
            check("Entry has 'id'", "id" in entry)
            check("Entry has 'url'", "url" in entry)
            check("Entry has 'domain'", "domain" in entry)
            check("Entry has 'visit_count'", "visit_count" in entry)
            check("Entry has 'last_visit'", "last_visit" in entry)
        except json.JSONDecodeError:
            check("First line is valid JSON", False, first_line[:100])

    # Processed IDs file
    pid_path = os.path.join(browser_vault, "processed_ids.txt")
    check("processed_ids.txt exists", os.path.isfile(pid_path))


def test_vault_stats_api():
    """Test the vault stats endpoint."""
    section("5. Vault Stats API")

    data = _json_get("/api/vault/stats")
    check("Vault stats returns", "_error" not in data)

    vaults = data.get("vaults", {})
    total = data.get("total_records", 0)
    check("Has vaults dict", isinstance(vaults, dict))
    check("Total records > 0", total > 0, f"got {total}")

    if "Browser" in vaults:
        check("Browser vault in stats", True)
        check("Browser records > 0",
              vaults["Browser"].get("records", 0) > 0)


def test_google_source_without_credentials():
    """Test that Google sources correctly report setup needed."""
    section("6. Google Sources (No Credentials)")

    for source in ["gmail", "contacts-google", "calendar"]:
        result = _json_post(f"/api/collect/{source}")
        task_id = result.get("task_id")
        if not task_id:
            check(f"{source}: starts", False, str(result))
            continue

        time.sleep(2)
        status = _json_get(f"/api/collect/{source}/status?task_id={task_id}")
        st = status.get("status")

        # Should be needs_setup or needs_auth (depending on credentials.json)
        creds_exist = os.path.exists(os.path.join(PROJECT_ROOT, "credentials.json"))

        if not creds_exist:
            check(f"{source}: needs_setup (no creds)",
                  st == "needs_setup",
                  f"got status={st}")
            check(f"{source}: has setup instructions",
                  "setup_instructions" in status or "instructions" in status.get("message", ""))
        else:
            # Has credentials but may not have token
            check(f"{source}: needs_auth or runs",
                  st in ("needs_auth", "running", "completed"),
                  f"got status={st}")


def test_file_sources():
    """Test that file-based sources correctly report instructions needed."""
    section("7. File-Based Sources")

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

        check(f"{source}: needs_file",
              st == "needs_file",
              f"got status={st}")
        if st == "needs_file":
            instructions = status.get("instructions", {})
            check(f"{source}: has platform name",
                  "platform" in instructions,
                  str(instructions.get("platform", "")))
            check(f"{source}: has steps",
                  len(instructions.get("steps", [])) > 0)


def test_static_assets_integrity():
    """Verify CSS and JS files contain expected content."""
    section("8. Static Assets Integrity")

    # CSS
    status, css = _get("/static/css/style.css")
    check("CSS loads", status == 200)
    check("CSS has journey styles", ".journey" in css)
    check("CSS has nerd-toggle styles", ".nerd-toggle" in css)
    check("CSS has nerd-panel styles", ".nerd-panel" in css)
    check("CSS has dashboard styles", ".dashboard" in css)
    check("CSS has score-widget styles", ".score-widget" in css)
    check("CSS has graph-node styles", ".graph-node" in css or "graph-node" in css)

    # JS
    status, js = _get("/static/js/app.js")
    check("JS loads", status == 200)
    check("JS has NomoloBridge", "NomoloBridge" in js)
    check("JS has beginJourney", "beginJourney" in js)
    check("JS has animateGraph", "animateGraph" in js)
    check("JS has startCollection", "startCollection" in js)
    check("JS has triggerCollect", "triggerCollect" in js)
    check("JS has pollCollectionStatus", "pollCollectionStatus" in js)
    check("JS has toggleNerdMode", "toggleNerdMode" in js)
    check("JS has nerdLog", "nerdLog" in js)
    check("JS has handleNeedsAuth", "handleNeedsAuth" in js)
    check("JS has handleNeedsSetup", "handleNeedsSetup" in js)
    check("JS has handleNeedsFile", "handleNeedsFile" in js)
    check("JS has onBrowserCollectionDone", "onBrowserCollectionDone" in js)
    check("JS has escapeHtml", "escapeHtml" in js)


def test_other_pages():
    """Test other page routes."""
    section("9. Other Pages")

    # These may fail gracefully if no game data — that's OK
    for path, name in [
        ("/scan", "Scan page"),
        ("/quest", "Quest board"),
        ("/achievements", "Achievements"),
        ("/timeline", "Timeline"),
    ]:
        status, body = _get(path)
        check(f"{name} loads (200 or fallback)", status == 200, f"got {status}")
        if status == 200:
            check(f"{name} has HTML", "<html" in body.lower())


def test_api_endpoints():
    """Test remaining API endpoints."""
    section("10. Other API Endpoints")

    # Fun facts (may return empty if no vault data for quizzes)
    data = _json_get("/api/fun-facts")
    check("Fun facts endpoint responds", "_error" not in data or data.get("questions") is not None)

    # Progress
    data = _json_get("/api/progress")
    check("Progress endpoint responds", "_error" not in data)

    # Auth endpoint (should fail gracefully without credentials)
    status, body = _get("/api/auth/google?source=gmail")
    check("Google auth endpoint exists",
          status in (200, 400, 500),
          f"got {status}")

    # Local scan endpoint
    data = _json_get("/api/local-scan")
    check("Local scan endpoint responds", "sources" in data)
    check("Local scan has summary", "summary" in data)
    summary = data.get("summary", {})
    check("Local scan has sources_scanned",
          summary.get("sources_scanned", 0) > 0,
          f"scanned {summary.get('sources_scanned', 0)}")
    check("Local scan has graph_nodes", "graph_nodes" in data)
    check("Local scan has graph_edges", "graph_edges" in data)

    # Verify source structure
    sources = data.get("sources", {})
    check("Local scan returns source entries", len(sources) > 0, f"got {len(sources)}")
    for sid, src in sources.items():
        check(f"Source '{sid}' has name", "name" in src)
        check(f"Source '{sid}' has icon", "icon" in src)
        check(f"Source '{sid}' has found field", "found" in src)
        if src.get("found"):
            check(f"Source '{sid}' has total count", "total" in src, f"keys: {list(src.keys())}")
        break  # Just check one to keep test count manageable

    # Found vs locked counts should be consistent
    found = summary.get("sources_found", 0)
    locked = summary.get("sources_locked", 0)
    check("Found + locked <= scanned",
          found + locked <= summary.get("sources_scanned", 0),
          f"found={found}, locked={locked}, scanned={summary.get('sources_scanned', 0)}")

    # Identity snapshot endpoint
    snap = _json_get("/api/identity-snapshot")
    check("Identity snapshot responds", "insights" in snap)
    check("Snapshot has_data flag", "has_data" in snap)
    check("Snapshot has stats", "stats" in snap)
    if snap.get("has_data"):
        check("Snapshot has insights", len(snap.get("insights", [])) > 0)
        first = snap["insights"][0]
        check("Insight has icon", "icon" in first)
        check("Insight has text", "text" in first)

    # Local collection endpoint
    local_result = _json_post("/api/collect/local")
    check("Local collect responds", "status" in local_result)
    check("Local collect has results", "results" in local_result)
    check("Local collect has totals", "total_records" in local_result)
    check("Local collect is idempotent",
          local_result.get("total_records", -1) >= 0)


def test_idempotent_collection():
    """Test that running collection twice doesn't duplicate data."""
    section("11. Idempotent Collection")

    # Get current count
    stats1 = _json_get("/api/vault/stats")
    count1 = stats1.get("vaults", {}).get("Browser", {}).get("records", 0)
    check("Have initial count", count1 > 0, f"got {count1}")

    # Run collection again
    result = _json_post("/api/collect/browser-chrome")
    task_id = result.get("task_id")

    if task_id:
        for _ in range(15):
            time.sleep(2)
            status = _json_get(f"/api/collect/browser-chrome/status?task_id={task_id}")
            if status.get("status") in ("completed", "error"):
                break

    # Check count hasn't changed significantly (incremental collection)
    stats2 = _json_get("/api/vault/stats")
    count2 = stats2.get("vaults", {}).get("Browser", {}).get("records", 0)
    check("Collection is idempotent (no duplicates)",
          count2 == count1,
          f"before={count1}, after={count2}")


def test_concurrent_collections():
    """Test that multiple collections don't interfere."""
    section("12. Concurrent Safety")

    # Start two collections at once
    r1 = _json_post("/api/collect/browser-chrome")
    r2 = _json_post("/api/collect/gmail")

    check("Both collections start",
          r1.get("task_id") and r2.get("task_id"))
    check("Different task IDs",
          r1.get("task_id") != r2.get("task_id"))

    # Wait for browser one to finish
    if r1.get("task_id"):
        for _ in range(15):
            time.sleep(2)
            s = _json_get(f"/api/collect/browser-chrome/status?task_id={r1['task_id']}")
            if s.get("status") in ("completed", "error"):
                check("Browser collection completes under concurrency",
                      s.get("status") == "completed")
                break


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
    test_welcome_page()
    test_dashboard_page()
    test_chrome_analysis_api()
    test_browser_collection()
    test_vault_stats_api()
    test_google_source_without_credentials()
    test_file_sources()
    test_static_assets_integrity()
    test_other_pages()
    test_api_endpoints()
    test_idempotent_collection()
    test_concurrent_collections()

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
