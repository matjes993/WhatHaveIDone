"""
NOMOLO Web Server — Local-first gamified personal data archeology UI.

Serves the Nomolo web interface on localhost:3000.
All data stays local. No external requests. No tracking.

Usage:
    python -m web.server
    # or from project root:
    python web/server.py
"""

import asyncio
import json
import logging
import os
import random
import re
import subprocess
import sys
import webbrowser
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4

import uvicorn
import yaml
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

WEB_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(WEB_DIR, "templates")
STATIC_DIR = os.path.join(WEB_DIR, "static")

logger = logging.getLogger("nomolo.web")

# ---------------------------------------------------------------------------
# Import scanner and game modules
# ---------------------------------------------------------------------------

from web.scanner import scan as scanner_scan, get_life_score
from web.game import (
    evaluate_achievements,
    generate_fun_facts,
    get_game_dashboard,
    get_progress as game_get_progress,
    get_quests,
    record_activity,
)
from web.rpg import (
    get_rpg_dashboard, get_demo_character,
    POWER_UPS, load_earned_powerups, save_earned_powerup,
    get_all_powerups, check_easter_eggs,
    get_level_dialogue, get_memory_state, MEMORY_DIALOGUE,
)
from web.dialogues import (
    get_dialogue as dialogues_get_dialogue,
    get_random_quip,
    get_insult_fight,
    get_encounter,
    list_characters as dialogues_list_characters,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def load_config() -> dict:
    """Load config.yaml from the project root."""
    config_path = os.path.join(PROJECT_ROOT, "config.yaml")
    if not os.path.exists(config_path):
        return {}
    try:
        with open(config_path, "r") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def get_vault_root(config: dict) -> str:
    """Resolve the vault root directory."""
    vault_root = config.get("vault_root", os.path.join(PROJECT_ROOT, "vaults"))
    vault_root = os.path.expanduser(vault_root)
    if not os.path.isabs(vault_root):
        vault_root = os.path.join(PROJECT_ROOT, vault_root)
    return vault_root


# ---------------------------------------------------------------------------
# Collection tasks (background)
# ---------------------------------------------------------------------------

# Track running collections
_collection_tasks: Dict[str, Dict] = {}

# Journey state persistence (survives Terminal restart for FDA flow)
_JOURNEY_STATE_PATH = os.path.join(PROJECT_ROOT, "web", "journey_state.json")


def _load_journey_state() -> dict:
    try:
        if os.path.exists(_JOURNEY_STATE_PATH):
            with open(_JOURNEY_STATE_PATH, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_journey_state(state: dict):
    try:
        with open(_JOURNEY_STATE_PATH, "w") as f:
            json.dump(state, f)
    except Exception:
        pass


async def run_collection(source: str, config: dict, vault_root: str, task_id: str):
    """Run a real collection in the background using existing collectors."""
    task = {
        "source": source,
        "status": "running",
        "started": datetime.now().isoformat(),
        "progress": 0,
        "message": f"Preparing to raid {source}...",
        "records": 0,
    }
    _collection_tasks[task_id] = task

    try:
        if source == "browser-chrome":
            task["message"] = "Boarding Chrome's ship and copying the logs..."
            task["progress"] = 10
            # Run the browser collector in a thread (it's synchronous)
            result = await asyncio.get_event_loop().run_in_executor(
                None, _collect_browser_chrome, config
            )
            task.update(result)

        elif source in ("gmail", "contacts-google", "calendar"):
            # Google API sources — check if credentials exist
            creds_file = os.path.join(PROJECT_ROOT, "credentials.json")
            if not os.path.exists(creds_file):
                task["status"] = "needs_setup"
                task["progress"] = 0
                task["message"] = (
                    "No letter of marque found! "
                    "Ye need credentials.json from the Omniscient Eye's Console."
                )
                task["setup_instructions"] = _get_google_setup_instructions(source)
                return

            # Check for existing token
            token_map = {
                "gmail": "token.json",
                "contacts-google": config.get("contacts", {}).get("token_file", "token_contacts.json"),
                "calendar": "token.json",
            }
            token_file = os.path.join(PROJECT_ROOT, token_map.get(source, "token.json"))

            if not os.path.exists(token_file):
                task["status"] = "needs_auth"
                task["progress"] = 0
                task["message"] = f"Need to board the Omniscient Eye to raid {source}."
                task["auth_url"] = f"/api/auth/google?source={source}"
                return

            task["message"] = f"Boarding {source}'s ship..."
            task["progress"] = 10
            result = await asyncio.get_event_loop().run_in_executor(
                None, _collect_google_source, source, config
            )
            task.update(result)

        else:
            # File-based sources — tell user what to do
            task["status"] = "needs_file"
            task["progress"] = 0
            task["message"] = f"{source} requires stolen cargo from the platform."
            task["instructions"] = _get_file_instructions(source)
            return

    except Exception as e:
        logger.exception("🏴‍☠️ Blimey! The raid on %s has sprung a leak", source)
        task["status"] = "error"
        task["message"] = f"Kraken attack! Raid failed: {e}"


def _collect_browser_chrome(config: dict) -> dict:
    """Run the Chrome browser collector synchronously. Returns status dict."""
    try:
        from collectors.browser import run_import
        run_import(config=config)

        # Count what was collected
        vault_root = get_vault_root(config)
        browser_vault = os.path.join(vault_root, "Browser")
        record_count = 0
        if os.path.exists(browser_vault):
            for fname in os.listdir(browser_vault):
                if fname.endswith(".jsonl"):
                    fpath = os.path.join(browser_vault, fname)
                    with open(fpath, "r") as f:
                        record_count += sum(1 for _ in f)

        return {
            "status": "completed",
            "progress": 100,
            "message": f"Chrome plundered! {record_count:,} URLs stashed in the vault.",
            "records": record_count,
        }
    except Exception as e:
        return {
            "status": "error",
            "progress": 0,
            "message": f"Kraken attack! Chrome raid failed: {e}",
        }


def _collect_google_source(source: str, config: dict) -> dict:
    """Run a Google API collector synchronously. Returns status dict."""
    try:
        if source == "gmail":
            from collectors.gmail_collector import run_export
            run_export(vault_name="Primary", config=config, full_scan=False)
            vault_name = "Gmail_Primary"
        elif source == "contacts-google":
            from collectors.google_contacts import run_export
            run_export(config=config)
            vault_name = "Contacts"
        elif source == "calendar":
            from collectors.calendar_collector import run_export
            run_export(config=config)
            vault_name = "Calendar"
        else:
            return {"status": "error", "message": f"Unknown Google source: {source}"}

        # Count records
        vault_root = get_vault_root(config)
        vault_path = os.path.join(vault_root, vault_name)
        record_count = _count_vault_records(vault_path)

        return {
            "status": "completed",
            "progress": 100,
            "message": f"{source} plundered! {record_count:,} pieces of loot stashed.",
            "records": record_count,
        }
    except Exception as e:
        return {
            "status": "error",
            "progress": 0,
            "message": f"Kraken attack! {source} raid failed: {e}",
        }


def _count_vault_records(vault_path: str) -> int:
    """Count total JSONL entries in a vault directory (recursive)."""
    count = 0
    if not os.path.exists(vault_path):
        return 0
    for root, dirs, files in os.walk(vault_path):
        for fname in files:
            if fname.endswith((".jsonl", ".jsonl.zst")):
                fpath = os.path.join(root, fname)
                if fname.endswith(".zst"):
                    try:
                        import zstandard
                        with open(fpath, "rb") as f:
                            dctx = zstandard.ZstdDecompressor()
                            with dctx.stream_reader(f) as reader:
                                import io
                                text_reader = io.TextIOWrapper(reader, encoding="utf-8")
                                count += sum(1 for _ in text_reader)
                    except ImportError:
                        pass  # Can't read zst without zstandard
                else:
                    try:
                        with open(fpath, "r") as f:
                            count += sum(1 for _ in f)
                    except OSError:
                        pass
    return count


def _get_google_setup_instructions(source: str) -> dict:
    """Return setup instructions for Google API sources."""
    return {
        "steps": [
            "Go to console.cloud.google.com",
            "Create a project (or select existing)",
            "Enable the API (Library > search for it)",
            "Go to APIs & Services > Credentials",
            "Create Credentials > OAuth Client ID > Desktop App",
            "Download the JSON and save as credentials.json in the Nomolo folder",
        ],
        "api_name": {
            "gmail": "Gmail API",
            "contacts-google": "People API",
            "calendar": "Google Calendar API",
        }.get(source, "Google API"),
    }


def _get_file_instructions(source: str) -> dict:
    """Return export instructions for file-based sources."""
    instructions = {
        "contacts-linkedin": {
            "platform": "LinkedIn",
            "steps": [
                "Go to linkedin.com > Settings > Data Privacy",
                "Get a copy of your data (select Connections)",
                "Download and unzip the archive",
                "Upload the Connections.csv file",
            ],
        },
        "youtube": {
            "platform": "YouTube",
            "steps": [
                "Go to takeout.google.com",
                "Select YouTube and YouTube Music",
                "Download and unzip",
                "Upload the watch-history.json file",
            ],
        },
        "music-spotify": {
            "platform": "Spotify",
            "steps": [
                "Go to spotify.com > Account > Privacy",
                "Request your data download",
                "Wait for email (can take days)",
                "Upload the streaming history JSON files",
            ],
        },
        "shopping-amazon": {
            "platform": "Amazon",
            "steps": [
                "Go to Amazon > Account > Order History",
                "Download order reports as CSV",
                "Upload the CSV file",
            ],
        },
        "finance-paypal": {
            "platform": "PayPal",
            "steps": [
                "Go to PayPal > Activity > Download",
                "Export as CSV",
                "Upload the CSV file",
            ],
        },
    }
    return instructions.get(source, {
        "platform": source,
        "steps": [f"Export your data from {source} and upload the file."],
    })


# ---------------------------------------------------------------------------
# WebSocket manager
# ---------------------------------------------------------------------------


class ConnectionManager:
    """Manage WebSocket connections."""

    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def send_json(self, websocket: WebSocket, data: dict):
        await websocket.send_json(data)

    async def broadcast(self, data: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(data)
            except Exception:
                pass


ws_manager = ConnectionManager()

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown logic."""
    logger.info("⚓ The SCUMM Bar is open for business! Hoist the colors!")
    yield
    logger.info("🏴‍☠️ The SCUMM Bar is closing. Lower the Jolly Roger.")


app = FastAPI(
    title="Nomolo",
    description="Your personal data archeology tool",
    lifespan=lifespan,
)

# CORS for local development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# Templates
templates = Jinja2Templates(directory=TEMPLATES_DIR)


# ---------------------------------------------------------------------------
# Helper: run the full scan and wrap results for templates
# ---------------------------------------------------------------------------


async def _run_scan() -> dict:
    """Run the scanner and return structured results."""
    config = load_config()
    vault_root = get_vault_root(config)

    scan_results = await scanner_scan(
        vault_root=vault_root,
        project_root=PROJECT_ROOT,
    )
    score = get_life_score(scan_results)

    return {
        "scan_results": scan_results,
        "score": score,
        "vault_root": vault_root,
    }


def _has_any_data(vault_root: str) -> bool:
    """Quick check: does the vaults directory have any collected data?"""
    if not os.path.isdir(vault_root):
        return False
    try:
        for entry in os.scandir(vault_root):
            if entry.is_dir() and not entry.name.startswith("."):
                # Check if directory has any files
                for sub in os.scandir(entry.path):
                    if sub.is_file() and (sub.name.endswith(".json") or sub.name.endswith(".jsonl") or sub.name.endswith(".zst")):
                        return True
    except OSError:
        pass
    return False


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


def _normalize_quest(q: dict) -> dict:
    """Bridge game engine quest dict to what dashboard.html template expects."""
    return {
        **q,
        "name": q.get("name", q.get("title", "Quest")),
        "reward_xp": q.get("reward_xp", q.get("xp_reward", 0)),
        "description": q.get("description", ""),
        "progress": 1.0 if q.get("completed") else 0.0,
        "steps": q.get("steps", []),
    }


def _normalize_score(progress: dict) -> dict:
    """Bridge the game engine's progress dict to what templates expect."""
    collected = progress.get("sources_collected", 0)
    available = progress.get("sources_available", 1) or 1
    pct = progress.get("completion_percentage", int(collected / available * 100))
    total_records = progress.get("total_records", 0)

    # Level tiers
    if pct >= 80:
        level = {"title": "Data Archeologist", "description": "You've unearthed most of your digital life"}
    elif pct >= 50:
        level = {"title": "Digital Explorer", "description": "Your archive is growing nicely"}
    elif pct >= 25:
        level = {"title": "Data Collector", "description": "You're on your way to a complete archive"}
    elif pct > 0:
        level = {"title": "Curious Beginner", "description": "Your journey has just begun"}
    else:
        level = {"title": "New Arrival", "description": "Start collecting to build your archive"}

    return {
        **progress,
        "percentage": pct,
        "sources_total": available,
        "sources_collected": collected,
        "score": total_records,
        "max_score": max(total_records, available * 1000),
        "level": level,
    }


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Landing page — welcome (new user) or dashboard (returning user)."""
    config = load_config()
    vault_root = get_vault_root(config)
    has_data = _has_any_data(vault_root)

    if has_data:
        try:
            dashboard = get_game_dashboard(vault_root)
            score = _normalize_score(dashboard["progress"])
            rpg = get_rpg_dashboard(vault_root)
            return templates.TemplateResponse("dashboard.html", {
                "request": request,
                "progress": score,
                "sources": score.get("sources", []),
                "score": score,
                "achievements": dashboard["achievements"],
                "quests": [_normalize_quest(q) for q in dashboard["quests"]],
                "xp": dashboard.get("xp", {}),
                "rpg": rpg,
            })
        except Exception as e:
            logger.warning("🏴‍☠️ Blimey! The dashboard has sprung a leak: %s", e)
            # Don't fall back to welcome for initialized users - show records instead
            from starlette.responses import RedirectResponse
            return RedirectResponse(url="/records", status_code=302)
    else:
        return templates.TemplateResponse("welcome.html", {
            "request": request,
        })


@app.get("/welcome", response_class=HTMLResponse)
async def welcome_page(request: Request):
    """Welcome/journey page (always accessible for testing or re-onboarding)."""
    return templates.TemplateResponse("welcome.html", {
        "request": request,
    })


@app.get("/intro", response_class=HTMLResponse)
async def intro(request: Request):
    """Monkey Island-style opening cinematic / intro sequence."""
    return templates.TemplateResponse("intro.html", {
        "request": request,
    })


@app.get("/scan", response_class=HTMLResponse)
async def scan_page(request: Request):
    """Trigger scan and show animated results page."""
    try:
        data = await _run_scan()
        return templates.TemplateResponse("welcome.html", {
            "request": request,
            "scan_results": data["scan_results"]["sources"],
            "score": data["score"],
            "auto_scan": True,
        })
    except Exception as e:
        logger.warning("🏴‍☠️ Blimey! The spyglass has sprung a leak: %s", e)
        return templates.TemplateResponse("welcome.html", {
            "request": request,
        })


@app.get("/api/chrome-analysis")
async def api_chrome_analysis():
    """Analyze Chrome history for the discovery step."""
    from web.chrome_analyzer import analyze_chrome_history, get_suggested_next_step
    result = analyze_chrome_history()
    if result.get("success"):
        config = load_config()
        vault_root = get_vault_root(config)
        # Determine which sources are already collected
        existing = []
        if os.path.isdir(vault_root):
            for entry in os.scandir(vault_root):
                if entry.is_dir() and not entry.name.startswith("."):
                    existing.append(entry.name)
        suggestion = get_suggested_next_step(result, existing_vaults=existing)
        result["suggestion"] = suggestion
    return JSONResponse(result)


@app.get("/api/journey-state")
async def api_get_journey_state():
    """Get saved journey state (for resume after Terminal restart)."""
    return JSONResponse(_load_journey_state())


@app.post("/api/journey-state")
async def api_save_journey_state(request: Request):
    """Save journey state checkpoint."""
    try:
        body = await request.json()
        state = _load_journey_state()
        state.update(body)
        state["saved_at"] = datetime.now().isoformat()
        _save_journey_state(state)
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


@app.delete("/api/journey-state")
async def api_clear_journey_state():
    """Clear journey state (journey complete)."""
    try:
        if os.path.exists(_JOURNEY_STATE_PATH):
            os.remove(_JOURNEY_STATE_PATH)
    except OSError:
        pass
    return JSONResponse({"ok": True})


@app.post("/api/credentials/upload")
async def api_credentials_upload(request: Request):
    """Upload Google OAuth credentials JSON file."""
    try:
        body = await request.body()
        if len(body) > 10240:
            return JSONResponse({"ok": False, "error": "File too large (max 10KB)"}, status_code=400)

        data = json.loads(body)
        # Validate structure — must have 'installed' or 'web' key with client_id
        client_config = data.get("installed") or data.get("web")
        if not client_config or "client_id" not in client_config:
            return JSONResponse({
                "ok": False,
                "error": "Invalid credentials file. Must be an OAuth client secrets JSON from Google Cloud Console."
            }, status_code=400)

        creds_path = os.path.join(PROJECT_ROOT, "credentials.json")
        fd = os.open(creds_path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump(data, f)

        return JSONResponse({"ok": True})
    except json.JSONDecodeError:
        return JSONResponse({"ok": False, "error": "Not valid JSON"}, status_code=400)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.get("/api/credentials/status")
async def api_credentials_status():
    """Check if Google credentials and tokens exist."""
    creds_exists = os.path.exists(os.path.join(PROJECT_ROOT, "credentials.json"))
    config = load_config()

    token_gmail = os.path.exists(os.path.join(PROJECT_ROOT, "token.json"))
    token_contacts = os.path.exists(os.path.join(
        PROJECT_ROOT,
        config.get("contacts", {}).get("token_file", "token_contacts.json")
    ))
    # Calendar shares token.json with Gmail
    token_calendar = token_gmail

    # Get vault stats for already-collected data
    vault_root = get_vault_root(config)
    vault_counts = {}
    vault_map = {"gmail": "Gmail_Primary", "contacts-google": "Contacts", "calendar": "Calendar"}
    for source, vault_name in vault_map.items():
        vault_path = os.path.join(vault_root, vault_name)
        count = _count_vault_records(vault_path)
        if count > 0:
            vault_counts[source] = count

    return JSONResponse({
        "credentials": creds_exists,
        "tokens": {
            "gmail": token_gmail,
            "contacts-google": token_contacts,
            "calendar": token_calendar,
        },
        "vault_path": vault_root,
        "existing_records": vault_counts,
    })


@app.get("/api/local-scan")
async def api_local_scan():
    """Scan local macOS data sources in preview mode (no data collected)."""
    import asyncio
    from web.local_scanner import scan_local_mac

    result = await asyncio.get_event_loop().run_in_executor(None, scan_local_mac)
    return JSONResponse(result)


@app.post("/api/collect/local")
async def api_collect_local():
    """Collect data from local macOS sources into vaults."""
    from collectors.local_mac import collect_all_local

    config = load_config()
    vault_root = get_vault_root(config)

    # Run in background thread (blocking I/O)
    result = await asyncio.get_event_loop().run_in_executor(
        None, collect_all_local, vault_root
    )
    return JSONResponse(result)


@app.post("/api/collect/local/{source_id}")
async def api_collect_local_source(source_id: str):
    """Collect from a specific local source."""
    from collectors.local_mac import collect_all_local

    config = load_config()
    vault_root = get_vault_root(config)

    result = await asyncio.get_event_loop().run_in_executor(
        None, collect_all_local, vault_root, [source_id]
    )
    return JSONResponse(result)


@app.get("/api/identity-snapshot")
async def api_identity_snapshot():
    """Generate quick identity snapshot — the 'magic moment'."""
    from collectors.local_mac import generate_identity_snapshot
    from web.local_scanner import scan_local_mac

    config = load_config()
    vault_root = get_vault_root(config)

    # Get scan data + generate snapshot in parallel-ish (both blocking)
    loop = asyncio.get_event_loop()
    scan_data = await loop.run_in_executor(None, scan_local_mac)
    snapshot = await loop.run_in_executor(
        None, generate_identity_snapshot, vault_root, scan_data
    )
    return JSONResponse(snapshot)


@app.get("/api/gateway-summary")
async def api_gateway_summary():
    """Gateway summary — combines vault stats, fun facts, and achievements for Screen 5."""
    config = load_config()
    vault_root = get_vault_root(config)

    # Gather all data in parallel-ish
    loop = asyncio.get_event_loop()

    # Vault stats
    vaults = {}
    total_records = 0
    if os.path.exists(vault_root):
        for name in os.listdir(vault_root):
            vault_path = os.path.join(vault_root, name)
            if os.path.isdir(vault_path) and not name.startswith("."):
                count = _count_vault_records(vault_path)
                if count > 0:
                    vaults[name] = count
                    total_records += count

    # Sources connected
    sources_connected = len(vaults)

    # Years of history
    years_of_history = 0
    from collectors.local_mac import generate_identity_snapshot
    snapshot = await loop.run_in_executor(None, generate_identity_snapshot, vault_root, None)
    stats = snapshot.get("stats", {})
    years_of_history = stats.get("years_of_history", 0)

    # Fun fact
    try:
        questions = generate_fun_facts(vault_root)
        fun_fact = questions[0] if questions else None
    except Exception:
        fun_fact = None

    # Achievements
    try:
        achievements = evaluate_achievements(vault_root)
        unlocked = [a for a in achievements if a.get("unlocked")]
    except Exception:
        unlocked = []

    return JSONResponse({
        "total_records": total_records,
        "sources_connected": sources_connected,
        "years_of_history": years_of_history,
        "vaults": vaults,
        "fun_fact": fun_fact,
        "achievements": unlocked[:4],  # Show up to 4 badges
    })


@app.get("/api/scan")
async def api_scan():
    """REST endpoint: run scanner, return JSON."""
    data = await _run_scan()
    return JSONResponse({
        "sources": data["scan_results"]["sources"],
        "score": data["score"],
        "summary": data["scan_results"].get("summary", {}),
        "scanned_at": datetime.now().isoformat(),
    })


@app.websocket("/ws/scan")
async def ws_scan(websocket: WebSocket):
    """Stream scan progress events in real-time."""
    await ws_manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_json()
            if data.get("type") == "start_scan":
                config = load_config()
                vault_root = get_vault_root(config)

                # Send scanning started
                await ws_manager.send_json(websocket, {
                    "type": "scan_started",
                    "data": {"message": "Charting the seas for buried treasure..."},
                })

                # Run the real scanner with progress callback
                discovered_sources = []

                async def on_progress(category: str, done: int, total: int):
                    await ws_manager.send_json(websocket, {
                        "type": "scan_category",
                        "data": {
                            "category": category,
                            "done": done,
                            "total": total,
                            "progress": int(done / total * 100) if total > 0 else 0,
                        },
                    })

                scan_results = await scanner_scan(
                    vault_root=vault_root,
                    project_root=PROJECT_ROOT,
                    progress=on_progress,
                )

                # Now reveal sources one by one for the dopamine hit
                sources = scan_results.get("sources", [])
                total = len(sources)

                for i, source in enumerate(sources):
                    await asyncio.sleep(0.25)  # Dramatic pause
                    await ws_manager.send_json(websocket, {
                        "type": "source_discovered",
                        "data": {
                            "source": source if isinstance(source, dict) else source.to_dict(),
                            "index": i,
                            "total": total,
                            "progress": int((i + 1) / total * 100),
                        },
                    })

                # Send final score
                score = get_life_score(scan_results)
                summary = scan_results.get("summary", {})
                await ws_manager.send_json(websocket, {
                    "type": "scan_complete",
                    "data": {
                        "sources": [
                            s if isinstance(s, dict) else s.to_dict()
                            for s in sources
                        ],
                        "score": score,
                        "summary": summary,
                    },
                })

    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)


@app.get("/quest", response_class=HTMLResponse)
async def quest_board(request: Request):
    """Quest board page."""
    config = load_config()
    vault_root = get_vault_root(config)

    try:
        dashboard = get_game_dashboard(vault_root)
        score = _normalize_score(dashboard["progress"])
        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "page": "quests",
            "progress": score,
            "sources": score.get("sources", []),
            "score": score,
            "achievements": dashboard["achievements"],
            "quests": [_normalize_quest(q) for q in dashboard["quests"]],
        })
    except Exception as e:
        logger.warning("🏴‍☠️ Blimey! The quest board has sprung a leak: %s", e)
        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "page": "quests",
            "progress": {},
            "sources": [],
            "score": _normalize_score({}),
            "achievements": [],
            "quests": [],
        })


@app.get("/achievements", response_class=HTMLResponse)
async def achievements_page(request: Request):
    """Achievement showcase page."""
    config = load_config()
    vault_root = get_vault_root(config)

    try:
        dashboard = get_game_dashboard(vault_root)
        score = _normalize_score(dashboard["progress"])
        return templates.TemplateResponse("achievements.html", {
            "request": request,
            "progress": score,
            "achievements": dashboard["achievements"],
            "score": score,
        })
    except Exception as e:
        logger.warning("🏴‍☠️ Blimey! The trophy case has sprung a leak: %s", e)
        return templates.TemplateResponse("achievements.html", {
            "request": request,
            "progress": {},
            "achievements": [],
            "score": _normalize_score({}),
        })


@app.get("/api/fun-facts")
async def api_fun_facts():
    """Generate fun fact quiz questions."""
    config = load_config()
    vault_root = get_vault_root(config)

    try:
        questions = generate_fun_facts(vault_root)
        return JSONResponse({"questions": questions})
    except Exception as e:
        logger.warning("🏴‍☠️ Blimey! The fun facts cannon misfired: %s", e)
        return JSONResponse({"questions": [{
            "question": "How many data platforms does the average person use?",
            "options": ["5-8", "9-12", "13-17", "18+"],
            "correct": 2,
            "explanation": "Studies show the average person has accounts on 15+ platforms!",
        }]})


@app.get("/api/progress")
async def api_progress():
    """Get full progress data."""
    config = load_config()
    vault_root = get_vault_root(config)

    try:
        dashboard = get_game_dashboard(vault_root)
        return JSONResponse(dashboard)
    except Exception as e:
        logger.warning("🏴‍☠️ Blimey! The progress compass has sprung a leak: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/demo-character")
async def api_demo_character():
    """Demo RPG character for the marketing website."""
    return JSONResponse(get_demo_character())


@app.get("/api/rpg")
async def api_rpg_dashboard():
    """Get the user's RPG dashboard data."""
    config = load_config()
    vault_root = get_vault_root(config)
    rpg = get_rpg_dashboard(vault_root)
    return JSONResponse(rpg)


@app.post("/api/collect/{source}")
async def api_collect(source: str):
    """Trigger a collection (runs in background)."""
    config = load_config()
    vault_root = get_vault_root(config)
    task_id = str(uuid4())

    asyncio.create_task(run_collection(source, config, vault_root, task_id))

    return JSONResponse({
        "task_id": task_id,
        "source": source,
        "status": "started",
    })


@app.get("/api/collect/{source}/status")
async def api_collect_status(source: str, task_id: Optional[str] = None):
    """Check collection progress."""
    if task_id and task_id in _collection_tasks:
        return JSONResponse(_collection_tasks[task_id])

    # Find most recent task for this source
    for tid, task in reversed(list(_collection_tasks.items())):
        if task["source"] == source:
            return JSONResponse(task)

    return JSONResponse({"status": "not_started", "source": source})


@app.get("/api/auth/google")
async def api_google_auth(source: str = "gmail"):
    """Trigger Google OAuth flow — opens browser for sign-in."""
    config = load_config()
    creds_file = os.path.join(PROJECT_ROOT, "credentials.json")

    if not os.path.exists(creds_file):
        return JSONResponse({
            "success": False,
            "error": "no_credentials",
            "message": "credentials.json not found. Download it from Google Cloud Console.",
            "instructions": _get_google_setup_instructions(source),
        }, status_code=400)

    # Determine scopes based on source
    scope_map = {
        "gmail": config.get("gmail", {}).get(
            "scope", "https://www.googleapis.com/auth/gmail.readonly"
        ),
        "contacts-google": config.get("contacts", {}).get(
            "scope", "https://www.googleapis.com/auth/contacts.readonly"
        ),
        "calendar": "https://www.googleapis.com/auth/calendar.readonly",
    }
    token_map = {
        "gmail": "token.json",
        "contacts-google": config.get("contacts", {}).get("token_file", "token_contacts.json"),
        "calendar": "token.json",
    }

    scopes = [scope_map.get(source, scope_map["gmail"])]
    token_file = os.path.join(PROJECT_ROOT, token_map.get(source, "token.json"))

    try:
        # Run OAuth in a thread (it opens a browser and blocks)
        def do_auth():
            from core.auth import get_google_credentials
            return get_google_credentials(creds_file, token_file, scopes)

        creds = await asyncio.get_event_loop().run_in_executor(None, do_auth)

        return JSONResponse({
            "success": True,
            "message": f"Authenticated for {source}!",
            "source": source,
        })
    except FileNotFoundError as e:
        return JSONResponse({
            "success": False,
            "error": "no_credentials",
            "message": str(e),
        }, status_code=400)
    except Exception as e:
        return JSONResponse({
            "success": False,
            "error": "auth_failed",
            "message": f"Authentication failed: {e}",
        }, status_code=500)


@app.get("/api/vault/stats")
async def api_vault_stats():
    """Return stats about collected vaults."""
    config = load_config()
    vault_root = get_vault_root(config)

    vaults = {}
    if os.path.exists(vault_root):
        for name in os.listdir(vault_root):
            vault_path = os.path.join(vault_root, name)
            if os.path.isdir(vault_path) and not name.startswith("."):
                count = _count_vault_records(vault_path)
                if count > 0:
                    vaults[name] = {"records": count}

    return JSONResponse({
        "vaults": vaults,
        "total_records": sum(v["records"] for v in vaults.values()),
    })


@app.get("/timeline", response_class=HTMLResponse)
async def timeline_page(request: Request):
    """Visual timeline of data coverage."""
    config = load_config()
    vault_root = get_vault_root(config)

    try:
        dashboard = get_game_dashboard(vault_root)
        score = _normalize_score(dashboard["progress"])
        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "page": "timeline",
            "progress": score,
            "sources": score.get("sources", []),
            "score": score,
            "achievements": dashboard["achievements"],
            "quests": [_normalize_quest(q) for q in dashboard["quests"]],
        })
    except Exception as e:
        logger.warning("🏴‍☠️ Blimey! The timeline has sprung a leak: %s", e)
        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "page": "timeline",
            "progress": {},
            "sources": [],
            "score": _normalize_score({}),
            "achievements": [],
            "quests": [],
        })


# ---------------------------------------------------------------------------
# Records browser
# ---------------------------------------------------------------------------


@app.get("/sources", response_class=HTMLResponse)
async def sources_page(request: Request):
    """Sources management — see connected and available data sources."""
    config = load_config()
    vault_root = get_vault_root(config)

    # Collected sources with record counts and last-updated times
    collected = {}
    collected_times = {}
    if os.path.isdir(vault_root):
        for name in sorted(os.listdir(vault_root)):
            vault_path = os.path.join(vault_root, name)
            if os.path.isdir(vault_path) and not name.startswith("."):
                count = _count_vault_records(vault_path)
                if count > 0:
                    collected[name] = count
                    # Find most recent file modification time
                    latest_mtime = 0
                    for root, dirs, files in os.walk(vault_path):
                        for fname in files:
                            fpath = os.path.join(root, fname)
                            try:
                                mt = os.path.getmtime(fpath)
                                if mt > latest_mtime:
                                    latest_mtime = mt
                            except OSError:
                                pass
                    if latest_mtime > 0:
                        collected_times[name] = datetime.fromtimestamp(latest_mtime).isoformat()

    # All available sources with categories
    all_sources = [
        # Local Mac (automatic)
        {"id": "browser-chrome", "name": "Chrome History", "category": "local", "icon": "globe", "vault": "Browser"},
        {"id": "bookmarks", "name": "Bookmarks", "category": "local", "icon": "bookmark", "vault": "Bookmarks"},
        {"id": "contacts-mac", "name": "Contacts", "category": "local", "icon": "users", "vault": "Contacts"},
        {"id": "calendar-mac", "name": "Calendar", "category": "local", "icon": "calendar", "vault": "Calendar"},
        {"id": "imessage", "name": "iMessage", "category": "local", "icon": "message-circle", "vault": "Messages"},
        {"id": "notes", "name": "Notes", "category": "local", "icon": "edit-3", "vault": "Notes"},
        {"id": "safari", "name": "Safari History", "category": "local", "icon": "compass", "vault": "Safari"},
        {"id": "photos", "name": "Photos", "category": "local", "icon": "image", "vault": "Photos"},
        {"id": "mail", "name": "Mail", "category": "local", "icon": "mail", "vault": "Mail"},
        # Google API
        {"id": "gmail", "name": "Gmail", "category": "google", "icon": "mail", "vault": "Gmail_Primary"},
        {"id": "contacts-google", "name": "Google Contacts", "category": "google", "icon": "users", "vault": "Contacts_Google"},
        {"id": "calendar-google", "name": "Google Calendar", "category": "google", "icon": "calendar", "vault": "Calendar_Google"},
        # Import-based
        {"id": "whatsapp", "name": "WhatsApp", "category": "import", "icon": "message-circle", "vault": "WhatsApp"},
        {"id": "telegram", "name": "Telegram", "category": "import", "icon": "send", "vault": "Telegram"},
        {"id": "slack", "name": "Slack", "category": "import", "icon": "hash", "vault": "Slack"},
        {"id": "youtube", "name": "YouTube", "category": "import", "icon": "play-circle", "vault": "YouTube"},
        {"id": "music-spotify", "name": "Spotify", "category": "import", "icon": "music", "vault": "Spotify"},
        {"id": "contacts-linkedin", "name": "LinkedIn", "category": "import", "icon": "briefcase", "vault": "LinkedIn"},
        {"id": "twitter", "name": "Twitter / X", "category": "import", "icon": "at-sign", "vault": "Twitter"},
        {"id": "reddit", "name": "Reddit", "category": "import", "icon": "message-square", "vault": "Reddit"},
        {"id": "netflix", "name": "Netflix", "category": "import", "icon": "film", "vault": "Netflix"},
        {"id": "finance-paypal", "name": "PayPal", "category": "import", "icon": "dollar-sign", "vault": "PayPal"},
        {"id": "shopping-amazon", "name": "Amazon", "category": "import", "icon": "shopping-cart", "vault": "Amazon"},
        {"id": "health", "name": "Apple Health", "category": "import", "icon": "heart", "vault": "Health"},
    ]

    # Mark which are collected
    for source in all_sources:
        vault_name = source["vault"]
        source["collected"] = vault_name in collected
        source["records"] = collected.get(vault_name, 0)
        source["last_updated"] = collected_times.get(vault_name, "")

    return templates.TemplateResponse("sources.html", {
        "request": request,
        "page": "sources",
        "sources": all_sources,
        "collected_count": len(collected),
        "total_sources": len(all_sources),
        "vault_root": vault_root,
    })


@app.get("/records", response_class=HTMLResponse)
async def records_page(request: Request):
    """Records browser — search and browse all collected data."""
    config = load_config()
    vault_root = get_vault_root(config)

    # Get vault stats for sidebar counts
    vaults = {}
    total_records = 0
    if os.path.exists(vault_root):
        for name in os.listdir(vault_root):
            vault_path = os.path.join(vault_root, name)
            if os.path.isdir(vault_path) and not name.startswith("."):
                count = _count_vault_records(vault_path)
                if count > 0:
                    vaults[name] = count
                    total_records += count

    return templates.TemplateResponse("records.html", {
        "request": request,
        "page": "records",
        "vaults": vaults,
        "total_records": total_records,
    })


@app.get("/api/records")
async def api_records(
    source: Optional[str] = None,
    q: Optional[str] = None,
    page: int = 1,
    per_page: int = 50,
    sort: str = "newest",
):
    """Browse and search records across all vaults."""
    config = load_config()
    vault_root = get_vault_root(config)

    if q and q.strip():
        # Use the search engine for queries
        return await _search_records(q.strip(), vault_root, config, source, page, per_page, sort)

    # No search query — browse by source
    results = []
    total = 0

    # Determine which vaults to read
    vault_dirs = []
    if source:
        vault_path = os.path.join(vault_root, source)
        if os.path.isdir(vault_path):
            vault_dirs = [(source, vault_path)]
    else:
        if os.path.isdir(vault_root):
            for name in sorted(os.listdir(vault_root)):
                vault_path = os.path.join(vault_root, name)
                if os.path.isdir(vault_path) and not name.startswith("."):
                    vault_dirs.append((name, vault_path))

    # Read entries with pagination
    from core.vault import read_all_entries
    all_entries = []
    for vault_name, vault_path in vault_dirs:
        for entry in read_all_entries(vault_path):
            entry["_source"] = vault_name
            all_entries.append(entry)

    total = len(all_entries)

    # Sort
    def get_date_key(e):
        for k in ("date", "timestamp", "created_at", "sent_at", "time"):
            v = e.get(k)
            if v:
                return str(v)
        return ""

    if sort == "newest":
        all_entries.sort(key=get_date_key, reverse=True)
    elif sort == "oldest":
        all_entries.sort(key=get_date_key)

    # Paginate
    start = (page - 1) * per_page
    end = start + per_page
    page_entries = all_entries[start:end]

    # Format for frontend
    results = [_format_record(e) for e in page_entries]

    return JSONResponse({
        "records": results,
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page,
    })


async def _search_records(query, vault_root, config, source, page, per_page, sort):
    """Search records using the hybrid search engine."""
    try:
        from core.search_engine import hybrid_search
        from core.vectordb import get_client

        chroma_client = get_client(vault_root)
        collections = None
        if source:
            # Map vault name to collection name
            col = source.lower().replace(" ", "_")
            col = "".join(c if c.isalnum() or c == "_" else "_" for c in col)
            if len(col) < 3:
                col = col + "___"
            collections = [col]

        sort_map = {"newest": "date_desc", "oldest": "date_asc", "relevance": "relevance"}
        sort_by = sort_map.get(sort, "relevance")

        results = hybrid_search(
            query=query,
            vault_root=vault_root,
            chroma_client=chroma_client,
            config=config,
            n_results=per_page * page,  # Fetch enough for pagination
            collections=collections,
            sort_by=sort_by,
        )

        total = len(results)
        start = (page - 1) * per_page
        page_results = results[start:start + per_page]

        formatted = []
        for r in page_results:
            meta = r.get("metadata", {})
            source_name = r.get("collection", r.get("source", ""))
            title = meta.get("subject") or meta.get("title") or meta.get("name") or ""
            from_val = meta.get("from") or ""
            date_val = meta.get("date") or ""
            preview = (r.get("snippet", "") or "")[:200]
            # Strip HTML from preview
            if preview:
                preview = re.sub(r'<[^>]+>', '', preview)
            formatted.append({
                "id": r.get("entry_id", ""),
                "source": source_name,
                "title": title[:120],
                "from": from_val[:80],
                "date": date_val,
                "preview": preview,
                "score": round(r.get("combined_score", 0), 3),
            })

        return JSONResponse({
            "records": formatted,
            "total": total,
            "page": page,
            "per_page": per_page,
            "pages": (total + per_page - 1) // per_page,
            "query": query,
        })
    except Exception as e:
        logger.warning("🏴‍☠️ Blimey! The search spyglass cracked: %s — falling back to browse", e)
        return JSONResponse({
            "records": [],
            "total": 0,
            "page": 1,
            "per_page": per_page,
            "pages": 0,
            "query": query,
            "error": str(e),
        })


def _format_record(entry):
    """Format a vault entry for the records API response.

    Handles source-specific fields for Contacts, Gmail, Browser, Calendar,
    Bookmarks, and Photos. Falls back to generic extraction for unknown sources.
    """
    source = entry.pop("_source", "")
    source_lower = source.lower()
    entry_type = entry.get("type", "")

    title = ""
    from_field = ""
    date = ""
    preview = ""

    # --- Contacts ---
    if "contact" in source_lower or entry_type == "contact":
        title = entry.get("name", "") or "Unknown Contact"
        from_field = entry.get("organization") or entry.get("job_title") or ""
        date = str(entry.get("updated_at") or entry.get("created_at") or "")
        # Preview: show phone/email if available, otherwise organization
        parts = []
        if entry.get("job_title"):
            parts.append(entry["job_title"])
        if entry.get("organization"):
            parts.append(entry["organization"])
        preview = " — ".join(parts) if parts else ""

    # --- Gmail ---
    elif "gmail" in source_lower:
        title = entry.get("subject", "") or "No Subject"
        from_field = entry.get("from") or ""
        date = str(entry.get("date") or "")
        body = entry.get("body_raw") or ""
        if body:
            preview = re.sub(r'<[^>]+>', '', body)[:200].strip()

    # --- Browser ---
    elif "browser" in source_lower:
        title = entry.get("title", "") or "Untitled Page"
        from_field = entry.get("domain") or ""
        date = str(entry.get("last_visit") or "")
        preview = entry.get("url") or ""

    # --- Calendar ---
    elif "calendar" in source_lower or entry_type == "event":
        title = entry.get("title", "") or "Untitled Event"
        from_field = entry.get("calendar") or ""
        date = str(entry.get("start_date") or "")
        desc = entry.get("description") or ""
        if desc:
            preview = desc[:200]
        else:
            # Build time range preview
            start = entry.get("start_date") or ""
            end = entry.get("end_date") or ""
            if start and end:
                preview = f"{start} — {end}"

    # --- Bookmarks ---
    elif "bookmark" in source_lower or entry_type == "bookmark":
        title = entry.get("name", "") or "Untitled Bookmark"
        from_field = entry.get("folder") or ""
        date = str(entry.get("date_added") or "")
        preview = entry.get("url") or ""

    # --- Photos ---
    elif "photo" in source_lower or entry_type == "photo":
        photo_date = str(entry.get("date") or "")
        title = entry.get("filename") or (f"Photo from {photo_date}" if photo_date else "Photo")
        from_field = ""
        date = photo_date
        # Build dimensions/duration preview
        parts = []
        w = entry.get("width")
        h = entry.get("height")
        if w and h:
            parts.append(f"{w}x{h}")
        dur = entry.get("duration")
        if dur:
            parts.append(f"{dur}s")
        if entry.get("has_location"):
            parts.append("has location")
        preview = " · ".join(parts)

    # --- Fallback for unknown sources ---
    else:
        title = (
            entry.get("subject")
            or entry.get("title")
            or entry.get("name")
            or entry.get("url", "")[:80]
            or "Untitled"
        )
        from_field = entry.get("from") or entry.get("sender") or entry.get("author") or ""
        for k in ("date", "timestamp", "created_at", "sent_at", "time"):
            if entry.get(k):
                date = str(entry[k])
                break
        for k in ("body_raw", "body", "content", "description"):
            v = entry.get(k)
            if v and isinstance(v, str) and len(v) > 10:
                preview = re.sub(r'<[^>]+>', '', v)[:200]
                break
        if not preview and entry.get("url"):
            preview = entry["url"]

    return {
        "id": entry.get("id", ""),
        "source": source,
        "title": (title or "Untitled")[:120],
        "from": (from_field or "")[:80],
        "date": date,
        "preview": preview,
    }


@app.post("/api/open-vault-folder")
async def api_open_vault_folder():
    """Open the vault folder in Finder."""
    config = load_config()
    vault_root = get_vault_root(config)

    if os.path.isdir(vault_root):
        subprocess.Popen(["open", vault_root])
        return JSONResponse({"ok": True, "path": vault_root})
    return JSONResponse({"ok": False, "error": "Vault directory not found"}, status_code=404)


@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    """Settings page."""
    config = load_config()
    vault_root = get_vault_root(config)

    # Count records and sources
    total_records = 0
    source_count = 0
    if os.path.isdir(vault_root):
        for name in os.listdir(vault_root):
            vault_path = os.path.join(vault_root, name)
            if os.path.isdir(vault_path) and not name.startswith("."):
                count = _count_vault_records(vault_path)
                if count > 0:
                    total_records += count
                    source_count += 1

    return templates.TemplateResponse("settings.html", {
        "request": request,
        "page": "settings",
        "vault_root": vault_root,
        "total_records": total_records,
        "source_count": source_count,
        "auto_scan": config.get("auto_scan", False),
    })


@app.post("/api/settings")
async def api_save_setting(request: Request):
    """Save a setting to config.yaml."""
    try:
        body = await request.json()
        config_path = os.path.join(PROJECT_ROOT, "config.yaml")
        config = load_config()
        config.update(body)
        with open(config_path, "w") as f:
            yaml.dump(config, f, default_flow_style=False)
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ---------------------------------------------------------------------------
# Power-Ups & Social Sharing APIs
# ---------------------------------------------------------------------------


@app.get("/api/share-card")
async def api_share_card():
    """Generate a JSON payload for a shareable character card."""
    config = load_config()
    vault_root = get_vault_root(config)
    rpg = get_rpg_dashboard(vault_root)

    villains_defeated = sum(1 for v in rpg["villains"] if v.get("raid_complete"))
    villains_raided = sum(1 for v in rpg["villains"] if v.get("raided"))

    return JSONResponse({
        "level": rpg["level"]["level"],
        "title": rpg["level"]["title"],
        "total_records": rpg["total_records"],
        "sources_connected": rpg["sources_connected"],
        "stats": rpg["stats"],
        "serotonin": rpg["serotonin"]["level"],
        "villains_defeated": villains_defeated,
        "villains_raided": villains_raided,
        "earned_powerups": len(rpg.get("earned_powerup_ids", [])),
        "share_text": {
            "twitter": (
                f"\U0001F3F4\u200D\u2620\uFE0F I'm a Level {rpg['level']['level']} "
                f"{rpg['level']['title']} in the Seven Seas of Data! "
                f"{rpg['total_records']:,} records reclaimed from "
                f"{villains_raided} Conglomerates. My data, my rules. "
                f"Join the crew: https://nomolo.app #DataPiracy #Nomolo"
            ),
            "linkedin": (
                f"Taking ownership of my digital footprint with Nomolo. "
                f"Level {rpg['level']['level']} \"{rpg['level']['title']}\" - "
                f"{rpg['total_records']:,} personal records reclaimed and stored locally. "
                f"Data sovereignty is the future. https://nomolo.app"
            ),
            "clipboard": (
                f"\U0001F3F4\u200D\u2620\uFE0F NOMOLO - Data Sovereignty Report\n"
                f"Level {rpg['level']['level']} | {rpg['level']['title']}\n"
                f"Records: {rpg['total_records']:,}\n"
                f"Sources: {rpg['sources_connected']}\n"
                f"STR {rpg['stats']['STR']} | WIS {rpg['stats']['WIS']} | "
                f"DEX {rpg['stats']['DEX']} | INT {rpg['stats']['INT']} | "
                f"CHA {rpg['stats']['CHA']} | END {rpg['stats']['END']}\n"
                f"Total Power: {rpg['stats']['total_power']}/600\n"
                f"https://nomolo.app"
            ),
        },
    })


@app.post("/api/claim-powerup")
async def api_claim_powerup(request: Request):
    """Claim a social power-up by ID."""
    try:
        body = await request.json()
        powerup_id = body.get("powerup_id")

        if not powerup_id or powerup_id not in POWER_UPS:
            return JSONResponse({"ok": False, "error": "Invalid power-up ID"}, status_code=400)

        powerup = POWER_UPS[powerup_id]

        config = load_config()
        vault_root = get_vault_root(config)
        earned = save_earned_powerup(vault_root, powerup_id)

        return JSONResponse({
            "ok": True,
            "powerup": {
                "id": powerup_id,
                "name": powerup["name"],
                "emoji": powerup["emoji"],
                "description": powerup["description"],
                "effect": powerup["effect"],
                "rarity": powerup["rarity"],
            },
            "total_earned": len(earned),
        })
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ---------------------------------------------------------------------------
# Memory Mini-Games (Data Quizzes from Vault)
# ---------------------------------------------------------------------------


def _read_random_vault_entries(vault_root, count=20):
    """Read random entries from vault JSONL files for quiz generation."""
    from core.vault import read_all_entries

    all_entries = []
    if not os.path.isdir(vault_root):
        return []

    for vault_dir_name in os.listdir(vault_root):
        vault_path = os.path.join(vault_root, vault_dir_name)
        if not os.path.isdir(vault_path) or vault_dir_name.startswith("."):
            continue
        try:
            entries = list(read_all_entries(vault_path))
            for entry in entries:
                entry["_source_dir"] = vault_dir_name
            all_entries.extend(entries)
        except Exception:
            continue

    if not all_entries:
        return []

    # Sample up to `count` random entries
    sample_size = min(count, len(all_entries))
    return random.sample(all_entries, sample_size)


def _generate_mini_game_question(entries):
    """Generate a quiz question from vault entries. Returns dict or None."""
    if len(entries) < 4:
        return None

    # Categorize entries by type
    email_entries = [e for e in entries if e.get("_source_dir", "").startswith("Gmail")
                     or e.get("_source_dir", "") == "Mail"]
    dated_entries = [e for e in entries if e.get("date")]
    all_with_source = [e for e in entries if e.get("from") or e.get("sender")]

    question_types = []
    if len(all_with_source) >= 4:
        question_types.append("who_sent_this")
    if len(dated_entries) >= 4:
        question_types.append("when_was_this")
        question_types.append("which_came_first")
    if len(entries) >= 4:
        question_types.append("how_many")

    if not question_types:
        return None

    qtype = random.choice(question_types)

    try:
        if qtype == "who_sent_this" and len(all_with_source) >= 4:
            return _q_who_sent_this(all_with_source)
        elif qtype == "when_was_this" and len(dated_entries) >= 4:
            return _q_when_was_this(dated_entries)
        elif qtype == "which_came_first" and len(dated_entries) >= 2:
            return _q_which_came_first(dated_entries)
        elif qtype == "how_many":
            return _q_how_many(entries)
    except Exception:
        pass

    return None


def _q_who_sent_this(entries):
    """'Who sent this?' — show subject, guess sender."""
    target = random.choice(entries)
    sender = target.get("from") or target.get("sender") or ""
    subject = target.get("subject") or target.get("title") or target.get("name") or ""

    if not sender or not subject:
        return None

    # Build distractors from other senders
    other_senders = list({
        e.get("from") or e.get("sender") or ""
        for e in entries
        if (e.get("from") or e.get("sender") or "") != sender
        and (e.get("from") or e.get("sender") or "")
    })

    if len(other_senders) < 3:
        return None

    distractors = random.sample(other_senders, 3)
    options = distractors + [sender]
    random.shuffle(options)
    correct = options.index(sender)

    return {
        "type": "who_sent_this",
        "question": f"Who sent you a message with the subject '{subject[:80]}'?",
        "options": options,
        "correct": correct,
        "flavor_correct": "Your memory is sharper than a cutlass! +5 WIS",
        "flavor_wrong": "Even the best pirates forget a face now and then.",
        "reward_stat": "WIS",
        "reward_amount": 5,
    }


def _q_when_was_this(entries):
    """'When was this?' — show a record, guess the year."""
    target = random.choice(entries)
    date_str = target.get("date", "")
    title = target.get("subject") or target.get("title") or target.get("name") or ""

    if not date_str or not title:
        return None

    try:
        year = int(date_str[:4])
    except (ValueError, IndexError):
        return None

    # Generate plausible wrong years
    wrong_years = list({y for y in [year - 2, year - 1, year + 1, year + 2] if 2000 <= y <= 2030} - {year})
    if len(wrong_years) < 3:
        wrong_years = [year - 3, year - 1, year + 1]

    distractors = random.sample(wrong_years, min(3, len(wrong_years)))
    while len(distractors) < 3:
        distractors.append(year + len(distractors) + 1)

    options = [str(y) for y in distractors] + [str(year)]
    random.shuffle(options)
    correct = options.index(str(year))

    return {
        "type": "when_was_this",
        "question": f"What year was '{title[:80]}' from?",
        "options": options,
        "correct": correct,
        "flavor_correct": "Time is but a river, and you remember every bend! +5 WIS",
        "flavor_wrong": "The years blur together when you've sailed as many seas as you have.",
        "reward_stat": "WIS",
        "reward_amount": 5,
    }


def _q_which_came_first(entries):
    """'Which came first?' — show 2 records, pick the earlier one."""
    pair = random.sample(entries, 2)
    dates = []
    for e in pair:
        try:
            dates.append(e["date"][:10])
        except (KeyError, TypeError):
            return None

    if dates[0] == dates[1]:
        return None

    titles = [
        e.get("subject") or e.get("title") or e.get("name") or "Unknown"
        for e in pair
    ]

    first_idx = 0 if dates[0] < dates[1] else 1

    options = [f"'{titles[0][:60]}'", f"'{titles[1][:60]}'"]
    correct = first_idx

    return {
        "type": "which_came_first",
        "question": "Which of these happened first?",
        "options": options,
        "correct": correct,
        "flavor_correct": "A true archaeologist knows the layers! +5 INT",
        "flavor_wrong": "The sands of time are tricky. Don't feel bad.",
        "reward_stat": "INT",
        "reward_amount": 5,
    }


def _q_how_many(entries):
    """'How many?' — count records from a source."""
    sources = {}
    for e in entries:
        src = e.get("_source_dir", "Unknown")
        sources[src] = sources.get(src, 0) + 1

    if not sources:
        return None

    source = random.choice(list(sources.keys()))
    real_count = sources[source]

    # Generate plausible wrong answers
    wrong = list({
        max(0, real_count + offset)
        for offset in [-5, -3, -1, 2, 4, 7]
    } - {real_count})

    if len(wrong) < 3:
        wrong = [real_count + 1, real_count + 3, max(0, real_count - 2)]

    distractors = random.sample(wrong, min(3, len(wrong)))
    while len(distractors) < 3:
        distractors.append(real_count + len(distractors) + 5)

    options = [str(d) for d in distractors] + [str(real_count)]
    random.shuffle(options)
    correct = options.index(str(real_count))

    return {
        "type": "how_many",
        "question": f"How many records do you have from {source.replace('_', ' ')}?",
        "options": options,
        "correct": correct,
        "flavor_correct": "You know your vault like the back of your hook hand! +5 INT",
        "flavor_wrong": "A pirate should always know the size of their treasure hoard.",
        "reward_stat": "INT",
        "reward_amount": 5,
    }


@app.get("/api/mini-game")
async def api_mini_game():
    """Generate a random quiz question from vault data."""
    config = load_config()
    vault_root = get_vault_root(config)

    if not os.path.isdir(vault_root):
        return JSONResponse({
            "error": "not_enough_data",
            "message": "Your vault is empty. Raid some Conglomerates first!",
        })

    try:
        entries = _read_random_vault_entries(vault_root, count=30)
    except Exception as e:
        logger.warning("🏴‍☠️ Blimey! The Memory Tavern barrel is leaking: %s", e)
        entries = []

    if len(entries) < 4:
        return JSONResponse({
            "error": "not_enough_data",
            "message": "Not enough records for a quiz yet. Collect more data to unlock Memory Tavern!",
        })

    # Try up to 3 times to generate a valid question
    for _ in range(3):
        question = _generate_mini_game_question(entries)
        if question:
            return JSONResponse(question)

    return JSONResponse({
        "error": "generation_failed",
        "message": "Couldn't generate a question this time. Try again!",
    })


# ---------------------------------------------------------------------------
# Easter Eggs
# ---------------------------------------------------------------------------


@app.get("/grog", response_class=HTMLResponse)
async def grog_page():
    """Hidden grog recipe page — a Monkey Island classic."""
    return HTMLResponse("""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>GROG Recipe</title>
    <style>
        body {
            margin: 0; padding: 0;
            background: #1a0e00;
            display: flex; justify-content: center; align-items: center;
            min-height: 100vh;
            font-family: 'Georgia', serif;
        }
        .parchment {
            max-width: 500px; margin: 40px; padding: 40px 50px;
            background: linear-gradient(135deg, #d4a762, #c49a5a, #b8894e, #d4a762);
            border-radius: 4px;
            box-shadow: 0 0 40px rgba(0,0,0,0.6), inset 0 0 30px rgba(139, 90, 43, 0.3);
            position: relative;
            transform: rotate(-1deg);
            color: #3d2b1f;
        }
        .parchment::before {
            content: '';
            position: absolute; top: -5px; left: -5px; right: -5px; bottom: -5px;
            border: 2px dashed rgba(61,43,31,0.3);
            border-radius: 6px;
        }
        .parchment h1 {
            font-size: 28px; text-align: center; margin: 0 0 20px;
            font-family: 'Press Start 2P', 'Georgia', serif;
            text-shadow: 1px 1px 0 rgba(139,90,43,0.3);
        }
        .parchment .skull { text-align: center; font-size: 40px; margin-bottom: 10px; }
        .parchment p { font-size: 16px; line-height: 1.7; margin: 0 0 15px; }
        .parchment .recipe { font-style: italic; font-size: 15px; }
        .parchment .warning {
            margin-top: 20px; padding: 10px;
            border: 1px solid rgba(61,43,31,0.4);
            font-size: 13px; text-align: center;
            font-weight: bold;
        }
        .parchment .back {
            display: block; text-align: center; margin-top: 20px;
            color: #3d2b1f; font-size: 14px;
        }
        .burn-marks {
            position: absolute; top: 10px; right: 10px;
            width: 30px; height: 30px;
            background: radial-gradient(circle, rgba(80,40,0,0.3) 0%, transparent 70%);
            border-radius: 50%;
        }
    </style>
</head>
<body>
    <div class="parchment">
        <div class="burn-marks"></div>
        <div class="skull">☠️</div>
        <h1>GROG RECIPE</h1>
        <p class="recipe">
            Mix one part <strong>kerosene</strong>,
            one part <strong>acetone</strong>,
            one part <strong>battery acid</strong>,
            one part <strong>red dye #2</strong>,
            one part <strong>scumm</strong>,
            one part <strong>axle grease</strong>,
            and one part <strong>pepperoni</strong>.
        </p>
        <div class="warning">
            ⚠️ WARNING: Dissolves mugs. Also effective at removing barnacles,
            stripping paint, and voiding warranties.
        </div>
        <a class="back" href="/">← Return to the SCUMM Bar</a>
    </div>
</body>
</html>
""")


@app.get("/api/rubber-chicken")
async def api_rubber_chicken():
    """The most important item in any adventure game."""
    return JSONResponse({
        "item": "rubber chicken with a pulley in the middle",
        "use": "unknown but probably important",
        "tradeable": False,
        "origin": "Monkey Island",
        "hint": "Try using it with the cable across the chasm.",
    })


# ---------------------------------------------------------------------------
# Dialogue API — Monkey Island-style conversations
# ---------------------------------------------------------------------------

@app.get("/api/memory-dialogue")
async def api_memory_dialogue(context: str = "greeting"):
    """Return level-appropriate dialogue based on the user's current memory state.

    Query params:
        context — "greeting", "error", "empty_vault", "loading", "celebration".

    The memory state is computed from the ACTUAL vault data / level.
    """
    config = load_config()
    vault_root = get_vault_root(config)
    rpg = get_rpg_dashboard(vault_root)
    level = rpg["level"]["level"]
    dialogue = get_level_dialogue(level, context)
    return JSONResponse(dialogue)


@app.get("/api/dialogue/characters")
async def api_dialogue_characters():
    """List all characters with available dialogue."""
    return JSONResponse(dialogues_list_characters())


@app.get("/api/dialogue/{character}")
async def api_dialogue(character: str, context: str = "random"):
    """Return dialogue for a character.

    Query params:
        context — "random" (crew quip), "insult" (fight tree),
                  "encounter" (multi-step), "all" (everything).
    """
    result = dialogues_get_dialogue(character, context)
    return JSONResponse(result)


@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Handle HTTP exceptions — pirate-themed 404 for missing pages."""
    if exc.status_code != 404:
        # For non-404 errors, return standard JSON response
        return JSONResponse(
            content={"detail": exc.detail},
            status_code=exc.status_code,
        )
    return custom_404_handler(request, exc)


async def custom_404_handler(request: Request, exc):
    """Pirate-themed 404 page."""
    return HTMLResponse(
        content="""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>404 — Off the Edge of the Map</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        body {
            margin: 0; padding: 0;
            background: #0a0a1a;
            color: #e0e0e0;
            font-family: 'Space Grotesk', sans-serif;
            display: flex; justify-content: center; align-items: center;
            min-height: 100vh;
            text-align: center;
        }
        .container { max-width: 500px; padding: 40px; }
        .map-emoji { font-size: 80px; margin-bottom: 20px; }
        h1 { font-size: 48px; margin: 0 0 10px; color: #ffd700; }
        .subtitle { font-size: 20px; margin-bottom: 30px; color: rgba(255,255,255,0.7); }
        .monsters { font-size: 16px; color: rgba(255,255,255,0.5); margin-bottom: 30px; }
        .back-link {
            display: inline-block; padding: 14px 28px;
            background: linear-gradient(135deg, #a855f7, #00d4ff);
            border-radius: 10px; color: white;
            text-decoration: none; font-weight: 600; font-size: 16px;
        }
        .back-link:hover { opacity: 0.9; }
    </style>
</head>
<body>
    <div class="container">
        <div class="map-emoji">🗺️</div>
        <h1>404</h1>
        <p class="subtitle">You've sailed off the edge of the map!</p>
        <p class="monsters">There be nothing here but sea monsters and broken links.</p>
        <a class="back-link" href="/">Return to the SCUMM Bar →</a>
    </div>
</body>
</html>
""",
        status_code=404,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    """Start the Nomolo web server."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    host = "127.0.0.1"
    port = 3000
    url = f"http://{host}:{port}"

    print()
    print("        ⛵                                  ")
    print("       __|__                                ")
    print("    .-'     '-.     🏴‍☠️ NOMOLO — The Data Pirate's Vessel")
    print("   /   ⚓   \\                              ")
    print("  |  SCUMM BAR  |   ⚓ Port: {:<14}".format(url))
    print("  |  ~~~~~~~~~~~  |   🔒 LOCAL ONLY — yer data stays aboard")
    print("   \\___________/                            ")
    print("  ~~~~~~~~~~~~~~~~~                         ")
    print()

    # Auto-open browser after a short delay
    import threading
    threading.Timer(1.5, lambda: webbrowser.open(url)).start()

    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
