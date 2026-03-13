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
        "message": f"Starting {source} collection...",
        "records": 0,
    }
    _collection_tasks[task_id] = task

    try:
        if source == "browser-chrome":
            task["message"] = "Copying Chrome history database..."
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
                    "Google API credentials not found. "
                    "You need credentials.json from Google Cloud Console."
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
                task["message"] = f"Need to sign in with Google to access {source}."
                task["auth_url"] = f"/api/auth/google?source={source}"
                return

            task["message"] = f"Connecting to {source} API..."
            task["progress"] = 10
            result = await asyncio.get_event_loop().run_in_executor(
                None, _collect_google_source, source, config
            )
            task.update(result)

        else:
            # File-based sources — tell user what to do
            task["status"] = "needs_file"
            task["progress"] = 0
            task["message"] = f"{source} requires a file export."
            task["instructions"] = _get_file_instructions(source)
            return

    except Exception as e:
        logger.exception("Collection failed for %s", source)
        task["status"] = "error"
        task["message"] = f"Collection failed: {e}"


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
            "message": f"Chrome history collected! {record_count:,} URLs saved.",
            "records": record_count,
        }
    except Exception as e:
        return {
            "status": "error",
            "progress": 0,
            "message": f"Chrome collection failed: {e}",
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
            vault_name = "Google_Contacts"
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
            "message": f"{source} collected! {record_count:,} records saved.",
            "records": record_count,
        }
    except Exception as e:
        return {
            "status": "error",
            "progress": 0,
            "message": f"{source} collection failed: {e}",
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
    logger.info("Nomolo web server starting...")
    yield
    logger.info("Nomolo web server shutting down.")


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
            return templates.TemplateResponse("dashboard.html", {
                "request": request,
                "progress": score,
                "sources": score.get("sources", []),
                "score": score,
                "achievements": dashboard["achievements"],
                "quests": [_normalize_quest(q) for q in dashboard["quests"]],
                "xp": dashboard.get("xp", {}),
            })
        except Exception as e:
            logger.warning("Dashboard load failed, falling back to welcome: %s", e)
            return templates.TemplateResponse("welcome.html", {
                "request": request,
            })
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
        logger.warning("Scan page failed: %s", e)
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
                    "data": {"message": "Scanning your digital life..."},
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
        logger.warning("Quest board failed: %s", e)
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
        logger.warning("Achievements page failed: %s", e)
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
        logger.warning("Fun facts generation failed: %s", e)
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
        logger.warning("Progress API failed: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


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
        logger.warning("Timeline page failed: %s", e)
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
    print("  +======================================+")
    print("  |          NOMOLO Web Interface         |")
    print("  |     Personal Data Archeology Tool     |")
    print("  +======================================+")
    print(f"  |  Running at: {url:<23} |")
    print("  |  LOCAL ONLY - data never leaves       |")
    print("  +======================================+")
    print()

    # Auto-open browser after a short delay
    import threading
    threading.Timer(1.5, lambda: webbrowser.open(url)).start()

    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
