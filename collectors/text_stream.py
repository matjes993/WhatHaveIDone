"""
NOMOLO Text Stream Collector
Captures user-typed text from the browser (via Chrome extension) and
archives it in the TextStream vault.

Runs a local HTTP server on localhost:19876 that accepts batched captures
from the Nomolo Text Capture Chrome extension.

The server ONLY binds to 127.0.0.1 — never exposed to the network.

Usage:
    nomolo collect text-stream start    Start the capture receiver
    nomolo collect text-stream stop     Stop the capture receiver

Vault structure:
    vaults/TextStream/YYYY/MM_Month.jsonl

Each entry:
    {
        "id": "<sha256 of timestamp+text>",
        "timestamp": "2026-03-12T14:30:00.000Z",
        "domain": "chat.openai.com",
        "text": "How do I implement a binary search tree in Rust?",
        "page_title": "ChatGPT",
        "field_type": "contenteditable",
        "year": 2026,
        "month": 3,
        "word_count": 11,
        "text_for_embedding": "[chat.openai.com] How do I implement a binary search tree in Rust?"
    }
"""

import hashlib
import json
import logging
import os
import signal
import sys
import threading
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler

from core.vault import flush_entries, load_processed_ids, append_processed_ids

logger = logging.getLogger("nomolo.text_stream")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HOST = "127.0.0.1"
PORT = 19876
PID_FILE_NAME = ".text_stream.pid"

# Month names for vault file naming
_MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


# ---------------------------------------------------------------------------
# Vault helpers
# ---------------------------------------------------------------------------

def _get_vault_path(config):
    """Return the TextStream vault path from config."""
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    default_vault = os.path.join(project_root, "vaults")
    vault_root = config.get("vault_root", default_vault)
    vault_root = os.path.expanduser(vault_root)
    if not os.path.isabs(vault_root):
        vault_root = os.path.join(project_root, vault_root)
    return os.path.join(vault_root, "TextStream")


def _get_pid_file_path():
    """Return the path to the PID file."""
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(project_root, PID_FILE_NAME)


def _make_entry_id(timestamp, text):
    """Generate a deterministic ID from timestamp + text."""
    raw = f"{timestamp}|{text}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _parse_timestamp(ts_str):
    """Parse an ISO timestamp string into a datetime. Returns None on failure."""
    if not ts_str:
        return None
    try:
        # Handle ISO format with or without timezone
        ts_str = ts_str.replace("Z", "+00:00")
        return datetime.fromisoformat(ts_str)
    except (ValueError, TypeError):
        return None


def process_captures(captures, config):
    """
    Process a list of raw captures from the Chrome extension.

    Each capture is a dict with: timestamp, domain, field_type, text, page_title.

    Deduplicates against processed_ids and writes to the TextStream vault
    organized by year/month.

    Returns (saved_count, duplicate_count).
    """
    vault_path = _get_vault_path(config)
    processed_ids = load_processed_ids(vault_path)

    # Group entries by vault file path
    file_groups = {}
    new_ids = []
    saved = 0
    duplicates = 0

    for capture in captures:
        timestamp = capture.get("timestamp", "")
        text = capture.get("text", "")
        domain = capture.get("domain", "unknown")
        page_title = capture.get("page_title", "")
        field_type = capture.get("field_type", "unknown")

        if not text or not text.strip():
            continue

        text = text.strip()
        entry_id = _make_entry_id(timestamp, text)

        # Skip duplicates
        if entry_id in processed_ids:
            duplicates += 1
            continue

        # Parse timestamp for filing
        dt = _parse_timestamp(timestamp)
        if dt:
            year = dt.year
            month = dt.month
        else:
            # Use current time as fallback
            now = datetime.now()
            year = now.year
            month = now.month

        word_count = len(text.split())

        # Build embedding text with domain context
        text_for_embedding = f"[{domain}] {text}"

        entry = {
            "id": entry_id,
            "timestamp": timestamp,
            "domain": domain,
            "text": text,
            "page_title": page_title,
            "field_type": field_type,
            "year": year,
            "month": month,
            "word_count": word_count,
            "text_for_embedding": text_for_embedding,
        }

        # Determine file path
        year_dir = os.path.join(vault_path, str(year))
        filename = f"{month:02d}_{_MONTH_NAMES[month - 1]}.jsonl"
        file_path = os.path.join(year_dir, filename)

        if file_path not in file_groups:
            file_groups[file_path] = {"year_dir": year_dir, "entries": []}
        file_groups[file_path]["entries"].append(entry)

        new_ids.append(entry_id)
        processed_ids.add(entry_id)
        saved += 1

    # Write entries grouped by file
    for file_path, group in file_groups.items():
        os.makedirs(group["year_dir"], exist_ok=True)
        flush_entries(group["entries"], os.path.dirname(file_path), os.path.basename(file_path))

    # Track processed IDs
    if new_ids:
        append_processed_ids(vault_path, new_ids)

    return saved, duplicates


# ---------------------------------------------------------------------------
# HTTP receiver
# ---------------------------------------------------------------------------

class CaptureHandler(BaseHTTPRequestHandler):
    """HTTP handler that accepts capture batches from the Chrome extension."""

    # Shared config set by run_server()
    config = {}

    def log_message(self, format, *args):
        """Route HTTP logs to our logger instead of stderr."""
        logger.debug(format, *args)

    def _send_json(self, status, data):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode("utf-8"))

    def do_OPTIONS(self):
        """Handle CORS preflight requests."""
        self._send_json(200, {"ok": True})

    def do_POST(self):
        """Handle capture batch submissions."""
        if self.path != "/capture":
            self._send_json(404, {"error": "not found"})
            return

        # Read request body
        content_length = int(self.headers.get("Content-Length", 0))
        if content_length == 0:
            self._send_json(400, {"error": "empty body"})
            return

        if content_length > 10 * 1024 * 1024:  # 10MB limit
            self._send_json(413, {"error": "payload too large"})
            return

        try:
            body = self.rfile.read(content_length)
            data = json.loads(body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            self._send_json(400, {"error": f"invalid JSON: {e}"})
            return

        captures = data.get("captures", [])
        if not isinstance(captures, list):
            self._send_json(400, {"error": "captures must be an array"})
            return

        if len(captures) == 0:
            self._send_json(200, {"saved": 0, "duplicates": 0})
            return

        try:
            saved, duplicates = process_captures(captures, self.config)
            logger.info(
                "Received %d captures: %d saved, %d duplicates",
                len(captures), saved, duplicates,
            )
            self._send_json(200, {"saved": saved, "duplicates": duplicates})
        except Exception as e:
            logger.error("Error processing captures: %s", e, exc_info=True)
            self._send_json(500, {"error": str(e)})

    def do_GET(self):
        """Health check endpoint."""
        if self.path == "/health":
            self._send_json(200, {"status": "running", "service": "nomolo-text-stream"})
            return
        self._send_json(404, {"error": "not found"})


# ---------------------------------------------------------------------------
# Server lifecycle
# ---------------------------------------------------------------------------

def run_server(config=None):
    """
    Start the text stream receiver HTTP server.
    Blocks until interrupted (Ctrl+C or SIGTERM).
    Writes a PID file for stop command.
    """
    config = config or {}
    CaptureHandler.config = config

    vault_path = _get_vault_path(config)
    os.makedirs(vault_path, exist_ok=True)

    # Write PID file
    pid_file = _get_pid_file_path()
    with open(pid_file, "w") as f:
        f.write(str(os.getpid()))

    server = HTTPServer((HOST, PORT), CaptureHandler)
    server.timeout = 1  # Allow periodic shutdown checks

    print(f"\n  Nomolo Text Stream receiver started")
    print(f"  Listening on http://{HOST}:{PORT}")
    print(f"  Vault: {vault_path}")
    print(f"  PID: {os.getpid()}")
    print()
    print(f"  Install the Chrome extension from:")
    print(f"    chrome://extensions > Load unpacked > collectors/browser_input/")
    print()
    print(f"  Press Ctrl+C to stop.\n")

    shutdown_event = threading.Event()

    def signal_handler(signum, frame):
        print("\n  Shutting down text stream receiver...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        while not shutdown_event.is_set():
            server.handle_request()
    finally:
        server.server_close()
        # Clean up PID file
        if os.path.exists(pid_file):
            os.remove(pid_file)
        print("  Text stream receiver stopped.")


def stop_server():
    """
    Stop a running text stream receiver by reading the PID file
    and sending SIGTERM.
    """
    pid_file = _get_pid_file_path()

    if not os.path.exists(pid_file):
        print("\n  Text stream receiver is not running (no PID file found).")
        return False

    try:
        with open(pid_file, "r") as f:
            pid = int(f.read().strip())
    except (ValueError, OSError):
        print("\n  Error: Could not read PID file.")
        if os.path.exists(pid_file):
            os.remove(pid_file)
        return False

    try:
        os.kill(pid, signal.SIGTERM)
        print(f"\n  Sent stop signal to text stream receiver (PID {pid}).")

        # Wait briefly and verify it stopped
        import time
        for _ in range(10):
            time.sleep(0.5)
            try:
                os.kill(pid, 0)  # Check if still running
            except OSError:
                print("  Receiver stopped successfully.")
                if os.path.exists(pid_file):
                    os.remove(pid_file)
                return True

        print("  Warning: Process may still be running.")
        return True

    except ProcessLookupError:
        print(f"\n  Process {pid} is not running (stale PID file). Cleaning up.")
        if os.path.exists(pid_file):
            os.remove(pid_file)
        return False
    except PermissionError:
        print(f"\n  Error: Permission denied sending signal to PID {pid}.")
        return False


def is_running():
    """Check if the text stream receiver is currently running."""
    pid_file = _get_pid_file_path()

    if not os.path.exists(pid_file):
        return False

    try:
        with open(pid_file, "r") as f:
            pid = int(f.read().strip())
        os.kill(pid, 0)  # Signal 0 just checks if process exists
        return True
    except (ValueError, OSError):
        return False
