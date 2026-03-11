"""
WHID Gmail Collector
Exports Gmail messages to a local JSONL vault organized by year/month.
Uses gmail.readonly scope — your email is never modified.

Usage:
    python -m collectors.gmail_collector [vault_name]
    python -m collectors.gmail_collector Primary
"""

import os
import json
import base64
import sys
import logging
import threading
import concurrent.futures
from datetime import datetime

import yaml
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from bs4 import BeautifulSoup

# --- Configuration ---

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config.yaml")

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

CONFIG = load_config()
GMAIL_CONFIG = CONFIG.get("gmail", {})

VAULT_NAME = sys.argv[1] if len(sys.argv) > 1 else "Primary"
VAULT_ROOT = os.path.join(CONFIG.get("vault_root", "Vaults"), f"Gmail_{VAULT_NAME}")
PROCESSED_LOG = os.path.join(VAULT_ROOT, "processed_ids.txt")
MISSING_LOG = os.path.join(VAULT_ROOT, "missing_ids.txt")
LOG_FILE = os.path.join(VAULT_ROOT, "extraction.log")
MAX_WORKERS = GMAIL_CONFIG.get("max_workers", 3)
PAGE_SIZE = GMAIL_CONFIG.get("page_size", 500)
SCOPES = [GMAIL_CONFIG.get("scope", "https://www.googleapis.com/auth/gmail.readonly")]
CREDENTIALS_FILE = GMAIL_CONFIG.get("credentials_file", "credentials.json")
TOKEN_FILE = GMAIL_CONFIG.get("token_file", "token.json")

os.makedirs(VAULT_ROOT, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("whid.gmail")

# Thread-safe lock for file writes
_write_lock = threading.Lock()
_processed_lock = threading.Lock()


def get_credentials():
    """Authenticate with Google OAuth2. Opens browser on first run."""
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(CREDENTIALS_FILE):
                logger.error(
                    "Missing %s — download it from Google Cloud Console "
                    "(APIs & Services > Credentials > OAuth 2.0 Client ID)",
                    CREDENTIALS_FILE,
                )
                sys.exit(1)
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())

    return creds


def clean_html_to_text(payload):
    """Extract plain text from a Gmail message payload, stripping HTML."""
    def parse_parts(parts):
        text = ""
        for part in parts:
            mime = part.get("mimeType", "")
            if mime == "text/plain":
                data = part.get("body", {}).get("data", "")
                if data:
                    text += base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")
            elif mime == "text/html":
                data = part.get("body", {}).get("data", "")
                if data:
                    html = base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")
                    soup = BeautifulSoup(html, "html.parser")
                    for tag in soup(["script", "style", "header", "footer", "nav"]):
                        tag.decompose()
                    text += soup.get_text(separator=" ")
            elif "parts" in part:
                text += parse_parts(part["parts"])
        return text

    if "parts" in payload:
        body = parse_parts(payload["parts"])
    else:
        data = payload.get("body", {}).get("data", "")
        body = base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore") if data else ""

    return " ".join(body.split())


def _parse_message_date(date_str):
    """Parse a message date string. Returns (datetime, is_valid) tuple."""
    if not date_str:
        return None, False

    clean = date_str.split(" (")[0].strip()
    try:
        return datetime.strptime(clean, "%a, %d %b %Y %H:%M:%S %z"), True
    except ValueError:
        pass

    try:
        return datetime.fromisoformat(date_str), True
    except ValueError:
        pass

    return None, False


def _write_entry_to_vault(entry, dt, is_valid_date):
    """Write a single entry to the appropriate vault file, thread-safe."""
    if is_valid_date and dt:
        target_dir = os.path.join(VAULT_ROOT, dt.strftime("%Y"))
        filename = dt.strftime("%m_%B.jsonl")
    else:
        target_dir = os.path.join(VAULT_ROOT, "_unknown")
        filename = "unknown_date.jsonl"

    os.makedirs(target_dir, exist_ok=True)
    file_path = os.path.join(target_dir, filename)

    with _write_lock:
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")


def process_message(m_id, creds, processed_ids):
    """Fetch and vault a single Gmail message. Returns (id, status) or (None, None)."""
    if m_id in processed_ids:
        return None, None

    try:
        service = build("gmail", "v1", credentials=creds, cache_discovery=False)
        msg = service.users().messages().get(userId="me", id=m_id).execute()

        headers = {
            h["name"].lower(): h["value"]
            for h in msg.get("payload", {}).get("headers", [])
        }

        entry = {
            "id": m_id,
            "threadId": msg.get("threadId", ""),
            "date": headers.get("date", ""),
            "subject": headers.get("subject", "No Subject"),
            "from": headers.get("from", ""),
            "to": headers.get("to", ""),
            "tags": msg.get("labelIds", []),
            "body_raw": clean_html_to_text(msg.get("payload", {})),
        }

        dt, is_valid = _parse_message_date(entry["date"])
        if not is_valid:
            logger.warning("Unparseable date for message %s: '%s' — filing under _unknown/", m_id, entry["date"])

        _write_entry_to_vault(entry, dt, is_valid)
        return m_id, "VAULTED"

    except Exception as e:
        logger.error("Failed to process message %s: %s", m_id, e)
        return None, None


def run_export():
    """Main export loop: fetch message IDs, process in parallel, track progress."""
    creds = get_credentials()
    service = build("gmail", "v1", credentials=creds)

    # Load already-processed IDs
    processed_ids = set()
    if os.path.exists(PROCESSED_LOG):
        with open(PROCESSED_LOG, "r") as f:
            processed_ids = {line.strip() for line in f if line.strip()}

    # Determine which messages to process
    is_sniper_run = False
    if os.path.exists(MISSING_LOG):
        # Sniper mode: recover ghost IDs
        with open(MISSING_LOG, "r") as f:
            to_process_ids = [line.strip() for line in f if line.strip()]
        is_sniper_run = True
        logger.info("Sniper mode: recovering %d ghost IDs", len(to_process_ids))
    else:
        # Normal mode: list all messages
        logger.info("Fetching message list from Gmail...")
        to_process_ids = []
        results = service.users().messages().list(
            userId="me", maxResults=PAGE_SIZE
        ).execute()
        msgs = results.get("messages", [])
        to_process_ids.extend(m["id"] for m in msgs)

        while "nextPageToken" in results:
            results = service.users().messages().list(
                userId="me",
                maxResults=PAGE_SIZE,
                pageToken=results["nextPageToken"],
            ).execute()
            msgs = results.get("messages", [])
            to_process_ids.extend(m["id"] for m in msgs)

        logger.info("Found %d total messages in Gmail", len(to_process_ids))

        # Filter out already processed
        to_process_ids = [mid for mid in to_process_ids if mid not in processed_ids]

    if not to_process_ids:
        logger.info("Nothing to process — vault is up to date.")
        return

    logger.info("Processing %d messages with %d workers...", len(to_process_ids), MAX_WORKERS)

    vaulted = 0
    failed = 0
    total = len(to_process_ids)
    newly_processed = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_message, m_id, creds, processed_ids): m_id
            for m_id in to_process_ids
        }

        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            m_id, status = future.result()

            if status == "VAULTED":
                vaulted += 1
                newly_processed.append(m_id)
            else:
                failed += 1

            # Progress update every 50 messages
            if i % 50 == 0 or i == total:
                logger.info("Progress: %d/%d (vaulted: %d, failed: %d)", i, total, vaulted, failed)

    # Append newly processed IDs (thread-safe, single writer now)
    if newly_processed:
        with _processed_lock:
            with open(PROCESSED_LOG, "a") as f:
                for mid in newly_processed:
                    f.write(f"{mid}\n")

    # Clean up missing_ids.txt after successful sniper run
    if is_sniper_run and vaulted > 0 and os.path.exists(MISSING_LOG):
        os.remove(MISSING_LOG)

    logger.info(
        "Export complete: %d vaulted, %d failed out of %d total",
        vaulted, failed, total,
    )


if __name__ == "__main__":
    run_export()
