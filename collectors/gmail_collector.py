"""
WHID Gmail Collector
Exports Gmail messages to a local JSONL vault organized by year/month.
Uses gmail.readonly scope — your email is never modified.

Uses the Gmail Batch API to fetch up to 100 messages per HTTP request (~10x faster).

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
from collections import defaultdict
from datetime import datetime

import yaml
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import BatchHttpRequest
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
MAX_WORKERS = GMAIL_CONFIG.get("max_workers", 10)
PAGE_SIZE = GMAIL_CONFIG.get("page_size", 500)
BATCH_SIZE = GMAIL_CONFIG.get("batch_size", 100)
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

# Lock for writing vault files to disk
_write_lock = threading.Lock()


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
                    text += base64.urlsafe_b64decode(data).decode(
                        "utf-8", errors="ignore"
                    )
            elif mime == "text/html":
                data = part.get("body", {}).get("data", "")
                if data:
                    html = base64.urlsafe_b64decode(data).decode(
                        "utf-8", errors="ignore"
                    )
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
        body = (
            base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")
            if data
            else ""
        )

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


def _msg_to_entry(m_id, msg):
    """Convert a raw Gmail API message dict into a vault entry."""
    headers = {
        h["name"].lower(): h["value"]
        for h in msg.get("payload", {}).get("headers", [])
    }

    return {
        "id": m_id,
        "threadId": msg.get("threadId", ""),
        "date": headers.get("date", ""),
        "subject": headers.get("subject", "No Subject"),
        "from": headers.get("from", ""),
        "to": headers.get("to", ""),
        "tags": msg.get("labelIds", []),
        "body_raw": clean_html_to_text(msg.get("payload", {})),
    }


def _flush_entries_to_vault(entries):
    """Write a batch of entries to vault files, grouped by target file."""
    # Group entries by destination file
    file_groups = defaultdict(list)

    for entry in entries:
        dt, is_valid = _parse_message_date(entry["date"])
        if not is_valid:
            logger.warning(
                "Unparseable date for message %s: '%s' — filing under _unknown/",
                entry["id"],
                entry["date"],
            )

        if is_valid and dt:
            target_dir = os.path.join(VAULT_ROOT, dt.strftime("%Y"))
            filename = dt.strftime("%m_%B.jsonl")
        else:
            target_dir = os.path.join(VAULT_ROOT, "_unknown")
            filename = "unknown_date.jsonl"

        file_path = os.path.join(target_dir, filename)
        file_groups[file_path].append((target_dir, entry))

    # Write each group in one go
    with _write_lock:
        for file_path, items in file_groups.items():
            os.makedirs(items[0][0], exist_ok=True)
            with open(file_path, "a", encoding="utf-8") as f:
                for _, entry in items:
                    f.write(json.dumps(entry) + "\n")


def _fetch_batch(service, message_ids):
    """
    Fetch a batch of messages using the Gmail Batch API.
    Sends up to 100 requests in a single HTTP call.
    Returns (list of entries, list of failed IDs).
    """
    entries = []
    failed = []
    lock = threading.Lock()

    def callback(request_id, response, exception):
        if exception is not None:
            logger.error("Batch fetch failed for %s: %s", request_id, exception)
            with lock:
                failed.append(request_id)
            return

        entry = _msg_to_entry(request_id, response)
        with lock:
            entries.append(entry)

    batch = service.new_batch_http_request(callback=callback)
    for m_id in message_ids:
        batch.add(
            service.users().messages().get(userId="me", id=m_id),
            request_id=m_id,
        )
    batch.execute()

    return entries, failed


def run_export():
    """Main export loop: fetch message IDs, batch-fetch in parallel, track progress."""
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
        with open(MISSING_LOG, "r") as f:
            to_process_ids = [line.strip() for line in f if line.strip()]
        is_sniper_run = True
        logger.info("Sniper mode: recovering %d ghost IDs", len(to_process_ids))
    else:
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
        to_process_ids = [mid for mid in to_process_ids if mid not in processed_ids]

    if not to_process_ids:
        logger.info("Nothing to process — vault is up to date.")
        return

    # Split into batches of BATCH_SIZE (max 100 per Gmail batch request)
    batches = [
        to_process_ids[i : i + BATCH_SIZE]
        for i in range(0, len(to_process_ids), BATCH_SIZE)
    ]

    total_msgs = len(to_process_ids)
    total_batches = len(batches)
    logger.info(
        "Processing %d messages in %d batches (batch_size=%d, workers=%d)...",
        total_msgs,
        total_batches,
        BATCH_SIZE,
        MAX_WORKERS,
    )

    vaulted = 0
    failed = 0
    all_vaulted_ids = []

    def process_batch(batch_ids):
        """Process one batch: fetch via batch API, return entries and failures."""
        # Each thread gets its own service instance (connection pooling)
        thread_service = build("gmail", "v1", credentials=creds, cache_discovery=False)
        return _fetch_batch(thread_service, batch_ids)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_idx = {
            executor.submit(process_batch, batch_ids): idx
            for idx, batch_ids in enumerate(batches)
        }

        for future in concurrent.futures.as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                entries, batch_failed = future.result()
            except Exception as e:
                logger.error("Batch %d raised an exception: %s", idx, e)
                failed += BATCH_SIZE
                continue

            # Flush entries to disk
            if entries:
                _flush_entries_to_vault(entries)
                vaulted += len(entries)
                all_vaulted_ids.extend(e["id"] for e in entries)

            failed += len(batch_failed)

            # Progress update
            processed_so_far = vaulted + failed
            logger.info(
                "Progress: %d/%d messages (vaulted: %d, failed: %d) — batch %d/%d done",
                processed_so_far,
                total_msgs,
                vaulted,
                failed,
                idx + 1,
                total_batches,
            )

    # Append newly processed IDs
    if all_vaulted_ids:
        with open(PROCESSED_LOG, "a") as f:
            for mid in all_vaulted_ids:
                f.write(f"{mid}\n")

    # Clean up missing_ids.txt after successful sniper run
    if is_sniper_run and vaulted > 0 and os.path.exists(MISSING_LOG):
        os.remove(MISSING_LOG)

    logger.info(
        "Export complete: %d vaulted, %d failed out of %d total",
        vaulted,
        failed,
        total_msgs,
    )


if __name__ == "__main__":
    run_export()
