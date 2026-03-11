"""
WHID Gmail Collector
Exports Gmail messages to a local JSONL vault organized by year/month.
Uses gmail.readonly scope — your email is never modified.

Uses the Gmail Batch API to fetch up to 100 messages per HTTP request (~10x faster).
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

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from bs4 import BeautifulSoup

logger = logging.getLogger("whid.gmail")

# Lock for writing vault files to disk
_write_lock = threading.Lock()


def get_credentials(credentials_file, token_file, scopes):
    """Authenticate with Google OAuth2. Opens browser on first run."""
    creds = None
    if os.path.exists(token_file):
        try:
            creds = Credentials.from_authorized_user_file(token_file, scopes)
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            logger.warning(
                "Existing token.json is corrupted (%s) — re-authenticating.", e
            )
            os.remove(token_file)
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                logger.warning(
                    "Token refresh failed (%s) — re-authenticating. "
                    "You may need to sign in again.",
                    e,
                )
                creds = None

        if not creds:
            if not os.path.exists(credentials_file):
                raise FileNotFoundError(
                    f"OAuth credentials not found: {credentials_file}\n\n"
                    "To get credentials.json:\n"
                    "  1. Go to https://console.cloud.google.com\n"
                    "  2. Create a project (or select existing)\n"
                    "  3. Enable the Gmail API (Library > search 'Gmail API')\n"
                    "  4. Go to APIs & Services > Credentials\n"
                    "  5. Create Credentials > OAuth Client ID > Desktop App\n"
                    "  6. Download the JSON and save as: credentials.json\n\n"
                    "For detailed instructions, see: docs/GOOGLE_SETUP.md"
                )

            try:
                flow = InstalledAppFlow.from_client_secrets_file(
                    credentials_file, scopes
                )
            except (json.JSONDecodeError, ValueError) as e:
                print(f"\nError: credentials.json is not valid JSON: {e}")
                print(
                    "Re-download it from Google Cloud Console > APIs & Services > Credentials."
                )
                sys.exit(1)

            try:
                creds = flow.run_local_server(port=0)
            except OSError as e:
                print(f"\nError: Could not start local OAuth server: {e}")
                print(
                    "This usually means another process is blocking the port."
                )
                print("Close other running WHID instances and try again.")
                sys.exit(1)

        with open(token_file, "w") as f:
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


def _flush_entries_to_vault(entries, vault_root):
    """Write a batch of entries to vault files, grouped by target file."""
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
            target_dir = os.path.join(vault_root, dt.strftime("%Y"))
            filename = dt.strftime("%m_%B.jsonl")
        else:
            target_dir = os.path.join(vault_root, "_unknown")
            filename = "unknown_date.jsonl"

        file_path = os.path.join(target_dir, filename)
        file_groups[file_path].append((target_dir, entry))

    with _write_lock:
        for file_path, items in file_groups.items():
            try:
                os.makedirs(items[0][0], exist_ok=True)
                with open(file_path, "a", encoding="utf-8") as f:
                    for _, entry in items:
                        f.write(json.dumps(entry) + "\n")
            except PermissionError:
                logger.error(
                    "Permission denied writing to %s — check folder permissions.",
                    file_path,
                )
                raise
            except OSError as e:
                if "No space left" in str(e) or e.errno == 28:
                    logger.error("Disk full — cannot write to %s. Free up space and re-run.", file_path)
                else:
                    logger.error("Failed to write %s: %s", file_path, e)
                raise


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
            if isinstance(exception, HttpError):
                status = exception.resp.status
                if status == 404:
                    logger.warning("Message %s no longer exists (deleted from Gmail).", request_id)
                elif status == 429:
                    logger.error("Rate limited by Gmail API. Reduce max_workers or batch_size in config.yaml.")
                elif status == 403:
                    logger.error(
                        "Permission denied for message %s. "
                        "Make sure Gmail API is enabled and scope is correct.",
                        request_id,
                    )
                else:
                    logger.error("API error %d for message %s: %s", status, request_id, exception)
            else:
                logger.error("Unexpected error for message %s: %s", request_id, exception)

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


def _handle_api_error(e):
    """Print a helpful message for common Gmail API errors and exit."""
    if not isinstance(e, HttpError):
        print(f"\nUnexpected error: {e}")
        sys.exit(1)

    status = e.resp.status

    if status == 403:
        error_detail = str(e)
        if "Gmail API has not been used" in error_detail or "accessNotConfigured" in error_detail:
            print("\nError: Gmail API is not enabled for your Google Cloud project.\n")
            print("To fix:")
            print("  1. Go to https://console.cloud.google.com/apis/library/gmail.googleapis.com")
            print("  2. Click 'Enable'")
            print("  3. Wait 1-2 minutes for it to activate")
            print("  4. Run whid again")
        elif "insufficientPermissions" in error_detail:
            print("\nError: Insufficient permissions.\n")
            print("Your OAuth token may have the wrong scope.")
            print("To fix:")
            print("  1. Delete token.json")
            print("  2. Run 'whid collect gmail' again")
            print("  3. Re-authorize in the browser")
        else:
            print(f"\nError: Access denied by Gmail API (403).\n")
            print(f"Details: {error_detail}")
            print("\nCommon fixes:")
            print("  - Enable Gmail API in Google Cloud Console")
            print("  - Delete token.json and re-authorize")
    elif status == 429:
        print("\nError: Rate limited by Gmail API.\n")
        print("You're sending too many requests. To fix:")
        print("  - Lower max_workers in config.yaml (try 5)")
        print("  - Lower batch_size in config.yaml (try 50)")
        print("  - Wait a few minutes and try again")
    elif status == 401:
        print("\nError: Authentication failed.\n")
        print("Your token may be expired or revoked. To fix:")
        print("  1. Delete token.json")
        print("  2. Run 'whid collect gmail' again")
        print("  3. Re-authorize in the browser")
    else:
        print(f"\nGmail API error (HTTP {status}): {e}")

    sys.exit(1)


def run_export(vault_name="Primary", config=None):
    """Main export: fetch message IDs, batch-fetch in parallel, track progress."""
    config = config or {}
    gmail_config = config.get("gmail", {})

    vault_root = os.path.join(
        os.path.expanduser(config.get("vault_root", "~/Documents/WHID_Vaults")),
        f"Gmail_{vault_name}",
    )
    max_workers = gmail_config.get("max_workers", 10)
    page_size = gmail_config.get("page_size", 500)
    batch_size = gmail_config.get("batch_size", 100)
    scopes = [
        gmail_config.get("scope", "https://www.googleapis.com/auth/gmail.readonly")
    ]
    credentials_file = gmail_config.get("credentials_file", "credentials.json")
    token_file = gmail_config.get("token_file", "token.json")

    processed_log = os.path.join(vault_root, "processed_ids.txt")
    missing_log = os.path.join(vault_root, "missing_ids.txt")
    log_file = os.path.join(vault_root, "extraction.log")

    try:
        os.makedirs(vault_root, exist_ok=True)
    except PermissionError:
        print(f"\nError: Permission denied creating vault directory: {vault_root}")
        print("Check folder permissions or change vault_root in config.yaml.")
        sys.exit(1)
    except OSError as e:
        print(f"\nError: Cannot create vault directory {vault_root}: {e}")
        sys.exit(1)

    # Add file handler for this vault's log
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    )
    logger.addHandler(file_handler)

    logger.info("Vault: %s", vault_root)

    creds = get_credentials(credentials_file, token_file, scopes)

    try:
        service = build("gmail", "v1", credentials=creds)
    except Exception as e:
        print(f"\nError: Could not connect to Gmail API: {e}")
        print("Check your internet connection and try again.")
        sys.exit(1)

    # Load already-processed IDs
    processed_ids = set()
    if os.path.exists(processed_log):
        with open(processed_log, "r") as f:
            processed_ids = {line.strip() for line in f if line.strip()}

    # Determine which messages to process
    is_sniper_run = False
    if os.path.exists(missing_log):
        with open(missing_log, "r") as f:
            to_process_ids = [line.strip() for line in f if line.strip()]
        is_sniper_run = True
        logger.info("Sniper mode: recovering %d ghost IDs", len(to_process_ids))
    else:
        logger.info("Fetching message list from Gmail...")
        to_process_ids = []

        try:
            results = (
                service.users()
                .messages()
                .list(userId="me", maxResults=page_size)
                .execute()
            )
        except HttpError as e:
            _handle_api_error(e)
        except Exception as e:
            print(f"\nError: Failed to fetch message list: {e}")
            print("Check your internet connection and try again.")
            sys.exit(1)

        msgs = results.get("messages", [])
        to_process_ids.extend(m["id"] for m in msgs)

        while "nextPageToken" in results:
            try:
                results = (
                    service.users()
                    .messages()
                    .list(
                        userId="me",
                        maxResults=page_size,
                        pageToken=results["nextPageToken"],
                    )
                    .execute()
                )
            except HttpError as e:
                _handle_api_error(e)
            except Exception as e:
                logger.error("Error fetching message list page: %s", e)
                logger.info("Continuing with %d messages collected so far.", len(to_process_ids))
                break

            msgs = results.get("messages", [])
            to_process_ids.extend(m["id"] for m in msgs)

        logger.info("Found %d total messages in Gmail", len(to_process_ids))
        to_process_ids = [mid for mid in to_process_ids if mid not in processed_ids]

    if not to_process_ids:
        logger.info("Nothing new to process — vault is up to date.")
        if processed_ids:
            logger.info("You have %d messages already vaulted.", len(processed_ids))
        else:
            logger.info("Your Gmail inbox appears empty.")
        logger.removeHandler(file_handler)
        file_handler.close()
        return

    # Split into batches
    batches = [
        to_process_ids[i : i + batch_size]
        for i in range(0, len(to_process_ids), batch_size)
    ]

    total_msgs = len(to_process_ids)
    total_batches = len(batches)
    logger.info(
        "Processing %d new messages in %d batches (batch_size=%d, workers=%d)...",
        total_msgs,
        total_batches,
        batch_size,
        max_workers,
    )

    vaulted = 0
    failed = 0
    all_vaulted_ids = []

    def process_batch(batch_ids):
        thread_service = build(
            "gmail", "v1", credentials=creds, cache_discovery=False
        )
        return _fetch_batch(thread_service, batch_ids)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {
            executor.submit(process_batch, batch_ids): idx
            for idx, batch_ids in enumerate(batches)
        }

        for future in concurrent.futures.as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                entries, batch_failed = future.result()
            except HttpError as e:
                logger.error("Batch %d hit API error: %s", idx, e)
                if e.resp.status == 429:
                    logger.error(
                        "Rate limited — reduce max_workers or batch_size in config.yaml."
                    )
                failed += batch_size
                continue
            except Exception as e:
                logger.error("Batch %d failed: %s", idx, e)
                failed += batch_size
                continue

            if entries:
                _flush_entries_to_vault(entries, vault_root)
                vaulted += len(entries)
                all_vaulted_ids.extend(e["id"] for e in entries)

            failed += len(batch_failed)

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

    if all_vaulted_ids:
        with open(processed_log, "a") as f:
            for mid in all_vaulted_ids:
                f.write(f"{mid}\n")

    if is_sniper_run and vaulted > 0 and os.path.exists(missing_log):
        try:
            os.remove(missing_log)
        except OSError as e:
            logger.warning("Could not remove missing_ids.txt: %s", e)

    logger.info(
        "Export complete: %d vaulted, %d failed out of %d total",
        vaulted,
        failed,
        total_msgs,
    )

    if failed > 0:
        logger.warning(
            "%d messages failed. Run 'whid groom gmail' then 'whid collect gmail' "
            "to recover them via Sniper mode.",
            failed,
        )

    # Clean up file handler
    logger.removeHandler(file_handler)
    file_handler.close()
