"""
WHID Gmail Collector
Exports Gmail messages to a local JSONL vault organized by year/month.
Uses gmail.readonly scope — your email is never modified.

Uses the Gmail Batch API to fetch up to 100 messages per HTTP request.
Adaptive rate limiting: automatically slows down when hitting API limits.
"""

import os
import json
import base64
import socket
import sys
import logging
import threading
import time
import concurrent.futures
from collections import defaultdict
from datetime import datetime

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from bs4 import BeautifulSoup
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout

logger = logging.getLogger("whid.gmail")

# Lock for writing vault files to disk
_write_lock = threading.Lock()

# Max retries for rate-limited batches
MAX_RETRIES = 4
RETRY_BASE_DELAY = 3  # seconds

# Network recovery settings
NETWORK_CHECK_INTERVAL = 10  # seconds between connectivity checks
NETWORK_MAX_WAIT = 600  # max seconds to wait for network (10 min)

# Exception types that indicate a network/timeout issue (not a permanent failure)
_NETWORK_ERRORS = (
    OSError,
    socket.timeout,
    ConnectionError,
    RequestsConnectionError,
    RequestsTimeout,
)


def _is_network_error(exc):
    """Return True if the exception looks like a transient network/timeout issue."""
    if isinstance(exc, _NETWORK_ERRORS):
        return True
    msg = str(exc).lower()
    return any(
        s in msg
        for s in ("timed out", "timeout", "connection reset", "connection refused",
                   "network is unreachable", "no route to host", "broken pipe",
                   "connection aborted", "eof occurred")
    )


def _wait_for_network():
    """Block until network connectivity is restored. Returns True if restored."""
    print("\n  Network appears down — waiting for reconnection...", end="", flush=True)
    waited = 0
    while waited < NETWORK_MAX_WAIT:
        try:
            socket.create_connection(("www.googleapis.com", 443), timeout=5).close()
            print(f" reconnected after {waited}s")
            return True
        except OSError:
            time.sleep(NETWORK_CHECK_INTERVAL)
            waited += NETWORK_CHECK_INTERVAL
            print(".", end="", flush=True)
    print(f" gave up after {NETWORK_MAX_WAIT}s")
    return False


class AdaptiveThrottle:
    """Automatically reduces concurrency when rate-limited."""

    def __init__(self, max_workers, batch_size):
        self._lock = threading.Lock()
        self.original_workers = max_workers
        self.original_batch_size = batch_size
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.rate_limit_count = 0
        self.success_streak = 0

    def on_rate_limit(self):
        with self._lock:
            self.rate_limit_count += 1
            self.success_streak = 0

            old_workers = self.max_workers
            old_batch = self.batch_size

            # Halve workers (min 1)
            if self.max_workers > 1:
                self.max_workers = max(1, self.max_workers // 2)

            # Halve batch size (min 10)
            if self.batch_size > 10:
                self.batch_size = max(10, self.batch_size // 2)

            if old_workers != self.max_workers or old_batch != self.batch_size:
                print(
                    f"\n  Throttling: workers {old_workers}->{self.max_workers}, "
                    f"batch {old_batch}->{self.batch_size}"
                )

    def on_success(self):
        with self._lock:
            self.success_streak += 1

            # After 20 consecutive successful batches, try ramping back up
            if self.success_streak >= 20:
                if self.max_workers < self.original_workers:
                    self.max_workers = min(
                        self.original_workers, self.max_workers + 1
                    )
                if self.batch_size < self.original_batch_size:
                    self.batch_size = min(
                        self.original_batch_size, self.batch_size + 10
                    )
                self.success_streak = 0


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

    # Replace named timezones with numeric offsets
    clean = clean.replace(" GMT", " +0000")
    clean = clean.replace(" UTC", " +0000")
    clean = clean.replace(" EST", " -0500")
    clean = clean.replace(" EDT", " -0400")
    clean = clean.replace(" CST", " -0600")
    clean = clean.replace(" CDT", " -0500")
    clean = clean.replace(" MST", " -0700")
    clean = clean.replace(" MDT", " -0600")
    clean = clean.replace(" PST", " -0800")
    clean = clean.replace(" PDT", " -0700")
    clean = clean.replace(" CET", " +0100")
    clean = clean.replace(" CEST", " +0200")

    # RFC 2822: "Mon, 01 Jan 2024 12:00:00 +0000"
    try:
        return datetime.strptime(clean, "%a, %d %b %Y %H:%M:%S %z"), True
    except ValueError:
        pass

    # Without day-of-week: "01 Jan 2024 12:00:00 +0000"
    try:
        return datetime.strptime(clean, "%d %b %Y %H:%M:%S %z"), True
    except ValueError:
        pass

    # ISO format: "2024-01-01T12:00:00+00:00"
    try:
        return datetime.fromisoformat(date_str), True
    except ValueError:
        pass

    # ctime format: "Fri Mar  6 09:19:53 2026"
    try:
        return datetime.strptime(clean, "%a %b %d %H:%M:%S %Y"), True
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
                    logger.error(
                        "Disk full — cannot write to %s. Free up space and re-run.",
                        file_path,
                    )
                else:
                    logger.error("Failed to write %s: %s", file_path, e)
                raise


def _fetch_batch(service, message_ids):
    """
    Fetch a batch of messages using the Gmail Batch API.
    Returns (entries, failed_ids, rate_limited_ids).
    """
    entries = []
    failed = []
    rate_limited = []
    lock = threading.Lock()

    def callback(request_id, response, exception):
        if exception is not None:
            if isinstance(exception, HttpError):
                status = exception.resp.status
                if status == 429:
                    with lock:
                        rate_limited.append(request_id)
                    return
                elif status == 404:
                    pass  # silently skip deleted messages
                elif status == 403:
                    logger.error(
                        "Permission denied for message %s. "
                        "Check Gmail API is enabled and scope is correct.",
                        request_id,
                    )
                else:
                    logger.error(
                        "API error %d for message %s: %s",
                        status,
                        request_id,
                        exception,
                    )
            else:
                logger.error(
                    "Unexpected error for message %s: %s",
                    request_id,
                    exception,
                )

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

    return entries, failed, rate_limited


def _handle_api_error(e):
    """Print a helpful message for common Gmail API errors and exit."""
    if not isinstance(e, HttpError):
        print(f"\nUnexpected error: {e}")
        sys.exit(1)

    status = e.resp.status

    if status == 403:
        error_detail = str(e)
        if (
            "Gmail API has not been used" in error_detail
            or "accessNotConfigured" in error_detail
        ):
            print(
                "\nError: Gmail API is not enabled for your Google Cloud project.\n"
            )
            print("To fix:")
            print(
                "  1. Go to https://console.cloud.google.com/apis/library/gmail.googleapis.com"
            )
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
        print("  - Lower max_workers in config.yaml (try 3)")
        print("  - Lower batch_size in config.yaml (try 25)")
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


def _process_batch_with_retry(batch_ids, service, throttle):
    """Process a batch with automatic retry on rate limiting and network errors."""
    try:
        entries, batch_failed, rate_limited = _fetch_batch(
            service, batch_ids
        )
    except Exception as e:
        if _is_network_error(e):
            logger.warning("Network error during batch fetch: %s", e)
            if _wait_for_network():
                # Retry the whole batch after reconnection
                try:
                    entries, batch_failed, rate_limited = _fetch_batch(
                        service, batch_ids
                    )
                except Exception as retry_e:
                    logger.error("Batch still failed after reconnect: %s", retry_e)
                    return [], list(batch_ids), False
            else:
                return [], list(batch_ids), False
        else:
            raise

    remaining = rate_limited
    attempt = 1

    while remaining and attempt <= MAX_RETRIES:
        throttle.on_rate_limit()
        delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
        logger.info(
            "Rate limited on %d messages — waiting %ds before retry %d/%d",
            len(remaining),
            delay,
            attempt,
            MAX_RETRIES,
        )
        time.sleep(delay)
        try:
            retry_entries, retry_failed, remaining = _fetch_batch(
                service, remaining
            )
        except Exception as e:
            if _is_network_error(e):
                logger.warning("Network error during retry: %s", e)
                if _wait_for_network():
                    continue  # retry same attempt after reconnect
                else:
                    batch_failed.extend(remaining)
                    remaining = []
                    break
            else:
                raise
        entries.extend(retry_entries)
        batch_failed.extend(retry_failed)
        attempt += 1

    # Any still rate-limited after all retries go to failed
    batch_failed.extend(remaining)

    if not rate_limited:
        throttle.on_success()

    return entries, batch_failed, len(rate_limited) > 0


def run_export(vault_name="Primary", config=None):
    """Main export: fetch message IDs, batch-fetch in parallel, track progress."""
    config = config or {}
    gmail_config = config.get("gmail", {})

    vault_root = os.path.join(
        os.path.expanduser(config.get("vault_root", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "vaults"))),
        f"Gmail_{vault_name}",
    )
    max_workers = gmail_config.get("max_workers", 8)
    page_size = gmail_config.get("page_size", 500)
    batch_size = gmail_config.get("batch_size", 100)
    scopes = [
        gmail_config.get(
            "scope", "https://www.googleapis.com/auth/gmail.readonly"
        )
    ]
    credentials_file = gmail_config.get("credentials_file", "credentials.json")
    token_file = gmail_config.get("token_file", "token.json")

    processed_log = os.path.join(vault_root, "processed_ids.txt")
    missing_log = os.path.join(vault_root, "missing_ids.txt")
    log_file = os.path.join(vault_root, "extraction.log")

    try:
        os.makedirs(vault_root, exist_ok=True)
    except PermissionError:
        print(
            f"\nError: Permission denied creating vault directory: {vault_root}"
        )
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

    print(f"\nSaving to: {vault_root}")

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

    if processed_ids:
        print(f"  + Already vaulted: {len(processed_ids):,} messages")

    # Track last run timestamp for incremental scans
    last_run_file = os.path.join(vault_root, "last_run.txt")

    # Determine which messages to process
    is_sniper_run = False
    if os.path.exists(missing_log):
        with open(missing_log, "r") as f:
            to_process_ids = [line.strip() for line in f if line.strip()]
        is_sniper_run = True
        print(
            f"  + Sniper mode: recovering {len(to_process_ids):,} missing messages"
        )
    else:
        to_process_ids = []

        def _print_scan_progress(count):
            frames = ["|", "/", "-", "\\"]
            frame = frames[(count // 500) % len(frames)]
            print(
                f"\r  {frame} Scanning inbox... {count:,} messages found",
                end="",
                flush=True,
            )

        # Gmail returns messages newest-first. Two scan strategies:
        #
        # 1. FIRST RUN (no processed_ids): full scan — page through everything.
        #
        # 2. UPDATE RUN (have processed_ids): scan newest-first and STOP as
        #    soon as an entire page of IDs is already processed. This makes
        #    daily updates near-instant — no need to crawl 130K messages.
        #
        # For extra safety on updates, we also use Gmail's `after:` query
        # when we have a last_run timestamp, so the API itself returns fewer
        # results.

        query = None
        is_incremental = bool(processed_ids)

        if is_incremental and os.path.exists(last_run_file):
            try:
                with open(last_run_file, "r") as f:
                    last_run_epoch = int(f.read().strip())
                # 1-day overlap to catch stragglers
                safe_epoch = last_run_epoch - 86400
                query = f"after:{safe_epoch}"
                from datetime import timezone
                last_dt = datetime.fromtimestamp(last_run_epoch, tz=timezone.utc)
                print(f"  + Incremental scan (messages since {last_dt.strftime('%Y-%m-%d %H:%M')} UTC)")
            except (ValueError, OSError):
                pass  # fall back to scan without query filter

        if is_incremental:
            print("  + Scanning newest-first, stopping at known messages...")

        list_kwargs = {"userId": "me", "maxResults": page_size}
        if query:
            list_kwargs["q"] = query

        def _fetch_list_page(**kwargs):
            """Fetch one page of message IDs with network retry."""
            try:
                return (
                    service.users()
                    .messages()
                    .list(**kwargs)
                    .execute()
                )
            except HttpError:
                raise
            except Exception as e:
                if _is_network_error(e):
                    logger.warning("Network error during scan: %s", e)
                    if _wait_for_network():
                        return (
                            service.users()
                            .messages()
                            .list(**kwargs)
                            .execute()
                        )
                    else:
                        raise
                raise

        try:
            results = _fetch_list_page(**list_kwargs)
        except HttpError as e:
            _handle_api_error(e)
        except Exception as e:
            if _is_network_error(e):
                print(f"\nError: Network unavailable. Try again later.")
            else:
                print(f"\nError: Failed to fetch message list: {e}")
                print("Check your internet connection and try again.")
            sys.exit(1)

        msgs = results.get("messages", [])
        page_ids = [m["id"] for m in msgs]
        to_process_ids.extend(page_ids)
        _print_scan_progress(len(to_process_ids))

        # Early termination: if every ID on this page is already processed,
        # we've reached messages we already have — stop scanning.
        scan_stopped_early = False
        if is_incremental and page_ids and all(mid in processed_ids for mid in page_ids):
            scan_stopped_early = True

        while "nextPageToken" in results and not scan_stopped_early:
            page_kwargs = {
                "userId": "me",
                "maxResults": page_size,
                "pageToken": results["nextPageToken"],
            }
            if query:
                page_kwargs["q"] = query

            try:
                results = _fetch_list_page(**page_kwargs)
            except HttpError as e:
                print()
                _handle_api_error(e)
            except Exception as e:
                if _is_network_error(e):
                    print(f"\n  (scan paused — network unavailable)")
                else:
                    logger.error("Error fetching message list page: %s", e)
                    print(f"\n  (stopped early: {e})")
                break

            msgs = results.get("messages", [])
            page_ids = [m["id"] for m in msgs]
            to_process_ids.extend(page_ids)
            _print_scan_progress(len(to_process_ids))

            # Check early termination after each page
            if is_incremental and page_ids and all(mid in processed_ids for mid in page_ids):
                scan_stopped_early = True

        if scan_stopped_early:
            print(f"\r  + Scan complete (caught up): {len(to_process_ids):,} messages checked        ")
        elif is_incremental:
            print(f"\r  + Scan complete (incremental): {len(to_process_ids):,} messages checked        ")
        else:
            print(f"\r  + Scan complete (full): {len(to_process_ids):,} messages found        ")

        to_process_ids = [
            mid for mid in to_process_ids if mid not in processed_ids
        ]

    if not to_process_ids:
        print("  + Nothing new — vault is up to date.")
        logger.removeHandler(file_handler)
        file_handler.close()
        return

    # Adaptive throttle
    throttle = AdaptiveThrottle(max_workers, batch_size)

    total_msgs = len(to_process_ids)
    print(f"  + Downloading {total_msgs:,} messages "
          f"(workers={max_workers}, batch={batch_size})\n")

    vaulted = 0
    failed = 0
    retried = 0
    all_vaulted_ids = []
    start_time = time.time()

    # Pre-build one service per worker thread (avoids repeated discovery calls)
    _thread_local = threading.local()

    def _get_thread_service():
        if not hasattr(_thread_local, "service"):
            _thread_local.service = build(
                "gmail", "v1", credentials=creds, cache_discovery=False
            )
        return _thread_local.service

    def _worker(batch_ids):
        return _process_batch_with_retry(batch_ids, _get_thread_service(), throttle)

    # Process in waves — allows adaptive throttle to adjust between waves
    offset = 0
    while offset < total_msgs:
        current_batch_size = throttle.batch_size
        current_workers = throttle.max_workers

        # Larger waves = better parallelism (4x instead of 2x)
        wave_end = min(offset + current_batch_size * current_workers * 4, total_msgs)
        wave_ids = to_process_ids[offset:wave_end]

        batches = [
            wave_ids[i : i + current_batch_size]
            for i in range(0, len(wave_ids), current_batch_size)
        ]

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=current_workers
        ) as executor:
            futures = {
                executor.submit(_worker, batch_ids): batch_ids
                for batch_ids in batches
            }

            for future in concurrent.futures.as_completed(futures):
                try:
                    entries, batch_failed, was_retried = future.result()
                except HttpError as e:
                    logger.error("Batch hit API error: %s", e)
                    failed += len(futures[future])
                    continue
                except Exception as e:
                    if _is_network_error(e):
                        logger.warning("Batch lost to network error: %s", e)
                        # Don't count as failed — these IDs stay unprocessed
                        # and will be picked up on next run via Sniper
                        failed += len(futures[future])
                        if _wait_for_network():
                            logger.info("Network restored — continuing remaining batches")
                        else:
                            logger.error("Network did not recover — aborting")
                            print("\n\n  Network unavailable. Progress saved — rerun to continue.")
                            offset = total_msgs  # break outer loop
                            break
                    else:
                        logger.error("Batch failed: %s", e)
                        failed += len(futures[future])
                    continue

                if entries:
                    _flush_entries_to_vault(entries, vault_root)
                    vaulted += len(entries)
                    all_vaulted_ids.extend(e["id"] for e in entries)

                if was_retried:
                    retried += 1

                failed += len(batch_failed)

                # Progress display
                elapsed = time.time() - start_time
                rate = vaulted / elapsed if elapsed > 0 else 0
                processed_so_far = vaulted + failed
                pct = int(processed_so_far / total_msgs * 100)

                bar_width = 25
                filled = int(bar_width * processed_so_far / total_msgs)
                bar = "#" * filled + "." * (bar_width - filled)

                fail_str = f" | {failed} failed" if failed > 0 else ""
                eta = ""
                if rate > 0 and vaulted < total_msgs:
                    remaining = (total_msgs - processed_so_far) / rate
                    if remaining > 60:
                        eta = f" | ~{remaining / 60:.0f}m left"
                    else:
                        eta = f" | ~{remaining:.0f}s left"

                print(
                    f"\r  [{bar}] {pct:3d}% | "
                    f"{vaulted:,}/{total_msgs:,} | "
                    f"{rate:.0f} msg/s{fail_str}{eta}   ",
                    end="",
                    flush=True,
                )

        offset = wave_end

    print()  # newline after progress bar

    if all_vaulted_ids:
        with open(processed_log, "a") as f:
            for mid in all_vaulted_ids:
                f.write(f"{mid}\n")

    if is_sniper_run and vaulted > 0 and os.path.exists(missing_log):
        try:
            os.remove(missing_log)
        except OSError as e:
            logger.warning("Could not remove missing_ids.txt: %s", e)

    # Save run timestamp for incremental scan next time
    try:
        with open(last_run_file, "w") as f:
            f.write(str(int(start_time)))
    except OSError as e:
        logger.warning("Could not save last_run.txt: %s", e)

    elapsed = time.time() - start_time
    rate = vaulted / elapsed if elapsed > 0 else 0

    print()
    print("  " + "=" * 45)
    print(f"    Done! {vaulted:,} messages vaulted")
    print(f"    Time: {elapsed:.1f}s ({rate:.0f} msg/s)")
    print(f"    Saved to: {vault_root}")
    print("  " + "=" * 45)

    if failed > 0:
        print(
            f"\n  {failed} messages failed — run 'whid groom gmail' then "
            "'whid collect gmail' to recover via Sniper."
        )

    if retried > 0:
        print(
            f"\n  Note: {retried} batches hit rate limits and were auto-retried."
        )
        if throttle.max_workers < throttle.original_workers:
            print(
                f"  Throttle adapted: workers {throttle.original_workers}->{throttle.max_workers}, "
                f"batch {throttle.original_batch_size}->{throttle.batch_size}"
            )
            print(
                "  Consider updating config.yaml with these lower values to avoid future throttling."
            )

    # Integrity check — count entries on disk vs processed_ids
    print("\n  Verifying vault integrity...", end="", flush=True)

    disk_ids = set()
    disk_entries = 0
    for root, _dirs, files in os.walk(vault_root):
        for f in files:
            if f.endswith(".jsonl"):
                try:
                    with open(os.path.join(root, f), "r") as fh:
                        for line in fh:
                            line = line.strip()
                            if line:
                                try:
                                    entry = json.loads(line)
                                    disk_entries += 1
                                    if "id" in entry:
                                        disk_ids.add(entry["id"])
                                except json.JSONDecodeError:
                                    pass
                except (OSError, PermissionError):
                    pass

    log_ids = set()
    if os.path.exists(processed_log):
        with open(processed_log, "r") as f:
            log_ids = {line.strip() for line in f if line.strip()}

    missing_from_disk = log_ids - disk_ids
    duplicates = disk_entries - len(disk_ids)

    if not missing_from_disk and duplicates == 0:
        print(f" OK ({len(disk_ids):,} entries, all accounted for)")
    else:
        print()
        if missing_from_disk:
            print(f"    WARNING: {len(missing_from_disk):,} IDs in log but missing from disk")
            print(f"    Run 'whid groom gmail' then 'whid collect gmail' to recover")
        if duplicates > 0:
            print(f"    INFO: {duplicates:,} duplicate entries found")
            print(f"    Run 'whid groom gmail' to deduplicate")

    # Clean up file handler
    logger.removeHandler(file_handler)
    file_handler.close()
