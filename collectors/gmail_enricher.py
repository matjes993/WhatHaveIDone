"""
NOMOLO Gmail Enricher
Re-fetches metadata from the Gmail API (format=metadata) to backfill
fields that weren't captured during the original collection:
cc, bcc, reply-to, message-id, in-reply-to, references,
list-unsubscribe, internalDate, sizeEstimate.

Uses the same adaptive throttling and network resilience as the collector.
Supports resume: entries that already have internalDate are skipped.
"""

import os
import json
import socket
import sys
import logging
import threading
import time
import concurrent.futures

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Suppress "file_cache is only supported with oauth2client<4.0.0" warning
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout

from collectors.gmail_collector import (
    get_credentials,
    AdaptiveThrottle,
    _is_network_error,
    _wait_for_network,
    MAX_RETRIES,
    RETRY_BASE_DELAY,
)
from core.vault import read_entries_by_file, rewrite_file_entries

logger = logging.getLogger("nomolo.enricher")

# Headers to fetch via format=metadata
METADATA_HEADERS = [
    "Cc", "Bcc", "Reply-To", "List-Unsubscribe",
    "Message-ID", "In-Reply-To", "References",
]


def _metadata_to_patch(msg):
    """Convert a metadata-only Gmail API response to a patch dict."""
    headers = {
        h["name"].lower(): h["value"]
        for h in msg.get("payload", {}).get("headers", [])
    }
    return {
        "cc": headers.get("cc", ""),
        "bcc": headers.get("bcc", ""),
        "reply_to": headers.get("reply-to", ""),
        "list_unsubscribe": headers.get("list-unsubscribe", ""),
        "message_id": headers.get("message-id", ""),
        "in_reply_to": headers.get("in-reply-to", ""),
        "references": headers.get("references", ""),
        "internalDate": msg.get("internalDate", ""),
        "sizeEstimate": msg.get("sizeEstimate", 0),
    }


def _fetch_metadata_batch(service, message_ids):
    """Batch-fetch metadata for a list of message IDs.
    Returns (patches, failed_ids, rate_limited_ids).
    patches: {msg_id: {field: value, ...}}
    """
    patches = {}
    failed = []
    rate_limited = []
    lock = threading.Lock()

    def callback(request_id, response, exception):
        if exception is not None:
            if isinstance(exception, HttpError):
                status = exception.resp.status
                if status in (429, 403):
                    with lock:
                        rate_limited.append(request_id)
                    return
                elif status == 404:
                    pass  # message deleted — skip silently
                else:
                    logger.error(
                        "API error %d for message %s: %s",
                        status, request_id, exception,
                    )
            else:
                logger.error(
                    "Unexpected error for message %s: %s",
                    request_id, exception,
                )
            with lock:
                failed.append(request_id)
            return

        patch = _metadata_to_patch(response)
        with lock:
            patches[request_id] = patch

    batch = service.new_batch_http_request(callback=callback)
    for m_id in message_ids:
        batch.add(
            service.users().messages().get(
                userId="me", id=m_id,
                format="metadata",
                metadataHeaders=METADATA_HEADERS,
            ),
            request_id=m_id,
        )
    batch.execute()

    return patches, failed, rate_limited


def _process_batch_with_retry(message_ids, service, throttle):
    """Fetch metadata for a batch, with automatic retry on rate limits."""
    try:
        patches, batch_failed, rate_limited = _fetch_metadata_batch(
            service, message_ids
        )
    except Exception as e:
        if _is_network_error(e):
            logger.warning("Network error during batch: %s", e)
            if _wait_for_network():
                try:
                    patches, batch_failed, rate_limited = _fetch_metadata_batch(
                        service, message_ids
                    )
                except Exception as retry_e:
                    logger.error("Batch still failed after reconnect: %s", retry_e)
                    return {}, message_ids, False
            else:
                return {}, message_ids, False
        else:
            raise

    if not rate_limited:
        throttle.on_success()
        return patches, batch_failed, False

    throttle.on_rate_limit()

    remaining = list(rate_limited)
    attempt = 1
    while remaining and attempt <= MAX_RETRIES:
        delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
        logger.info(
            "Rate limited on %d messages — waiting %ds before retry %d/%d",
            len(remaining), delay, attempt, MAX_RETRIES,
        )
        time.sleep(delay)
        try:
            retry_patches, retry_failed, remaining = _fetch_metadata_batch(
                service, remaining
            )
        except Exception as e:
            if _is_network_error(e):
                logger.warning("Network error during retry: %s", e)
                if _wait_for_network():
                    continue
                else:
                    batch_failed.extend(remaining)
                    remaining = []
                    break
            else:
                raise
        patches.update(retry_patches)
        batch_failed.extend(retry_failed)
        attempt += 1

    batch_failed.extend(remaining)

    if not rate_limited:
        throttle.on_success()

    return patches, batch_failed, len(rate_limited) > 0


def _print_progress(enriched, failed, total, start_time):
    """Print a progress bar with ETA."""
    elapsed = time.time() - start_time
    processed = enriched + failed
    rate = processed / elapsed if elapsed > 0 else 0

    pct = int(processed / total * 100) if total > 0 else 100
    bar_width = 25
    filled = int(bar_width * processed / total) if total > 0 else bar_width
    bar = "#" * filled + "." * (bar_width - filled)

    fail_str = f" | {failed} failed" if failed > 0 else ""
    eta = ""
    if rate > 0 and processed < total:
        remaining = (total - processed) / rate
        if remaining > 60:
            eta = f" | ~{remaining / 60:.0f}m left"
        else:
            eta = f" | ~{remaining:.0f}s left"

    print(
        f"\r  [{bar}] {pct:3d}% | "
        f"{enriched:,}/{total:,}{fail_str}{eta}   ",
        end="", flush=True,
    )


def run_enrich(vault_name="Primary", config=None):
    """Main entry point: backfill metadata for existing vault entries."""
    config = config or {}
    gmail_config = config.get("gmail", {})
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    vault_root_base = config.get("vault_root", os.path.join(project_root, "vaults"))
    vault_root_base = os.path.expanduser(vault_root_base)
    if not os.path.isabs(vault_root_base):
        vault_root_base = os.path.join(project_root, vault_root_base)
    vault_path = os.path.join(vault_root_base, f"Gmail_{vault_name}")

    if not os.path.exists(vault_path):
        print(f"\nError: Vault not found: {vault_path}")
        print("Run 'nomolo collect gmail' first.")
        sys.exit(1)

    max_workers = gmail_config.get("max_workers", 8)
    batch_size = min(gmail_config.get("batch_size", 100), 100)
    scopes = [
        gmail_config.get(
            "scope", "https://www.googleapis.com/auth/gmail.readonly"
        )
    ]
    credentials_file = gmail_config.get("credentials_file", "credentials.json")
    token_file = gmail_config.get("token_file", "token.json")

    log_file = os.path.join(vault_path, "enrichment.log")

    print(f"\n  NOMOLO Gmail Enricher")
    print(f"  {'=' * 45}")
    print(f"  Vault: {vault_path}")

    # Set up file logging
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    )
    logger.addHandler(file_handler)

    # Phase 1: Scan vault for unenriched entries
    print("\n  Phase 1: Scanning vault...")
    file_entries = read_entries_by_file(vault_path)

    total_entries = sum(len(entries) for entries in file_entries.values())
    print(f"  Found {total_entries:,} entries across {len(file_entries)} files")

    # Build map: msg_id -> file_path for entries needing enrichment
    unenriched_ids = []
    id_to_file = {}
    already_enriched = 0

    for file_path, entries in file_entries.items():
        for entry in entries:
            msg_id = entry.get("id", "")
            if not msg_id:
                continue
            if entry.get("internalDate"):
                already_enriched += 1
            else:
                unenriched_ids.append(msg_id)
                id_to_file[msg_id] = file_path

    if not unenriched_ids:
        print(f"  All {total_entries:,} entries already enriched — nothing to do.")
        logger.removeHandler(file_handler)
        file_handler.close()
        return

    if already_enriched > 0:
        print(f"  Already enriched: {already_enriched:,} (skipping)")
    print(f"  To enrich: {len(unenriched_ids):,}")

    # Phase 2: Authenticate
    print("\n  Phase 2: Authenticating...")
    try:
        creds = get_credentials(credentials_file, token_file, scopes)
    except FileNotFoundError as e:
        print(f"\n{e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: Authentication failed: {e}")
        sys.exit(1)

    # Phase 3: Batch-fetch metadata
    print(f"\n  Phase 3: Fetching metadata (format=metadata, workers={max_workers}, batch={batch_size})")
    print(f"  This fetches only headers — no body re-download.\n")

    throttle = AdaptiveThrottle(max_workers, batch_size)
    all_patches = {}
    enriched = 0
    failed = 0
    all_failed_ids = []
    start_time = time.time()

    _thread_local = threading.local()

    def _get_thread_service():
        if not hasattr(_thread_local, "service"):
            _thread_local.service = build(
                "gmail", "v1", credentials=creds, cache_discovery=False
            )
        return _thread_local.service

    def _worker(batch_ids):
        return _process_batch_with_retry(batch_ids, _get_thread_service(), throttle)

    total_to_fetch = len(unenriched_ids)
    offset = 0

    while offset < total_to_fetch:
        current_batch_size = throttle.batch_size
        current_workers = throttle.max_workers

        wave_end = min(offset + current_batch_size * current_workers * 4, total_to_fetch)
        wave_ids = unenriched_ids[offset:wave_end]

        batches = [
            wave_ids[i:i + current_batch_size]
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
                    patches, batch_failed, was_retried = future.result()
                except HttpError as e:
                    logger.error("Batch hit API error: %s", e)
                    failed += len(futures[future])
                    all_failed_ids.extend(futures[future])
                    continue
                except Exception as e:
                    if _is_network_error(e):
                        logger.warning("Batch lost to network error: %s", e)
                        failed += len(futures[future])
                        all_failed_ids.extend(futures[future])
                        if _wait_for_network():
                            logger.info("Network restored — continuing")
                        else:
                            logger.error("Network did not recover — aborting")
                            print("\n\n  Network unavailable. Progress saved — rerun to continue.")
                            offset = total_to_fetch
                            break
                    else:
                        logger.error("Batch failed: %s", e)
                        failed += len(futures[future])
                        all_failed_ids.extend(futures[future])
                    continue

                all_patches.update(patches)
                enriched += len(patches)
                failed += len(batch_failed)
                all_failed_ids.extend(batch_failed)

                _print_progress(enriched, failed, total_to_fetch, start_time)

        offset = wave_end

    _print_progress(enriched, failed, total_to_fetch, start_time)
    print()

    elapsed_fetch = time.time() - start_time
    print(f"\n  Fetched {enriched:,} metadata records in {elapsed_fetch:.1f}s")

    if not all_patches:
        print("  No metadata retrieved — nothing to write.")
        logger.removeHandler(file_handler)
        file_handler.close()
        return

    # Phase 4: Apply patches to vault files
    print(f"\n  Phase 4: Writing enriched entries to vault...")
    write_start = time.time()
    files_written = 0
    entries_patched = 0
    write_errors = 0

    # Group patches by file
    patches_by_file = {}
    for msg_id, patch in all_patches.items():
        file_path = id_to_file.get(msg_id)
        if file_path:
            if file_path not in patches_by_file:
                patches_by_file[file_path] = {}
            patches_by_file[file_path][msg_id] = patch

    total_files_to_write = len(patches_by_file)
    for i, (file_path, file_patches) in enumerate(patches_by_file.items()):
        entries = file_entries.get(file_path, [])
        changed = False

        for entry in entries:
            msg_id = entry.get("id", "")
            if msg_id in file_patches:
                entry.update(file_patches[msg_id])
                entries_patched += 1
                changed = True

        if changed:
            try:
                rewrite_file_entries(file_path, entries)
                files_written += 1
            except (PermissionError, OSError) as e:
                logger.error("Failed to write %s: %s", file_path, e)
                write_errors += 1

        if (i + 1) % 10 == 0 or (i + 1) == total_files_to_write:
            print(
                f"\r  Writing: {i + 1}/{total_files_to_write} files   ",
                end="", flush=True,
            )

    print()
    write_elapsed = time.time() - write_start

    total_elapsed = time.time() - start_time

    print(f"\n  {'=' * 45}")
    print(f"  Done! Enriched {entries_patched:,} entries in {total_elapsed:.1f}s")
    print(f"  Files rewritten: {files_written}")
    if failed > 0:
        print(f"  Failed: {failed} (will be retried on next run)")
    if write_errors > 0:
        print(f"  Write errors: {write_errors} (check logs)")
    print(f"  Log: {log_file}")
    print(f"  {'=' * 45}")

    print(f"\n  New fields per entry:")
    print(f"    internalDate     — Gmail's reliable timestamp (epoch ms)")
    print(f"    sizeEstimate     — message size in bytes")
    print(f"    cc / bcc         — additional recipients")
    print(f"    reply_to         — real reply address")
    print(f"    message_id       — RFC Message-ID")
    print(f"    in_reply_to      — parent message reference")
    print(f"    references       — full thread reference chain")
    print(f"    list_unsubscribe — newsletter detection header")
    print(f"\n  Next step: run 'nomolo clean gmail' for RAG-optimized processing.")
    print()

    logger.removeHandler(file_handler)
    file_handler.close()
