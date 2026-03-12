"""
WHID Google Contacts Collector
Exports Google Contacts to a local JSONL vault as a flat file.
Uses contacts.readonly scope — your contacts are never modified.
"""

import os
import sys
import logging

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from core.auth import get_google_credentials
from core.vault import flush_entries, load_processed_ids, append_processed_ids

logger = logging.getLogger("whid.contacts")

SCOPES = ["https://www.googleapis.com/auth/contacts.readonly"]
PERSON_FIELDS = (
    "names,emailAddresses,phoneNumbers,"
    "organizations,biographies,urls,memberships"
)


def get_credentials(credentials_file, token_file, scopes):
    """Wrap core.auth.get_google_credentials for use by setup."""
    return get_google_credentials(credentials_file, token_file, scopes)


def _contact_to_entry(person):
    """Convert a People API person resource into a vault entry."""
    resource_name = person.get("resourceName", "")

    # Name fields
    names = person.get("names", [{}])
    primary_name = names[0] if names else {}
    display_name = primary_name.get("displayName", "")
    given_name = primary_name.get("givenName", "")
    family_name = primary_name.get("familyName", "")

    # Emails
    emails = [
        e.get("value", "")
        for e in person.get("emailAddresses", [])
        if e.get("value")
    ]

    # Phones
    phones = [
        p.get("value", "")
        for p in person.get("phoneNumbers", [])
        if p.get("value")
    ]

    # Organization and title
    orgs = person.get("organizations", [{}])
    primary_org = orgs[0] if orgs else {}
    organization = primary_org.get("name", "")
    title = primary_org.get("title", "")

    # Updated timestamp from metadata
    metadata = person.get("metadata", {})
    sources = metadata.get("sources", [{}])
    updated_at = ""
    if sources:
        updated_at = sources[0].get("updateTime", "")

    return {
        "id": f"contacts:google:{resource_name}",
        "source": "google",
        "source_id": resource_name,
        "name": {
            "display": display_name,
            "given": given_name,
            "family": family_name,
        },
        "emails": emails,
        "phones": phones,
        "organization": organization,
        "title": title,
        "updated_at": updated_at,
    }


def run_export(config=None):
    """Main entry point: fetch all Google Contacts and save to vault."""
    config = config or {}
    contacts_config = config.get("contacts", {})
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    vault_root_base = config.get("vault_root", os.path.join(project_root, "vaults"))
    vault_root_base = os.path.expanduser(vault_root_base)
    if not os.path.isabs(vault_root_base):
        vault_root_base = os.path.join(project_root, vault_root_base)
    vault_root = os.path.join(vault_root_base, "Contacts_Google")

    credentials_file = contacts_config.get("credentials_file", "credentials.json")
    token_file = contacts_config.get("token_file", "token_contacts.json")
    scopes = [contacts_config.get("scope", SCOPES[0])]

    try:
        os.makedirs(vault_root, exist_ok=True)
    except PermissionError:
        print(f"\nError: Permission denied creating vault directory: {vault_root}")
        sys.exit(1)
    except OSError as e:
        print(f"\nError: Cannot create vault directory {vault_root}: {e}")
        sys.exit(1)

    print(f"\nSaving to: {vault_root}")

    # Authenticate
    try:
        creds = get_credentials(credentials_file, token_file, scopes)
    except FileNotFoundError as e:
        print(f"\n{e}")
        sys.exit(1)

    # Build People API service
    try:
        service = build("people", "v1", credentials=creds)
    except Exception as e:
        print(f"\nError: Could not connect to People API: {e}")
        print("Check your internet connection and try again.")
        sys.exit(1)

    # Load already-processed IDs
    processed_ids = load_processed_ids(vault_root)
    if processed_ids:
        print(f"  + Already vaulted: {len(processed_ids):,} contacts")

    # Fetch all contacts with pagination
    all_contacts = []
    page_token = None

    print("  Fetching contacts...", end="", flush=True)

    while True:
        try:
            kwargs = {
                "resourceName": "people/me",
                "pageSize": 1000,
                "personFields": PERSON_FIELDS,
            }
            if page_token:
                kwargs["pageToken"] = page_token

            results = (
                service.people()
                .connections()
                .list(**kwargs)
                .execute()
            )
        except HttpError as e:
            status = e.resp.status
            if status == 403:
                print(
                    "\n\nError: People API access denied (403).\n"
                    "Make sure the People API is enabled in Google Cloud Console\n"
                    "and your OAuth token has the contacts.readonly scope.\n"
                    "Try deleting token_contacts.json and re-authorizing."
                )
            elif status == 429:
                print(
                    "\n\nError: Rate limited by People API.\n"
                    "Wait a few minutes and try again."
                )
            elif status == 401:
                print(
                    "\n\nError: Authentication failed.\n"
                    "Delete token_contacts.json and re-authorize."
                )
            else:
                print(f"\n\nPeople API error (HTTP {status}): {e}")
            sys.exit(1)
        except Exception as e:
            print(f"\n\nError fetching contacts: {e}")
            sys.exit(1)

        connections = results.get("connections", [])
        all_contacts.extend(connections)

        # Progress update (single-line)
        print(f" {len(all_contacts):,}...", end="", flush=True)

        page_token = results.get("nextPageToken")
        if not page_token:
            break

    print(f" found {len(all_contacts):,} total")

    # Filter out already-processed contacts
    new_contacts = []
    for person in all_contacts:
        resource_name = person.get("resourceName", "")
        entry_id = f"contacts:google:{resource_name}"
        if entry_id not in processed_ids:
            new_contacts.append(person)

    if not new_contacts:
        print("  + Nothing new — vault is up to date.")
        return

    print(f"  + Processing {len(new_contacts):,} new contacts...")

    # Convert to entries
    entries = []
    for person in new_contacts:
        try:
            entry = _contact_to_entry(person)
            entries.append(entry)
        except Exception as e:
            resource_name = person.get("resourceName", "unknown")
            logger.warning("Skipping contact %s: %s", resource_name, e)

    # Flush to vault
    if entries:
        flush_entries(entries, vault_root, "contacts.jsonl")
        new_ids = [e["id"] for e in entries]
        append_processed_ids(vault_root, new_ids)

    print()
    print("  " + "=" * 45)
    print(f"    Done! {len(entries):,} contacts saved to {vault_root}")
    print("  " + "=" * 45)
