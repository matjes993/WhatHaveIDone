"""
NOMOLO Google Contacts Collector
Comprehensive export of Google Contacts to a local JSONL vault.
Captures the full contact record: names, communication channels,
addresses, social profiles, relationships, dates, notes, photos, and more.

Uses contacts.readonly scope — your contacts are never modified.
"""

import os
import sys
import time
import logging

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Suppress "file_cache is only supported with oauth2client<4.0.0" warning
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)

from core.auth import get_google_credentials
from core.vault import flush_entries, load_processed_ids, append_processed_ids

logger = logging.getLogger("nomolo.contacts")

SCOPES = ["https://www.googleapis.com/auth/contacts.readonly"]

# Request every available field from the People API
PERSON_FIELDS = ",".join([
    "addresses",
    "ageRanges",
    "biographies",
    "birthdays",
    "calendarUrls",
    "clientData",
    "coverPhotos",
    "emailAddresses",
    "events",
    "externalIds",
    "genders",
    "imClients",
    "interests",
    "locales",
    "locations",
    "memberships",
    "metadata",
    "miscKeywords",
    "names",
    "nicknames",
    "occupations",
    "organizations",
    "phoneNumbers",
    "photos",
    "relations",
    "sipAddresses",
    "skills",
    "urls",
    "userDefined",
])


def get_credentials(credentials_file, token_file, scopes):
    """Wrap core.auth.get_google_credentials for use by setup."""
    return get_google_credentials(credentials_file, token_file, scopes)


# ---------------------------------------------------------------------------
# Field extractors — each handles one People API field gracefully
# ---------------------------------------------------------------------------

def _extract_names(person):
    """Extract all name parts from the primary name."""
    names = person.get("names", [])
    if not names:
        return {
            "display": "",
            "given": "",
            "family": "",
            "middle": "",
            "prefix": "",
            "suffix": "",
            "phonetic_given": "",
            "phonetic_family": "",
        }
    primary = names[0]
    return {
        "display": primary.get("displayName", ""),
        "given": primary.get("givenName", ""),
        "family": primary.get("familyName", ""),
        "middle": primary.get("middleName", ""),
        "prefix": primary.get("honorificPrefix", ""),
        "suffix": primary.get("honorificSuffix", ""),
        "phonetic_given": primary.get("phoneticGivenName", ""),
        "phonetic_family": primary.get("phoneticFamilyName", ""),
    }


def _extract_nicknames(person):
    """Extract nicknames."""
    return [
        {"value": n.get("value", ""), "type": n.get("type", "")}
        for n in person.get("nicknames", [])
        if n.get("value")
    ]


def _extract_emails(person):
    """Extract email addresses with type and metadata."""
    return [
        {
            "value": e.get("value", "").lower(),
            "type": e.get("type", ""),
            "display_name": e.get("displayName", ""),
            "primary": _is_primary(e),
        }
        for e in person.get("emailAddresses", [])
        if e.get("value")
    ]


def _extract_phones(person):
    """Extract phone numbers with type."""
    return [
        {
            "value": p.get("value", ""),
            "type": p.get("type", ""),
            "canonical": p.get("canonicalForm", ""),
            "primary": _is_primary(p),
        }
        for p in person.get("phoneNumbers", [])
        if p.get("value")
    ]


def _extract_addresses(person):
    """Extract physical addresses."""
    return [
        {
            "type": a.get("type", ""),
            "formatted": a.get("formattedValue", ""),
            "street": a.get("streetAddress", ""),
            "city": a.get("city", ""),
            "region": a.get("region", ""),
            "postal_code": a.get("postalCode", ""),
            "country": a.get("country", ""),
            "country_code": a.get("countryCode", ""),
            "primary": _is_primary(a),
        }
        for a in person.get("addresses", [])
        if a.get("formattedValue") or a.get("city") or a.get("streetAddress")
    ]


def _extract_organizations(person):
    """Extract all organizations (not just the first)."""
    return [
        {
            "name": o.get("name", ""),
            "title": o.get("title", ""),
            "department": o.get("department", ""),
            "type": o.get("type", ""),
            "job_description": o.get("jobDescription", ""),
            "start_date": _date_to_str(o.get("startDate")),
            "end_date": _date_to_str(o.get("endDate")),
            "current": o.get("current", False),
            "primary": _is_primary(o),
        }
        for o in person.get("organizations", [])
        if o.get("name") or o.get("title")
    ]


def _extract_birthdays(person):
    """Extract birthday and other dates."""
    results = []
    for b in person.get("birthdays", []):
        date_obj = b.get("date", {})
        if date_obj:
            results.append({
                "year": date_obj.get("year", 0),
                "month": date_obj.get("month", 0),
                "day": date_obj.get("day", 0),
                "text": b.get("text", ""),
            })
    return results


def _extract_events(person):
    """Extract events (anniversaries, custom dates)."""
    return [
        {
            "type": e.get("type", ""),
            "date": _date_to_str(e.get("date")),
            "formatted_type": e.get("formattedType", ""),
        }
        for e in person.get("events", [])
        if e.get("date")
    ]


def _extract_urls(person):
    """Extract websites and profile URLs."""
    return [
        {
            "value": u.get("value", ""),
            "type": u.get("type", ""),
            "formatted_type": u.get("formattedType", ""),
        }
        for u in person.get("urls", [])
        if u.get("value")
    ]


def _extract_im_clients(person):
    """Extract instant messaging handles (WhatsApp, Telegram, Signal, etc.)."""
    return [
        {
            "username": im.get("username", ""),
            "protocol": im.get("protocol", ""),
            "type": im.get("type", ""),
            "formatted_protocol": im.get("formattedProtocol", ""),
        }
        for im in person.get("imClients", [])
        if im.get("username")
    ]


def _extract_external_ids(person):
    """Extract external IDs (social media handles, account IDs)."""
    return [
        {
            "value": eid.get("value", ""),
            "type": eid.get("type", ""),
            "formatted_type": eid.get("formattedType", ""),
        }
        for eid in person.get("externalIds", [])
        if eid.get("value")
    ]


def _extract_relations(person):
    """Extract relationships (spouse, parent, child, sibling, etc.)."""
    return [
        {
            "person": r.get("person", ""),
            "type": r.get("type", ""),
            "formatted_type": r.get("formattedType", ""),
        }
        for r in person.get("relations", [])
        if r.get("person")
    ]


def _extract_biographies(person):
    """Extract notes/biographies."""
    return [
        {
            "value": b.get("value", ""),
            "content_type": b.get("contentType", "TEXT_PLAIN"),
        }
        for b in person.get("biographies", [])
        if b.get("value")
    ]


def _extract_photos(person):
    """Extract photo URLs."""
    return [
        {
            "url": p.get("url", ""),
            "default": p.get("default", False),
        }
        for p in person.get("photos", [])
        if p.get("url")
    ]


def _extract_memberships(person):
    """Extract contact group memberships (labels)."""
    results = []
    for m in person.get("memberships", []):
        group = m.get("contactGroupMembership", {})
        if group:
            results.append({
                "group_id": group.get("contactGroupId", ""),
                "group_resource": group.get("contactGroupResourceName", ""),
            })
    return results


def _extract_interests(person):
    """Extract interests/hobbies."""
    return [i.get("value", "") for i in person.get("interests", []) if i.get("value")]


def _extract_skills(person):
    """Extract professional skills."""
    return [s.get("value", "") for s in person.get("skills", []) if s.get("value")]


def _extract_occupations(person):
    """Extract occupations."""
    return [o.get("value", "") for o in person.get("occupations", []) if o.get("value")]


def _extract_locations(person):
    """Extract current locations."""
    return [
        {
            "value": loc.get("value", ""),
            "type": loc.get("type", ""),
            "building_id": loc.get("buildingId", ""),
            "floor": loc.get("floor", ""),
            "desk_code": loc.get("deskCode", ""),
        }
        for loc in person.get("locations", [])
        if loc.get("value")
    ]


def _extract_sip_addresses(person):
    """Extract SIP addresses."""
    return [
        {"value": s.get("value", ""), "type": s.get("type", "")}
        for s in person.get("sipAddresses", [])
        if s.get("value")
    ]


def _extract_calendar_urls(person):
    """Extract calendar URLs."""
    return [
        {"url": c.get("url", ""), "type": c.get("type", "")}
        for c in person.get("calendarUrls", [])
        if c.get("url")
    ]


def _extract_user_defined(person):
    """Extract user-defined custom fields."""
    return [
        {"key": u.get("key", ""), "value": u.get("value", "")}
        for u in person.get("userDefined", [])
        if u.get("key") or u.get("value")
    ]


def _extract_genders(person):
    """Extract gender information."""
    return [
        {"value": g.get("value", ""), "formatted": g.get("formattedValue", "")}
        for g in person.get("genders", [])
        if g.get("value")
    ]


def _extract_locales(person):
    """Extract preferred locales/languages."""
    return [loc.get("value", "") for loc in person.get("locales", []) if loc.get("value")]


def _extract_age_ranges(person):
    """Extract age range."""
    return [a.get("ageRange", "") for a in person.get("ageRanges", []) if a.get("ageRange")]


def _extract_misc_keywords(person):
    """Extract misc keywords."""
    return [
        {"value": k.get("value", ""), "type": k.get("type", "")}
        for k in person.get("miscKeywords", [])
        if k.get("value")
    ]


def _extract_client_data(person):
    """Extract client-specific data."""
    return [
        {"key": c.get("key", ""), "value": c.get("value", "")}
        for c in person.get("clientData", [])
        if c.get("key") or c.get("value")
    ]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_primary(field_obj):
    """Check if a field is marked as primary."""
    metadata = field_obj.get("metadata", {})
    return metadata.get("primary", False) or metadata.get("sourcePrimary", False)


def _date_to_str(date_obj):
    """Convert a People API date object {year, month, day} to a string."""
    if not date_obj:
        return ""
    year = date_obj.get("year", 0)
    month = date_obj.get("month", 0)
    day = date_obj.get("day", 0)
    if year and month and day:
        return f"{year:04d}-{month:02d}-{day:02d}"
    if month and day:
        return f"--{month:02d}-{day:02d}"
    return ""


# ---------------------------------------------------------------------------
# Main entry builder
# ---------------------------------------------------------------------------

def _contact_to_entry(person):
    """Convert a People API person resource into a comprehensive vault entry."""
    resource_name = person.get("resourceName", "")

    # Metadata
    metadata = person.get("metadata", {})
    sources = metadata.get("sources", [])
    updated_at = ""
    created_at = ""
    if sources:
        updated_at = sources[0].get("updateTime", "")
        # The first source often has the creation info
        for src in sources:
            if src.get("type") == "CONTACT":
                updated_at = src.get("updateTime", updated_at)

    name_data = _extract_names(person)

    # Build a rich embedding text for RAG
    display = name_data["display"]
    orgs = _extract_organizations(person)
    emails = _extract_emails(person)
    primary_org = orgs[0]["name"] if orgs else ""
    primary_title = orgs[0]["title"] if orgs else ""
    primary_email = emails[0]["value"] if emails else ""
    bio = _extract_biographies(person)
    bio_text = bio[0]["value"] if bio else ""

    embedding_parts = [display]
    if primary_title and primary_org:
        embedding_parts.append(f"{primary_title} at {primary_org}")
    elif primary_org:
        embedding_parts.append(primary_org)
    if primary_email:
        embedding_parts.append(primary_email)
    if bio_text:
        embedding_parts.append(bio_text[:200])

    return {
        "id": f"contacts:google:{resource_name}",
        "sources": ["google"],
        "source_id": resource_name,

        # Identity
        "name": name_data,
        "nicknames": _extract_nicknames(person),
        "photos": _extract_photos(person),

        # Communication
        "emails": emails,
        "phones": _extract_phones(person),
        "addresses": _extract_addresses(person),
        "im_clients": _extract_im_clients(person),
        "sip_addresses": _extract_sip_addresses(person),
        "calendar_urls": _extract_calendar_urls(person),

        # Professional
        "organizations": orgs,
        "occupations": _extract_occupations(person),
        "skills": _extract_skills(person),

        # Social
        "urls": _extract_urls(person),
        "external_ids": _extract_external_ids(person),
        "relations": _extract_relations(person),
        "biographies": bio,

        # Dates
        "birthdays": _extract_birthdays(person),
        "events": _extract_events(person),

        # Demographics
        "genders": _extract_genders(person),
        "age_ranges": _extract_age_ranges(person),
        "locales": _extract_locales(person),

        # Personal
        "interests": _extract_interests(person),
        "locations": _extract_locations(person),

        # Organization / Metadata
        "memberships": _extract_memberships(person),
        "user_defined": _extract_user_defined(person),
        "client_data": _extract_client_data(person),
        "misc_keywords": _extract_misc_keywords(person),

        # Timestamps
        "updated_at": updated_at,

        # RAG embedding text
        "contact_for_embedding": " — ".join(embedding_parts),
    }


def _print_progress(fetched, start_time):
    """Print inline progress."""
    elapsed = time.time() - start_time
    rate = fetched / elapsed if elapsed > 0 else 0
    print(
        f"\r  Fetching contacts... {fetched:,} ({rate:.0f}/s)",
        end="", flush=True,
    )


def run_export(config=None):
    """Main entry point: fetch all Google Contacts and save to vault."""
    config = config or {}
    contacts_config = config.get("contacts", {})
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    vault_root_base = config.get("vault_root", os.path.join(project_root, "vaults"))
    vault_root_base = os.path.expanduser(vault_root_base)
    if not os.path.isabs(vault_root_base):
        vault_root_base = os.path.join(project_root, vault_root_base)
    vault_root = os.path.join(vault_root_base, "Contacts")

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

    print(f"\n  NOMOLO Google Contacts Collector")
    print(f"  {'=' * 45}")
    print(f"  Vault: {vault_root}")

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
        print(f"  Already vaulted: {len(processed_ids):,} contacts")

    # Fetch all contacts with pagination
    all_contacts = []
    page_token = None
    start_time = time.time()

    print()

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

        _print_progress(len(all_contacts), start_time)

        page_token = results.get("nextPageToken")
        if not page_token:
            break

    elapsed = time.time() - start_time
    print(f"\r  Fetched {len(all_contacts):,} contacts in {elapsed:.1f}s        ")

    # Also fetch contact groups for label resolution
    groups = {}
    try:
        group_results = service.contactGroups().list(pageSize=1000).execute()
        for g in group_results.get("contactGroups", []):
            groups[g.get("resourceName", "")] = g.get("name", "")
        if groups:
            logger.info("Loaded %d contact groups", len(groups))
    except Exception as e:
        logger.warning("Could not fetch contact groups: %s", e)

    # Filter out already-processed contacts
    new_contacts = []
    for person in all_contacts:
        resource_name = person.get("resourceName", "")
        entry_id = f"contacts:google:{resource_name}"
        if entry_id not in processed_ids:
            new_contacts.append(person)

    if not new_contacts:
        print("  Nothing new — vault is up to date.")
        return

    print(f"  Processing {len(new_contacts):,} new contacts...")

    # Convert to entries
    entries = []
    skipped = 0
    for person in new_contacts:
        try:
            entry = _contact_to_entry(person)

            # Resolve group names if available
            if groups and entry.get("memberships"):
                for m in entry["memberships"]:
                    group_resource = m.get("group_resource", "")
                    if group_resource in groups:
                        m["group_name"] = groups[group_resource]

            entries.append(entry)
        except Exception as e:
            resource_name = person.get("resourceName", "unknown")
            logger.warning("Skipping contact %s: %s", resource_name, e)
            skipped += 1

    # Flush to vault
    if entries:
        flush_entries(entries, vault_root, "contacts.jsonl")
        new_ids = [e["id"] for e in entries]
        append_processed_ids(vault_root, new_ids)

    # Summary stats
    total_emails = sum(len(e.get("emails", [])) for e in entries)
    total_phones = sum(len(e.get("phones", [])) for e in entries)
    total_addresses = sum(len(e.get("addresses", [])) for e in entries)
    total_orgs = sum(len(e.get("organizations", [])) for e in entries)
    total_bdays = sum(len(e.get("birthdays", [])) for e in entries)
    total_urls = sum(len(e.get("urls", [])) for e in entries)
    total_relations = sum(len(e.get("relations", [])) for e in entries)
    total_im = sum(len(e.get("im_clients", [])) for e in entries)
    total_photos = sum(len(e.get("photos", [])) for e in entries)
    with_bio = sum(1 for e in entries if e.get("biographies"))

    print()
    print(f"  {'=' * 45}")
    print(f"  Done! {len(entries):,} contacts saved")
    print(f"  {'=' * 45}")
    print(f"    Emails:        {total_emails:,}")
    print(f"    Phones:        {total_phones:,}")
    print(f"    Addresses:     {total_addresses:,}")
    print(f"    Organizations: {total_orgs:,}")
    print(f"    Birthdays:     {total_bdays:,}")
    print(f"    Websites:      {total_urls:,}")
    print(f"    Relationships: {total_relations:,}")
    print(f"    IM handles:    {total_im:,}")
    print(f"    Photos:        {total_photos:,}")
    print(f"    With bio/notes:{with_bio:,}")
    if skipped:
        print(f"    Skipped:       {skipped}")
    print()
