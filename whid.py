#!/usr/bin/env python3
"""
WHID CLI — WhatHaveIDone command-line interface.

Usage:
    whid setup gmail              Guided setup for Gmail
    whid setup contacts-google    Guided setup for Google Contacts
    whid collect gmail            Export your Gmail (incremental)
    whid collect gmail --full     Full rescan (find missing messages)
    whid collect contacts-google  Export your Google Contacts
    whid collect contacts-linkedin <file>   Import LinkedIn CSV export
    whid collect contacts-facebook <file>   Import Facebook JSON export
    whid collect contacts-instagram <dir>   Import Instagram JSON export
    whid enrich gmail             Backfill metadata from Gmail API
    whid clean gmail              RAG-optimized cleaning pass
    whid groom gmail              Deduplicate and sort
    whid vectorize                Vectorize all vaults for semantic search
    whid vectorize gmail          Vectorize Gmail only
    whid search 'query'           Search your data (no LLM needed)
    whid search 'query' -s gmail  Search Gmail only
    whid status                   See your vaults
    whid update                   Pull latest version from GitHub
"""

import argparse
import glob
import json
import logging
import os
import platform
import shutil
import subprocess
import sys
import webbrowser

import yaml

CONFIG_LOCATIONS = [
    os.path.join(os.getcwd(), "config.yaml"),
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml"),
    os.path.expanduser("~/.config/whid/config.yaml"),
]

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

logger = logging.getLogger("whid")


def load_config():
    for path in CONFIG_LOCATIONS:
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    config = yaml.safe_load(f)
                logger.info("Using config: %s", path)
                if config is None:
                    logger.warning("Config file %s is empty — using defaults.", path)
                    return {}
                return config
            except yaml.YAMLError as e:
                print(f"\nError: config.yaml has invalid YAML syntax.\n")
                print(f"  File: {path}")
                if hasattr(e, "problem_mark"):
                    mark = e.problem_mark
                    print(f"  Line: {mark.line + 1}, Column: {mark.column + 1}")
                print(f"  Details: {e}\n")
                print("Fix the syntax error and try again.")
                print("Tip: use https://yamlchecker.com to validate your config.")
                sys.exit(1)
            except PermissionError:
                print(f"\nError: Permission denied reading {path}")
                print(f"Fix with: chmod 644 {path}")
                sys.exit(1)

    logger.info("No config.yaml found — using defaults.")
    return {}


def get_vault_root(config):
    default_vault = os.path.join(PROJECT_ROOT, "vaults")
    vault_root = config.get("vault_root", default_vault)
    vault_root = os.path.expanduser(vault_root)
    if not os.path.isabs(vault_root):
        vault_root = os.path.join(PROJECT_ROOT, vault_root)

    parent = os.path.dirname(vault_root)
    if not os.path.exists(parent):
        print(f"\nError: Vault root parent directory does not exist: {parent}")
        print(f"Check vault_root in config.yaml: {config.get('vault_root', 'vaults')}")
        if "/Volumes/" in parent:
            print("Hint: Is your external drive connected?")
        sys.exit(1)

    return vault_root


def validate_gmail_config(config):
    """Validate Gmail-specific config values and return cleaned config."""
    gmail = config.get("gmail", {})

    max_workers = gmail.get("max_workers", 10)
    if not isinstance(max_workers, int) or max_workers < 1:
        print(f"\nError: gmail.max_workers must be a positive integer, got: {max_workers}")
        sys.exit(1)
    if max_workers > 20:
        logger.warning("gmail.max_workers=%d is very high — you may hit Google API rate limits.", max_workers)

    batch_size = gmail.get("batch_size", 100)
    if not isinstance(batch_size, int) or batch_size < 1:
        print(f"\nError: gmail.batch_size must be a positive integer, got: {batch_size}")
        sys.exit(1)
    if batch_size > 100:
        print(f"\nError: gmail.batch_size cannot exceed 100 (Gmail API limit), got: {batch_size}")
        print("Set batch_size to 100 or lower in config.yaml.")
        sys.exit(1)

    return config


def _find_credentials_file():
    """Search common locations for a Google OAuth credentials file."""
    search_paths = [
        os.path.join(PROJECT_ROOT, "credentials.json"),
        os.path.join(PROJECT_ROOT, "client_secret.json"),
        os.path.expanduser("~/Downloads/credentials.json"),
        os.path.expanduser("~/Downloads/client_secret.json"),
        os.path.expanduser("~/Desktop/credentials.json"),
        os.path.expanduser("~/Desktop/client_secret.json"),
    ]

    # Also check for client_secret_*.json in Downloads and Desktop (Google's default name)
    for folder in ["~/Downloads", "~/Desktop"]:
        folder_path = os.path.expanduser(folder)
        if os.path.exists(folder_path):
            search_paths.extend(
                sorted(
                    glob.glob(os.path.join(folder_path, "client_secret*.json")),
                    key=os.path.getmtime,
                    reverse=True,
                )
            )

    for path in search_paths:
        if os.path.exists(path):
            # Validate it looks like a Google credentials file
            try:
                with open(path) as f:
                    data = json.load(f)
                if "installed" in data or "web" in data:
                    return path
            except (json.JSONDecodeError, KeyError, PermissionError, OSError):
                continue

    return None


def _prompt(message, default=None):
    """Prompt user for input with optional default."""
    if default:
        result = input(f"{message} [{default}]: ").strip()
        return result if result else default
    return input(f"{message}: ").strip()


def _setup_google_credentials(config):
    """Shared credential setup for any Google API collector. Returns path to credentials.json."""
    target = os.path.join(PROJECT_ROOT, "credentials.json")

    if os.path.exists(target):
        print(f"\nCredentials found: {target}")
        return target

    print("\nStep 1: Get Google OAuth credentials")
    print("-" * 40)

    found = _find_credentials_file()
    if found:
        print(f"\nFound credentials file: {found}")
        answer = _prompt("Use this file? (y/n)", "y")
        if answer.lower() in ("y", "yes", ""):
            shutil.copy2(found, target)
            print(f"Copied to {target}")
            return target
        found = None

    print("\nI'll open Google Cloud Console in your browser.")
    print("Follow these steps:\n")
    print("  1. Create a project (or select existing)")
    print("  2. Enable the API you need (Gmail API, People API, etc.)")
    print("  3. Go to Credentials > Create > OAuth Client ID > Desktop App")
    print("  4. Download the JSON file")
    print()
    print("  The file will be named 'client_secret*.json'")

    input("\nPress Enter to open Google Cloud Console...")
    webbrowser.open("https://console.cloud.google.com/apis/credentials")

    print("\nAfter downloading, I'll search your Downloads folder for")
    print("'client_secret_*.json' or 'credentials.json' automatically.")
    input("Press Enter when you've downloaded the credentials file...")

    found = _find_credentials_file()
    if found:
        print(f"\nFound: {found}")
        shutil.copy2(found, target)
        print(f"Copied to {target}")
        return target

    print("\nCouldn't find the credentials file automatically.")
    print("(On macOS, you may need to grant Terminal access to Downloads")
    print(" in System Settings > Privacy & Security > Files and Folders)\n")

    while True:
        path = _prompt("Drag the file here or paste its full path")
        path = path.strip("'\" ")
        path = os.path.expanduser(path)

        if not path:
            continue

        if os.path.isdir(path):
            candidates = glob.glob(os.path.join(path, "client_secret*.json"))
            candidates += glob.glob(os.path.join(path, "credentials.json"))
            for c in sorted(candidates, key=os.path.getmtime, reverse=True):
                try:
                    with open(c) as f:
                        data = json.load(f)
                    if "installed" in data or "web" in data:
                        path = c
                        break
                except (json.JSONDecodeError, PermissionError, OSError):
                    continue
            else:
                print(f"No credentials file found in {path}")
                print("Look for 'client_secret*.json' or 'credentials.json'\n")
                continue

        if not os.path.exists(path):
            print(f"File not found: {path}\n")
            continue

        if not os.path.isfile(path):
            print(f"Not a file: {path}\n")
            continue

        try:
            with open(path) as f:
                data = json.load(f)
            if "installed" not in data and "web" not in data:
                print("This doesn't look like a Google OAuth credentials file.")
                print("Make sure you downloaded the OAuth Client ID JSON.\n")
                continue
        except (json.JSONDecodeError, PermissionError) as e:
            print(f"Can't read file: {e}\n")
            continue

        shutil.copy2(path, target)
        print(f"Copied to {target}")
        return target

    return None


def cmd_setup(args, config):
    """Guided setup for a collector."""
    if args.source == "gmail":
        _setup_gmail(args, config)
    elif args.source == "contacts-google":
        _setup_contacts_google(args, config)
    else:
        print(f"Setup not available for '{args.source}' yet.")
        print("Available: gmail, contacts-google")
        sys.exit(1)


def _setup_gmail(args, config):
    """Guided Gmail setup."""
    token = os.path.join(PROJECT_ROOT, "token.json")

    print("\n" + "=" * 50)
    print("  WHID Gmail Setup")
    print("=" * 50)

    target = os.path.join(PROJECT_ROOT, "credentials.json")

    # Check if already set up
    if os.path.exists(target) and os.path.exists(token):
        print("\nGmail is already set up!")
        print(f"  Credentials: {target}")
        print(f"  Token: {token}")
        print("\nRun 'whid collect gmail' to start downloading.")
        return

    _setup_google_credentials(config)

    if not os.path.exists(target):
        print("\nError: credentials.json still not found. Setup incomplete.")
        sys.exit(1)

    print("\nStep 2: Sign in with Google")
    print("-" * 40)
    print("\nA browser window will open for Google sign-in.")
    print("Sign in with the Gmail account you want to export.")
    print("(WHID uses read-only access — it cannot modify your email)\n")

    input("Press Enter to open the sign-in page...")

    try:
        from collectors.gmail_collector import get_credentials

        scopes = [config.get("gmail", {}).get(
            "scope", "https://www.googleapis.com/auth/gmail.readonly"
        )]
        creds = get_credentials(target, token, scopes)
    except Exception as e:
        print(f"\nAuthentication failed: {e}")
        print("Try running 'whid setup gmail' again.")
        sys.exit(1)

    # Test connection
    print("\nStep 3: Testing connection")
    print("-" * 40)

    try:
        from googleapiclient.discovery import build

        service = build("gmail", "v1", credentials=creds)
        profile = service.users().getProfile(userId="me").execute()
        email = profile.get("emailAddress", "unknown")
        total = int(profile.get("messagesTotal", 0))

        print(f"\n  Connected to: {email}")
        print(f"  Total messages: {total:,}")
    except Exception as e:
        print(f"\n  Connection test failed: {e}")
        print("  But credentials are saved — try 'whid collect gmail' anyway.")

    print("\n" + "=" * 50)
    print("  Setup complete!")
    print("=" * 50)
    print(f"\n  Run:  whid collect gmail")
    print(f"  Data: {os.path.join(PROJECT_ROOT, 'vaults', 'Gmail_Primary')}/")
    print()


def _setup_contacts_google(args, config):
    """Guided Google Contacts setup."""
    contacts_cfg = config.get("contacts", {})
    token = os.path.join(
        PROJECT_ROOT, contacts_cfg.get("token_file", "token_contacts.json")
    )

    print("\n" + "=" * 50)
    print("  WHID Google Contacts Setup")
    print("=" * 50)

    target = os.path.join(PROJECT_ROOT, "credentials.json")

    # Check if already set up
    if os.path.exists(target) and os.path.exists(token):
        print("\nGoogle Contacts is already set up!")
        print(f"  Credentials: {target}")
        print(f"  Token: {token}")
        print("\nRun 'whid collect contacts-google' to start downloading.")
        return

    _setup_google_credentials(config)

    if not os.path.exists(target):
        print("\nError: credentials.json still not found. Setup incomplete.")
        sys.exit(1)

    print("\nStep 2: Enable the People API")
    print("-" * 40)
    print("\nMake sure the People API is enabled in your Google Cloud project:")
    print("  1. Go to APIs & Services > Library")
    print("  2. Search for 'People API'")
    print("  3. Click Enable (if not already enabled)")

    input("\nPress Enter to open the API Library...")
    webbrowser.open("https://console.cloud.google.com/apis/library/people.googleapis.com")
    input("Press Enter when the People API is enabled...")

    print("\nStep 3: Sign in with Google")
    print("-" * 40)
    print("\nA browser window will open for Google sign-in.")
    print("Sign in with the account whose contacts you want to export.")
    print("(WHID uses read-only access — it cannot modify your contacts)\n")

    input("Press Enter to open the sign-in page...")

    try:
        from core.auth import get_google_credentials

        scopes = [contacts_cfg.get(
            "scope", "https://www.googleapis.com/auth/contacts.readonly"
        )]
        creds = get_google_credentials(target, token, scopes)
    except Exception as e:
        print(f"\nAuthentication failed: {e}")
        print("Try running 'whid setup contacts-google' again.")
        sys.exit(1)

    # Test connection
    print("\nStep 4: Testing connection")
    print("-" * 40)

    try:
        from googleapiclient.discovery import build

        service = build("people", "v1", credentials=creds)
        results = service.people().connections().list(
            resourceName="people/me",
            pageSize=1,
            personFields="names",
        ).execute()
        total = results.get("totalPeople", results.get("totalItems", "unknown"))

        print(f"\n  Connected! Total contacts: {total}")
    except Exception as e:
        print(f"\n  Connection test failed: {e}")
        print("  But credentials are saved — try 'whid collect contacts-google' anyway.")

    print("\n" + "=" * 50)
    print("  Setup complete!")
    print("=" * 50)
    print(f"\n  Run:  whid collect contacts-google")
    print(f"  Data: {os.path.join(PROJECT_ROOT, 'vaults', 'Contacts')}/")
    print()


def cmd_collect(args, config):
    """Run a collector."""
    if args.source == "gmail":
        config = validate_gmail_config(config)

        creds_file = config.get("gmail", {}).get("credentials_file", "credentials.json")
        if not os.path.exists(creds_file):
            print("\nGmail is not set up yet. Run:\n")
            print("  whid setup gmail\n")
            sys.exit(1)

        try:
            from collectors.gmail_collector import run_export
        except ImportError as e:
            print(f"\nError: Failed to load Gmail collector: {e}")
            print("Try reinstalling: pip install -e .")
            sys.exit(1)

        run_export(vault_name=args.vault, config=config, full_scan=args.full)

    elif args.source == "contacts-google":
        creds_file = config.get("contacts", {}).get("credentials_file", "credentials.json")
        if not os.path.exists(creds_file):
            print("\nGoogle Contacts is not set up yet. Run:\n")
            print("  whid setup contacts-google\n")
            sys.exit(1)

        try:
            from collectors.google_contacts import run_export
        except ImportError as e:
            print(f"\nError: Failed to load Google Contacts collector: {e}")
            print("Try reinstalling: pip install -e .")
            sys.exit(1)

        run_export(config=config)

    elif args.source == "contacts-linkedin":
        if not args.vault or args.vault == "Primary":
            print("\nUsage: whid collect contacts-linkedin <path-to-csv-or-export-dir>")
            print("\nExport your LinkedIn data:")
            print("  1. Go to linkedin.com > Settings > Data Privacy")
            print("  2. Get a copy of your data (select all or at least Connections)")
            print("  3. Download and unzip the archive")
            print("  4. Run: whid collect contacts-linkedin ~/Downloads/linkedin-export/")
            print("     or:  whid collect contacts-linkedin ~/Downloads/Connections.csv")
            sys.exit(1)

        export_path = os.path.expanduser(args.vault)
        if not os.path.exists(export_path):
            print(f"\nPath not found: {export_path}")
            sys.exit(1)

        from collectors.linkedin_contacts import run_import
        run_import(export_path=export_path, config=config)

    elif args.source == "contacts-facebook":
        if not args.vault or args.vault == "Primary":
            print("\nUsage: whid collect contacts-facebook <path-to-json-or-dir>")
            print("\nExport your Facebook data:")
            print("  1. Go to facebook.com > Settings > Your Information")
            print("  2. Download Your Information > select JSON format")
            print("  3. Download and unzip")
            print("  4. Run: whid collect contacts-facebook ~/Downloads/facebook-export/")
            sys.exit(1)

        export_path = os.path.expanduser(args.vault)
        if not os.path.exists(export_path):
            print(f"\nPath not found: {export_path}")
            sys.exit(1)

        from collectors.facebook_contacts import run_import
        run_import(export_path=export_path, config=config)

    elif args.source == "contacts-instagram":
        if not args.vault or args.vault == "Primary":
            print("\nUsage: whid collect contacts-instagram <path-to-json-or-dir>")
            print("\nExport your Instagram data:")
            print("  1. Go to Instagram > Settings > Your Activity")
            print("  2. Download Your Information > select JSON format")
            print("  3. Download and unzip")
            print("  4. Run: whid collect contacts-instagram ~/Downloads/instagram-export/")
            sys.exit(1)

        export_path = os.path.expanduser(args.vault)
        if not os.path.exists(export_path):
            print(f"\nPath not found: {export_path}")
            sys.exit(1)

        from collectors.instagram_contacts import run_import
        run_import(export_path=export_path, config=config)

    elif args.source == "books-goodreads":
        if not args.vault or args.vault == "Primary":
            print("\nUsage: whid collect books-goodreads <path-to-csv>")
            print("\nExport from Goodreads: My Books > Import/Export > Export Library")
            sys.exit(1)
        export_path = os.path.expanduser(args.vault)
        if not os.path.exists(export_path):
            print(f"\nFile not found: {export_path}")
            sys.exit(1)
        from collectors.books import run_import_goodreads
        run_import_goodreads(export_path, config)

    elif args.source == "books-audible":
        if not args.vault or args.vault == "Primary":
            print("\nUsage: whid collect books-audible <path-to-csv>")
            print("\nExport your Audible library as CSV.")
            sys.exit(1)
        export_path = os.path.expanduser(args.vault)
        if not os.path.exists(export_path):
            print(f"\nFile not found: {export_path}")
            sys.exit(1)
        from collectors.books import run_import_audible
        run_import_audible(export_path, config)

    elif args.source == "youtube":
        if not args.vault or args.vault == "Primary":
            print("\nUsage: whid collect youtube <path-to-watch-history-json-or-dir>")
            print("\nGoogle Takeout: takeout.google.com > YouTube > watch-history.json")
            print("(YouTube watch history API was deprecated — Takeout is the only option)")
            sys.exit(1)
        export_path = os.path.expanduser(args.vault)
        if not os.path.exists(export_path):
            print(f"\nPath not found: {export_path}")
            sys.exit(1)
        from collectors.youtube import run_import
        run_import(export_path, config)

    elif args.source == "music-spotify":
        if not args.vault or args.vault == "Primary":
            print("\nUsage: whid collect music-spotify <path-to-json-or-dir>")
            print("\nRequest your data: spotify.com > Account > Privacy > Download your data")
            sys.exit(1)
        export_path = os.path.expanduser(args.vault)
        if not os.path.exists(export_path):
            print(f"\nPath not found: {export_path}")
            sys.exit(1)
        from collectors.music import run_import
        run_import(export_path, config)

    elif args.source == "finance-paypal":
        if not args.vault or args.vault == "Primary":
            print("\nUsage: whid collect finance-paypal <path-to-csv>")
            print("\nPayPal: Activity > Download > CSV")
            sys.exit(1)
        export_path = os.path.expanduser(args.vault)
        if not os.path.exists(export_path):
            print(f"\nFile not found: {export_path}")
            sys.exit(1)
        from collectors.finance import run_import_paypal
        run_import_paypal(export_path, config)

    elif args.source == "finance-bank":
        if not args.vault or args.vault == "Primary":
            print("\nUsage: whid collect finance-bank <path-to-csv>")
            print("\nExport your bank transactions as CSV.")
            print("Use --bank-name to tag the source (e.g., --bank-name deutschebank)")
            sys.exit(1)
        export_path = os.path.expanduser(args.vault)
        if not os.path.exists(export_path):
            print(f"\nFile not found: {export_path}")
            sys.exit(1)
        from collectors.finance import run_import_bank
        bank_name = getattr(args, "bank_name", "bank")
        run_import_bank(export_path, config, bank_name=bank_name)

    elif args.source == "shopping-amazon":
        if not args.vault or args.vault == "Primary":
            print("\nUsage: whid collect shopping-amazon <path-to-csv>")
            print("\nAmazon: Account > Order History > Download order reports")
            sys.exit(1)
        export_path = os.path.expanduser(args.vault)
        if not os.path.exists(export_path):
            print(f"\nFile not found: {export_path}")
            sys.exit(1)
        from collectors.shopping import run_import_amazon
        run_import_amazon(export_path, config)

    elif args.source == "notes":
        if not args.vault or args.vault == "Primary":
            print("\nUsage: whid collect notes <path-to-directory>")
            print("\nPoint to a directory of .md, .txt, or .markdown files.")
            sys.exit(1)
        export_path = os.path.expanduser(args.vault)
        if not os.path.exists(export_path):
            print(f"\nPath not found: {export_path}")
            sys.exit(1)
        from collectors.notes import run_import
        run_import(export_path, config)

    elif args.source == "podcasts":
        if not args.vault or args.vault == "Primary":
            print("\nUsage: whid collect podcasts <path-to-backup-db-or-csv>")
            print("\nPodcast Addict: Settings > Backup/Restore > Backup")
            sys.exit(1)
        export_path = os.path.expanduser(args.vault)
        if not os.path.exists(export_path):
            print(f"\nFile not found: {export_path}")
            sys.exit(1)
        from collectors.podcasts import run_import
        run_import(export_path, config)

    elif args.source == "health":
        if not args.vault or args.vault == "Primary":
            print("\nUsage: whid collect health <path-to-export-xml-or-dir>")
            print("\nApple Health: Health app > Profile > Export All Health Data")
            sys.exit(1)
        export_path = os.path.expanduser(args.vault)
        if not os.path.exists(export_path):
            print(f"\nPath not found: {export_path}")
            sys.exit(1)
        from collectors.health import run_import
        run_import(export_path, config)

    elif args.source == "browser-chrome":
        from collectors.browser import run_import
        run_import(config=config)

    elif args.source == "calendar":
        creds_file = config.get("calendar", {}).get("credentials_file", "credentials.json")
        if not os.path.exists(creds_file):
            print("\nGoogle Calendar is not set up yet.")
            print("Make sure credentials.json exists and Calendar API is enabled.")
            print("Run: whid setup gmail  (shares the same credentials)")
            sys.exit(1)
        from collectors.calendar_collector import run_export
        run_export(config=config)

    elif args.source == "calendar-ics":
        if not args.vault or args.vault == "Primary":
            print("\nUsage: whid collect calendar-ics <path-to-ics-file>")
            print("\nExport your calendar as .ics file from any calendar app.")
            sys.exit(1)
        export_path = os.path.expanduser(args.vault)
        if not os.path.exists(export_path):
            print(f"\nFile not found: {export_path}")
            sys.exit(1)
        from collectors.calendar_collector import run_import_ics
        run_import_ics(export_path, config)

    elif args.source == "maps":
        if not args.vault or args.vault == "Primary":
            print("\nUsage: whid collect maps <path-to-semantic-location-history-dir>")
            print("\nGoogle Takeout: takeout.google.com > Location History")
            print("(No API available for Maps Timeline — Takeout is the only option)")
            sys.exit(1)
        export_path = os.path.expanduser(args.vault)
        if not os.path.exists(export_path):
            print(f"\nPath not found: {export_path}")
            sys.exit(1)
        from collectors.maps import run_import
        run_import(export_path, config)

    else:
        print(f"\nError: Unknown source '{args.source}'")
        print()
        print("Available collectors:")
        print()
        print("  Google API-based:")
        print("    gmail               Gmail inbox (API)")
        print("    contacts-google     Google Contacts (API)")
        print("    calendar            Google Calendar (API)")
        print()
        print("  File imports — Contacts:")
        print("    contacts-linkedin   LinkedIn export (CSV or full dir)")
        print("    contacts-facebook   Facebook export (JSON)")
        print("    contacts-instagram  Instagram export (JSON)")
        print()
        print("  File imports — Media:")
        print("    books-goodreads     Goodreads library (CSV)")
        print("    books-audible       Audible library (CSV)")
        print("    youtube             YouTube history (Takeout JSON)")
        print("    music-spotify       Spotify streaming history (JSON)")
        print("    podcasts            Podcast history (DB or CSV)")
        print()
        print("  File imports — Life data:")
        print("    finance-paypal      PayPal transactions (CSV)")
        print("    finance-bank        Bank transactions (CSV)")
        print("    shopping-amazon     Amazon orders (CSV)")
        print("    notes               Markdown/text notes (directory)")
        print("    maps                Google Maps timeline (Takeout JSON)")
        print("    health              Apple Health (XML export)")
        print("    browser-chrome      Chrome browsing history (local)")
        print("    calendar-ics        Calendar events (ICS file)")
        print()
        sys.exit(1)


_VAULT_DIR_MAP = {
    "gmail": lambda vault: f"Gmail_{vault}",
    "contacts-google": lambda vault: "Contacts",
    "contacts-linkedin": lambda vault: "Contacts",
    "contacts-facebook": lambda vault: "Contacts",
    "contacts-instagram": lambda vault: "Contacts",
    "books-goodreads": lambda vault: "Books",
    "books-audible": lambda vault: "Books",
    "youtube": lambda vault: "YouTube",
    "music-spotify": lambda vault: "Music",
    "finance-paypal": lambda vault: "Finance",
    "finance-bank": lambda vault: "Finance",
    "shopping-amazon": lambda vault: "Shopping",
    "notes": lambda vault: "Notes",
    "podcasts": lambda vault: "Podcasts",
    "health": lambda vault: "Health",
    "browser-chrome": lambda vault: "Browser",
    "calendar": lambda vault: "Calendar",
    "calendar-ics": lambda vault: "Calendar",
    "maps": lambda vault: "Maps",
}


def cmd_enrich(args, config):
    """Enrich vault entries with additional metadata from the API."""
    if args.source == "gmail":
        config = validate_gmail_config(config)

        creds_file = config.get("gmail", {}).get("credentials_file", "credentials.json")
        if not os.path.exists(creds_file):
            print("\nGmail is not set up yet. Run:\n")
            print("  whid setup gmail\n")
            sys.exit(1)

        from collectors.gmail_enricher import run_enrich
        run_enrich(vault_name=args.vault, config=config)
    else:
        print(f"\nEnrich not available for '{args.source}' yet.")
        print("Available: gmail")
        sys.exit(1)


def cmd_clean(args, config):
    """RAG-optimized cleaning pass (local processing, no API calls)."""
    if args.source == "gmail":
        from core.cleaner import run_clean
        run_clean(vault_name=args.vault, config=config)
    else:
        print(f"\nClean not available for '{args.source}' yet.")
        print("Available: gmail")
        sys.exit(1)


def cmd_groom(args, config):
    """Groom a vault."""
    from core.groomer import groom_vault

    vault_root = get_vault_root(config)

    dir_fn = _VAULT_DIR_MAP.get(args.source)
    if dir_fn:
        vault_path = os.path.join(vault_root, dir_fn(args.vault))
    else:
        vault_path = os.path.join(vault_root, args.source)

    if not os.path.exists(vault_path):
        print(f"\nError: Vault not found: {vault_path}")
        print()
        print(f"Run 'whid collect {args.source}' first to create your vault.")
        sys.exit(1)

    groom_vault(vault_path)


def cmd_update():
    """Pull the latest version from GitHub and reinstall."""
    print("\nUpdating WHID...")

    # git pull from the project root
    result = subprocess.run(
        ["git", "pull", "--ff-only"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"\nGit pull failed:\n{result.stderr.strip()}")
        print(f"\nTry manually: cd {PROJECT_ROOT} && git pull")
        sys.exit(1)

    output = result.stdout.strip()
    if "Already up to date" in output:
        print("Already up to date.")
        return

    print(output)

    # Reinstall in case dependencies changed
    venv_pip = os.path.join(PROJECT_ROOT, "venv", "bin", "pip")
    if os.path.exists(venv_pip):
        print("Reinstalling dependencies...")
        subprocess.run(
            [venv_pip, "install", "-q", "-e", PROJECT_ROOT],
            check=False,
        )

    # Update zsh completions if the user has them set up
    comp_src = os.path.join(PROJECT_ROOT, "completions", "whid.zsh")
    comp_dst = os.path.expanduser("~/.zsh/completions/_whid")
    if os.path.exists(comp_src) and os.path.isdir(os.path.dirname(comp_dst)):
        import shutil
        shutil.copy2(comp_src, comp_dst)
        print("Updated zsh completions. Run 'exec zsh' to reload.")

    print("\nUpdated successfully!")


def _ensure_search_deps():
    """Auto-install chromadb + sentence-transformers if missing."""
    try:
        import chromadb  # noqa: F401
        import sentence_transformers  # noqa: F401
    except ImportError:
        print("\n  Search dependencies not installed (chromadb, sentence-transformers).")
        print("  Installing now — this is a one-time setup...\n")
        import subprocess
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install",
             "chromadb>=1.0.0", "sentence-transformers>=3.0.0"],
        )
        print("\n  Done! Continuing...\n")


def cmd_vectorize(args, config):
    """Vectorize vault data into ChromaDB for semantic search."""
    _ensure_search_deps()
    from core.vectordb import get_client, vectorize_all, get_status

    vault_root = config.get("vault_root", os.path.join(PROJECT_ROOT, "vaults"))
    vault_root = os.path.expanduser(vault_root)
    if not os.path.isabs(vault_root):
        vault_root = os.path.join(PROJECT_ROOT, vault_root)

    source = getattr(args, "source", None)
    force = getattr(args, "force", False)
    show_status = getattr(args, "status", False)

    print(f"\n  WHID Vectorizer")
    print(f"  {'=' * 45}")
    print(f"  Vault root: {vault_root}")

    client = get_client(vault_root)

    if show_status:
        status = get_status(client, vault_root)
        if not status:
            print("  No vectorized data yet. Run 'whid vectorize' first.")
            return
        print()
        for s in sorted(status, key=lambda x: x["collection"]):
            pct = ""
            if s["vault_entries"] > 0:
                pct = f" ({s['vectorized'] / s['vault_entries'] * 100:.0f}%)"
            print(f"  {s['collection']}: {s['vectorized']:,} / {s['vault_entries']:,}{pct}")
        print()
        return

    # Map CLI source name to vault directory
    source_filter = None
    if source:
        # Try direct match first (e.g. "gmail" → "Gmail_Primary")
        # Check _VAULT_DIR_MAP for the mapping
        if source.lower() == "gmail":
            # Find all Gmail_* directories
            if os.path.isdir(vault_root):
                gmail_dirs = [d for d in os.listdir(vault_root)
                              if d.startswith("Gmail") and os.path.isdir(os.path.join(vault_root, d))]
                if gmail_dirs:
                    for gd in gmail_dirs:
                        print(f"\n  Processing {gd}...")
                        from core.vectordb import vectorize_vault
                        vault_path = os.path.join(vault_root, gd)
                        new, skipped, total = vectorize_vault(vault_path, gd, client, config, force)
                    print(f"\n  {'=' * 45}")
                    print(f"  Vectorization complete!")
                    print()
                    return
        else:
            # Map source names to vault directories
            source_map = {
                "contacts": "Contacts",
                "books": "Books",
                "youtube": "YouTube",
                "music": "Music",
                "finance": "Finance",
                "shopping": "Shopping",
                "notes": "Notes",
                "podcasts": "Podcasts",
                "health": "Health",
                "browser": "Browser",
                "calendar": "Calendar",
                "maps": "Maps",
            }
            source_filter = source_map.get(source.lower())
            if not source_filter:
                print(f"  Unknown source: {source}")
                print(f"  Available: gmail, {', '.join(sorted(source_map.keys()))}")
                return

    results = vectorize_all(vault_root, client, config, source_filter, force)

    print(f"\n  {'=' * 45}")
    if results:
        total_new = sum(r["new"] for r in results.values())
        total_db = sum(r["total"] for r in results.values())
        print(f"  Vectorization complete! {total_new:,} new entries ({total_db:,} total in DB)")
    else:
        print(f"  No vaults to vectorize.")
    print()


def cmd_search(args, config):
    """Search the vector database using semantic similarity (no LLM needed)."""
    _ensure_search_deps()
    from core.vectordb import get_client, search, get_full_entry

    vault_root = config.get("vault_root", os.path.join(PROJECT_ROOT, "vaults"))
    vault_root = os.path.expanduser(vault_root)
    if not os.path.isabs(vault_root):
        vault_root = os.path.join(PROJECT_ROOT, vault_root)

    query = args.query
    n_results = getattr(args, "n", 10)
    source = getattr(args, "source", None)
    show_full = getattr(args, "full", False)
    year = getattr(args, "year", None)

    client = get_client(vault_root)

    # Determine which collections to search
    collections = None
    if source:
        source_map = {
            "gmail": "gmail_",
            "contacts": "contacts",
            "books": "books",
            "youtube": "youtube",
            "music": "music",
            "finance": "finance",
            "shopping": "shopping",
            "notes": "notes",
            "podcasts": "podcasts",
            "health": "health",
            "browser": "browser",
            "calendar": "calendar",
            "maps": "maps",
        }
        prefix = source_map.get(source.lower())
        if not prefix:
            print(f"  Unknown source: {source}")
            print(f"  Available: {', '.join(sorted(source_map.keys()))}")
            return

        all_cols = client.list_collections()
        collections = [c.name for c in all_cols if c.name.startswith(prefix)]
        if not collections:
            print(f"  No vectorized data for '{source}'. Run 'whid vectorize {source}' first.")
            return

    where_filter = None
    if year:
        where_filter = {"year": year}

    results = search(client, query, collections=collections,
                     n_results=n_results, where_filter=where_filter, config=config)

    if not results:
        print(f"\n  No results for: {query}")
        if source:
            print(f"  (filtered to source: {source})")
        print()
        return

    print(f"\n  {len(results)} results for: \"{query}\"\n")

    for i, r in enumerate(results, 1):
        meta = r.get("metadata", {})
        distance = r.get("distance", 0)
        relevance = max(0, round((1 - distance) * 100))
        collection = r.get("collection", "?")

        # Header line
        title = meta.get("subject") or meta.get("title") or "(untitled)"
        print(f"  {i}. [{relevance}%] {title}")

        # Metadata line
        parts = []
        if meta.get("from"):
            parts.append(f"From: {meta['from']}")
        if meta.get("date"):
            parts.append(meta["date"])
        if meta.get("tags"):
            parts.append(f"Tags: {meta['tags']}")
        parts.append(f"[{collection}]")
        print(f"     {' | '.join(parts)}")

        if show_full:
            # Show full entry from vault
            vault_dir = meta.get("source", "")
            entry_id = meta.get("entry_id", r.get("id", ""))
            full = get_full_entry(vault_root, vault_dir, entry_id)
            if full:
                import json
                print(f"     ---")
                for k, v in full.items():
                    if k.endswith("_for_embedding"):
                        continue
                    val_str = str(v)
                    if len(val_str) > 200:
                        val_str = val_str[:200] + "..."
                    print(f"     {k}: {val_str}")
        else:
            # Show snippet
            doc = r.get("document", "")
            snippet = doc[:200].replace("\n", " ")
            if len(doc) > 200:
                snippet += "..."
            print(f"     {snippet}")

        print()


def cmd_compress(args, config):
    """Compress vault JSONL files with Zstandard."""
    from core.vault import compress_vault, _find_jsonl_files

    vault_root = config.get("vault_root", os.path.join(PROJECT_ROOT, "vaults"))
    vault_root = os.path.expanduser(vault_root)
    if not os.path.isabs(vault_root):
        vault_root = os.path.join(PROJECT_ROOT, vault_root)

    source = getattr(args, "source", None)

    print(f"\n  WHID Vault Compressor (Zstandard)")
    print(f"  {'=' * 45}")
    print(f"  Vault root: {vault_root}")

    if not os.path.isdir(vault_root):
        print(f"  Error: Vault root not found: {vault_root}")
        return

    # Find vault directories to compress
    targets = []
    for entry in sorted(os.listdir(vault_root)):
        path = os.path.join(vault_root, entry)
        if not os.path.isdir(path) or entry.startswith("."):
            continue
        if source and not entry.lower().startswith(source.lower()):
            continue
        targets.append((entry, path))

    if not targets:
        print(f"  No vaults found{f' matching {source}' if source else ''}.")
        return

    grand_files = 0
    grand_original = 0
    grand_compressed = 0

    for name, path in targets:
        # Count uncompressed files
        plain_files = [f for f in _find_jsonl_files(path) if not f.endswith(".zst")]
        if not plain_files:
            continue

        print(f"\n  {name} ({len(plain_files)} files)")

        file_count = [0]

        def on_progress(file_path, orig, comp):
            file_count[0] += 1
            ratio = orig / comp if comp > 0 else 0
            fname = os.path.basename(file_path)
            print(f"\r    [{file_count[0]}/{len(plain_files)}] {fname}"
                  f" — {orig / 1024:.0f} KB → {comp / 1024:.0f} KB ({ratio:.1f}x)",
                  end="", flush=True)

        files, orig_total, comp_total = compress_vault(path, progress_fn=on_progress)

        if files > 0:
            ratio = orig_total / comp_total if comp_total > 0 else 0
            print(f"\r    {files} files: {orig_total / (1024*1024):.1f} MB → "
                  f"{comp_total / (1024*1024):.1f} MB ({ratio:.1f}x compression)"
                  f"          ")  # trailing spaces to clear progress line
            grand_files += files
            grand_original += orig_total
            grand_compressed += comp_total

    if grand_files == 0:
        print("\n  Everything already compressed.")
    else:
        saved = grand_original - grand_compressed
        ratio = grand_original / grand_compressed if grand_compressed > 0 else 0
        print(f"\n  {'=' * 45}")
        print(f"  Done! {grand_files} files compressed")
        print(f"  Original:   {grand_original / (1024*1024):,.1f} MB")
        print(f"  Compressed: {grand_compressed / (1024*1024):,.1f} MB")
        print(f"  Saved:      {saved / (1024*1024):,.1f} MB ({ratio:.1f}x)")
    print()


def cmd_status(args, config):
    """Show vault status."""
    vault_root = get_vault_root(config)

    if not os.path.exists(vault_root):
        print(f"No vaults found at {vault_root}")
        print()
        print("Get started:")
        print("  whid setup gmail      Set up Gmail export")
        print("  whid collect gmail    Export your Gmail inbox")
        return

    entries_found = False
    print(f"Vault root: {vault_root}\n")

    for entry in sorted(os.listdir(vault_root)):
        vault_path = os.path.join(vault_root, entry)
        if not os.path.isdir(vault_path):
            continue

        entries_found = True

        from core.vault import count_entries as _count_entries
        total_entries, jsonl_files = _count_entries(vault_path)

        processed_log = os.path.join(vault_path, "processed_ids.txt")
        processed = 0
        if os.path.exists(processed_log):
            try:
                with open(processed_log, "r") as f:
                    processed = sum(1 for line in f if line.strip())
            except (OSError, PermissionError) as e:
                logger.warning("Could not read processed log: %s", e)

        missing_log = os.path.join(vault_path, "missing_ids.txt")
        ghosts = 0
        if os.path.exists(missing_log):
            try:
                with open(missing_log, "r") as f:
                    ghosts = sum(1 for line in f if line.strip())
            except (OSError, PermissionError) as e:
                logger.warning("Could not read missing log: %s", e)

        status = "OK"
        if ghosts > 0:
            status = f"GHOSTS ({ghosts} missing — run 'whid collect' to recover)"
        elif total_entries == 0:
            status = "EMPTY"

        print(f"  {entry}")
        print(f"    Entries:   {total_entries:,} across {jsonl_files} files")
        print(f"    Processed: {processed:,}")
        print(f"    Status:    {status}")
        print()

    if not entries_found:
        print("  (no vaults yet)")
        print()
        print("  Get started: whid setup gmail")


def main():
    parser = argparse.ArgumentParser(
        prog="whid",
        description="WhatHaveIDone — Your life. Your data. Your hard drive.",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # whid setup
    setup_parser = subparsers.add_parser("setup", help="Guided setup for a data source")
    setup_parser.add_argument("source", help="Data source to set up (e.g., gmail)")

    # whid collect
    collect_parser = subparsers.add_parser("collect", help="Collect data from a source")
    collect_parser.add_argument(
        "source",
        help="Data source (gmail, contacts-google, contacts-linkedin, contacts-facebook, contacts-instagram)",
    )
    collect_parser.add_argument(
        "vault",
        nargs="?",
        default="Primary",
        help="Vault name (default: Primary) or path to export file for import sources",
    )
    collect_parser.add_argument(
        "--full",
        action="store_true",
        help="Force a full scan instead of incremental (Gmail only)",
    )

    # whid enrich
    enrich_parser = subparsers.add_parser(
        "enrich", help="Backfill metadata from the API (Gmail only)"
    )
    enrich_parser.add_argument("source", help="Data source (e.g., gmail)")
    enrich_parser.add_argument(
        "vault", nargs="?", default="Primary", help="Vault name (default: Primary)"
    )

    # whid clean
    clean_parser = subparsers.add_parser(
        "clean", help="RAG-optimized cleaning pass (local processing)"
    )
    clean_parser.add_argument("source", help="Data source (e.g., gmail)")
    clean_parser.add_argument(
        "vault", nargs="?", default="Primary", help="Vault name (default: Primary)"
    )

    # whid groom
    groom_parser = subparsers.add_parser(
        "groom", help="Groom a vault (deduplicate, sort, detect ghosts)"
    )
    groom_parser.add_argument("source", help="Data source (e.g., gmail)")
    groom_parser.add_argument(
        "vault", nargs="?", default="Primary", help="Vault name (default: Primary)"
    )

    # whid vectorize
    vectorize_parser = subparsers.add_parser(
        "vectorize", help="Vectorize vault data for semantic search"
    )
    vectorize_parser.add_argument(
        "source", nargs="?", default=None,
        help="Source to vectorize (gmail, contacts, notes, etc.) — omit for all",
    )
    vectorize_parser.add_argument(
        "--force", action="store_true",
        help="Re-vectorize all entries (ignore existing)",
    )
    vectorize_parser.add_argument(
        "--status", action="store_true",
        help="Show vectorization status without processing",
    )

    # whid search
    search_parser = subparsers.add_parser(
        "search", help="Semantic search across your vault data"
    )
    search_parser.add_argument("query", help="Search query (natural language)")
    search_parser.add_argument(
        "--source", "-s", default=None,
        help="Limit to a source (gmail, contacts, notes, etc.)",
    )
    search_parser.add_argument(
        "--n", type=int, default=10,
        help="Number of results (default: 10)",
    )
    search_parser.add_argument(
        "--year", "-y", type=int, default=None,
        help="Filter by year (e.g. 2024)",
    )
    search_parser.add_argument(
        "--full", "-f", action="store_true",
        help="Show full entry details instead of snippets",
    )

    # whid compress
    compress_parser = subparsers.add_parser(
        "compress", help="Compress vault files with Zstandard (~5x smaller)"
    )
    compress_parser.add_argument(
        "source", nargs="?", default=None,
        help="Vault to compress (gmail, contacts, etc.) — omit for all",
    )

    # whid status
    subparsers.add_parser("status", help="Show vault status overview")

    # whid update
    subparsers.add_parser("update", help="Pull the latest version from GitHub")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        print()
        print("Quick start:")
        print("  whid setup gmail               Guided Gmail setup")
        print("  whid collect gmail             Export your Gmail inbox")
        print("  whid setup contacts-google     Guided Google Contacts setup")
        print("  whid collect contacts-google   Export your Google Contacts")
        print("  whid collect contacts-linkedin ~/Downloads/Connections.csv")
        print("  whid collect contacts-facebook ~/Downloads/facebook-export/")
        print("  whid collect contacts-instagram ~/Downloads/instagram-export/")
        print("  whid enrich gmail              Backfill metadata from Gmail API")
        print("  whid clean gmail               RAG-optimized cleaning pass")
        print("  whid groom gmail               Deduplicate and sort")
        print("  whid vectorize                 Vectorize all vaults for search")
        print("  whid vectorize gmail           Vectorize Gmail only")
        print("  whid search 'your query'       Search across all your data")
        print("  whid search 'query' -s gmail   Search Gmail only")
        print("  whid status                    See your vaults")
        print("  whid update                    Pull latest version")
        sys.exit(0)

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    if args.command == "update":
        cmd_update()
        return

    config = load_config()

    if args.command == "setup":
        cmd_setup(args, config)
    elif args.command == "collect":
        cmd_collect(args, config)
    elif args.command == "enrich":
        cmd_enrich(args, config)
    elif args.command == "clean":
        cmd_clean(args, config)
    elif args.command == "groom":
        cmd_groom(args, config)
    elif args.command == "vectorize":
        cmd_vectorize(args, config)
    elif args.command == "search":
        cmd_search(args, config)
    elif args.command == "compress":
        cmd_compress(args, config)
    elif args.command == "status":
        cmd_status(args, config)


if __name__ == "__main__":
    main()
