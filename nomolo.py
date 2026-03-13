#!/usr/bin/env python3
"""
NOMOLO CLI — Nomolo command-line interface.

Usage:
    nomolo setup gmail              Guided setup for Gmail
    nomolo setup contacts-google    Guided setup for Google Contacts
    nomolo collect gmail            Export your Gmail (incremental)
    nomolo collect gmail --full     Full rescan (find missing messages)
    nomolo collect contacts-google  Export your Google Contacts
    nomolo collect contacts-linkedin <file>   Import LinkedIn CSV export
    nomolo collect contacts-facebook <file>   Import Facebook JSON export
    nomolo collect contacts-instagram <dir>   Import Instagram JSON export
    nomolo collect text-stream start         Start text input capture receiver
    nomolo collect text-stream stop          Stop text input capture receiver
    nomolo enrich gmail             Backfill metadata from Gmail API
    nomolo clean gmail              RAG-optimized cleaning pass
    nomolo groom gmail              Deduplicate and sort
    nomolo vectorize                Vectorize all vaults for semantic search
    nomolo vectorize gmail          Vectorize Gmail only
    nomolo search 'query'           Search your data (no LLM needed)
    nomolo search 'query' -s gmail  Search Gmail only
    nomolo web                      Launch the visual web interface
    nomolo scan                     Discover data sources on your machine
    nomolo status                   See your vaults
    nomolo update                   Pull latest version from GitHub
    nomolo mcp setup                Auto-configure MCP server for Claude
    nomolo --version                Show version
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
    os.path.expanduser("~/.config/nomolo/config.yaml"),
]

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

logger = logging.getLogger("nomolo")


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
    print("  \u2693 NOMOLO — Board the Omniscient Eye (Gmail)")
    print("=" * 50)

    target = os.path.join(PROJECT_ROOT, "credentials.json")

    # Check if already set up
    if os.path.exists(target) and os.path.exists(token):
        print("\n\u2694\ufe0f Gmail is already boarded, Captain!")
        print(f"  Letter of Marque: {target}")
        print(f"  Boarding pass: {token}")
        print("\nRun 'nomolo collect gmail' to raid yer Scrolls.")
        return

    _setup_google_credentials(config)

    if not os.path.exists(target):
        print("\nError: credentials.json still not found. Setup incomplete.")
        sys.exit(1)

    print("\nStep 2: Sign in with Google")
    print("-" * 40)
    print("\nA browser window will open for Google sign-in.")
    print("Sign in with the Gmail account you want to export.")
    print("(NOMOLO uses read-only access — it cannot modify your email)\n")

    input("Press Enter to open the sign-in page...")

    try:
        from collectors.gmail_collector import get_credentials

        scopes = [config.get("gmail", {}).get(
            "scope", "https://www.googleapis.com/auth/gmail.readonly"
        )]
        creds = get_credentials(target, token, scopes)
    except Exception as e:
        print(f"\nAuthentication failed: {e}")
        print("Try running 'nomolo setup gmail' again.")
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
        print("  But credentials are saved — try 'nomolo collect gmail' anyway.")

    print("\n" + "=" * 50)
    print("  Setup complete!")
    print("=" * 50)
    print(f"\n  Run:  nomolo collect gmail")
    print(f"  Data: {os.path.join(PROJECT_ROOT, 'vaults', 'Gmail_Primary')}/")
    print()


def _setup_contacts_google(args, config):
    """Guided Google Contacts setup."""
    contacts_cfg = config.get("contacts", {})
    token = os.path.join(
        PROJECT_ROOT, contacts_cfg.get("token_file", "token_contacts.json")
    )

    print("\n" + "=" * 50)
    print("  \u2693 NOMOLO — Raid the Soul Bonds (Google Contacts)")
    print("=" * 50)

    target = os.path.join(PROJECT_ROOT, "credentials.json")

    # Check if already set up
    if os.path.exists(target) and os.path.exists(token):
        print("\n\u2694\ufe0f Google Contacts already boarded, Captain!")
        print(f"  Letter of Marque: {target}")
        print(f"  Boarding pass: {token}")
        print("\nRun 'nomolo collect contacts-google' to raid yer Soul Bonds.")
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
    print("(NOMOLO uses read-only access — it cannot modify your contacts)\n")

    input("Press Enter to open the sign-in page...")

    try:
        from core.auth import get_google_credentials

        scopes = [contacts_cfg.get(
            "scope", "https://www.googleapis.com/auth/contacts.readonly"
        )]
        creds = get_google_credentials(target, token, scopes)
    except Exception as e:
        print(f"\nAuthentication failed: {e}")
        print("Try running 'nomolo setup contacts-google' again.")
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
        print("  But credentials are saved — try 'nomolo collect contacts-google' anyway.")

    print("\n" + "=" * 50)
    print("  Setup complete!")
    print("=" * 50)
    print(f"\n  Run:  nomolo collect contacts-google")
    print(f"  Data: {os.path.join(PROJECT_ROOT, 'vaults', 'Contacts')}/")
    print()


def _collect_file_import(args, config, usage_lines, collector_module,
                         collector_fn, expected_type="file", extra_kwargs=None):
    """Shared boilerplate for file/directory-based collectors.

    Args:
        args: Parsed CLI args (needs .vault).
        config: Loaded config dict.
        usage_lines: List of strings to print as usage help (after the Usage: line).
        collector_module: Dotted module path to import from.
        collector_fn: Function name to call in that module.
        expected_type: "file" or "path" — controls the "not found" wording.
        extra_kwargs: Optional dict of extra keyword args to pass to the collector.
    """
    if not args.vault or args.vault == "Primary":
        print(f"\nUsage: nomolo collect {args.source} <{expected_type}>")
        for line in usage_lines:
            print(line)
        sys.exit(1)

    export_path = os.path.expanduser(args.vault)
    if not os.path.exists(export_path):
        label = "File" if expected_type == "file" else "Path"
        print(f"\n{label} not found: {export_path}")
        sys.exit(1)

    import importlib
    mod = importlib.import_module(collector_module)
    fn = getattr(mod, collector_fn)

    kwargs = {"config": config}
    if extra_kwargs:
        kwargs.update(extra_kwargs)
    fn(export_path, **kwargs)


# Registry of file-based collectors: source -> kwargs for _collect_file_import
_FILE_COLLECTORS = {
    "contacts-linkedin": {
        "usage_lines": [
            "\nExport your LinkedIn data:",
            "  1. Go to linkedin.com > Settings > Data Privacy",
            "  2. Get a copy of your data (select all or at least Connections)",
            "  3. Download and unzip the archive",
            "  4. Run: nomolo collect contacts-linkedin ~/Downloads/linkedin-export/",
            "     or:  nomolo collect contacts-linkedin ~/Downloads/Connections.csv",
        ],
        "collector_module": "collectors.linkedin_contacts",
        "collector_fn": "run_import",
        "expected_type": "path",
    },
    "contacts-facebook": {
        "usage_lines": [
            "\nExport your Facebook data:",
            "  1. Go to facebook.com > Settings > Your Information",
            "  2. Download Your Information > select JSON format",
            "  3. Download and unzip",
            "  4. Run: nomolo collect contacts-facebook ~/Downloads/facebook-export/",
        ],
        "collector_module": "collectors.facebook_contacts",
        "collector_fn": "run_import",
        "expected_type": "path",
    },
    "contacts-instagram": {
        "usage_lines": [
            "\nExport your Instagram data:",
            "  1. Go to Instagram > Settings > Your Activity",
            "  2. Download Your Information > select JSON format",
            "  3. Download and unzip",
            "  4. Run: nomolo collect contacts-instagram ~/Downloads/instagram-export/",
        ],
        "collector_module": "collectors.instagram_contacts",
        "collector_fn": "run_import",
        "expected_type": "path",
    },
    "books-goodreads": {
        "usage_lines": [
            "\nExport from Goodreads: My Books > Import/Export > Export Library",
        ],
        "collector_module": "collectors.books",
        "collector_fn": "run_import_goodreads",
        "expected_type": "file",
    },
    "books-audible": {
        "usage_lines": [
            "\nExport your Audible library as CSV.",
        ],
        "collector_module": "collectors.books",
        "collector_fn": "run_import_audible",
        "expected_type": "file",
    },
    "youtube": {
        "usage_lines": [
            "\nGoogle Takeout: takeout.google.com > YouTube > watch-history.json",
            "(YouTube watch history API was deprecated — Takeout is the only option)",
        ],
        "collector_module": "collectors.youtube",
        "collector_fn": "run_import",
        "expected_type": "path",
    },
    "music-spotify": {
        "usage_lines": [
            "\nRequest your data: spotify.com > Account > Privacy > Download your data",
        ],
        "collector_module": "collectors.music",
        "collector_fn": "run_import",
        "expected_type": "path",
    },
    "finance-paypal": {
        "usage_lines": [
            "\nPayPal: Activity > Download > CSV",
        ],
        "collector_module": "collectors.finance",
        "collector_fn": "run_import_paypal",
        "expected_type": "file",
    },
    "finance-bank": {
        "usage_lines": [
            "\nExport your bank transactions as CSV.",
            "Use --bank-name to tag the source (e.g., --bank-name deutschebank)",
        ],
        "collector_module": "collectors.finance",
        "collector_fn": "run_import_bank",
        "expected_type": "file",
    },
    "shopping-amazon": {
        "usage_lines": [
            "\nAmazon: Account > Order History > Download order reports",
        ],
        "collector_module": "collectors.shopping",
        "collector_fn": "run_import_amazon",
        "expected_type": "file",
    },
    "notes": {
        "usage_lines": [
            "\nPoint to a directory of .md, .txt, or .markdown files.",
        ],
        "collector_module": "collectors.notes",
        "collector_fn": "run_import",
        "expected_type": "path",
    },
    "podcasts": {
        "usage_lines": [
            "\nPodcast Addict: Settings > Backup/Restore > Backup",
        ],
        "collector_module": "collectors.podcasts",
        "collector_fn": "run_import",
        "expected_type": "file",
    },
    "health": {
        "usage_lines": [
            "\nApple Health: Health app > Profile > Export All Health Data",
        ],
        "collector_module": "collectors.health",
        "collector_fn": "run_import",
        "expected_type": "path",
    },
    "calendar-ics": {
        "usage_lines": [
            "\nExport your calendar as .ics file from any calendar app.",
        ],
        "collector_module": "collectors.calendar_collector",
        "collector_fn": "run_import_ics",
        "expected_type": "file",
    },
    "maps": {
        "usage_lines": [
            "\nGoogle Takeout: takeout.google.com > Location History",
            "(No API available for Maps Timeline — Takeout is the only option)",
        ],
        "collector_module": "collectors.maps",
        "collector_fn": "run_import",
        "expected_type": "path",
    },
}


def cmd_collect(args, config):
    """Run a collector."""
    if args.source == "gmail":
        config = validate_gmail_config(config)

        creds_file = config.get("gmail", {}).get("credentials_file", "credentials.json")
        if not os.path.exists(creds_file):
            print("\nGmail is not set up yet. Run:\n")
            print("  nomolo setup gmail\n")
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
            print("  nomolo setup contacts-google\n")
            sys.exit(1)

        try:
            from collectors.google_contacts import run_export
        except ImportError as e:
            print(f"\nError: Failed to load Google Contacts collector: {e}")
            print("Try reinstalling: pip install -e .")
            sys.exit(1)

        run_export(config=config)

    elif args.source == "browser-chrome":
        from collectors.browser import run_import
        run_import(config=config)

    elif args.source == "calendar":
        creds_file = config.get("calendar", {}).get("credentials_file", "credentials.json")
        if not os.path.exists(creds_file):
            print("\nGoogle Calendar is not set up yet.")
            print("Make sure credentials.json exists and Calendar API is enabled.")
            print("Run: nomolo setup gmail  (shares the same credentials)")
            sys.exit(1)
        from collectors.calendar_collector import run_export
        run_export(config=config)

    elif args.source in _FILE_COLLECTORS:
        entry = _FILE_COLLECTORS[args.source]
        extra_kwargs = None
        if args.source == "finance-bank":
            extra_kwargs = {"bank_name": getattr(args, "bank_name", "bank")}
        _collect_file_import(
            args, config,
            usage_lines=entry["usage_lines"],
            collector_module=entry["collector_module"],
            collector_fn=entry["collector_fn"],
            expected_type=entry["expected_type"],
            extra_kwargs=extra_kwargs,
        )

    elif args.source == "text-stream":
        from collectors.text_stream import run_server, stop_server, is_running

        subcommand = args.vault  # reuse vault positional arg for start/stop
        if subcommand == "start":
            if is_running():
                print("\n  Text stream receiver is already running.")
                print("  Use 'nomolo collect text-stream stop' to stop it first.")
                sys.exit(1)
            run_server(config=config)
        elif subcommand == "stop":
            stop_server()
        elif subcommand == "status":
            if is_running():
                print("\n  Text stream receiver is running.")
            else:
                print("\n  Text stream receiver is not running.")
                print("  Start it with: nomolo collect text-stream start")
        else:
            print("\nUsage: nomolo collect text-stream <start|stop|status>")
            print()
            print("  start   Start the local text capture receiver (localhost:19876)")
            print("  stop    Stop the running receiver")
            print("  status  Check if the receiver is running")
            print()
            print("  Install the Chrome extension from:")
            print("    chrome://extensions > Load unpacked > collectors/browser_input/")
            sys.exit(1)

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
        print("  Live capture:")
        print("    text-stream start   Start text input receiver (Chrome extension)")
        print("    text-stream stop    Stop the receiver")
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
    "text-stream": lambda vault: "TextStream",
}


def cmd_enrich(args, config):
    """Enrich vault entries with additional metadata from the API."""
    if args.source == "gmail":
        config = validate_gmail_config(config)

        creds_file = config.get("gmail", {}).get("credentials_file", "credentials.json")
        if not os.path.exists(creds_file):
            print("\nGmail is not set up yet. Run:\n")
            print("  nomolo setup gmail\n")
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
        print(f"Run 'nomolo collect {args.source}' first to create your vault.")
        sys.exit(1)

    groom_vault(vault_path)


def cmd_web(args):
    """Launch the Nomolo web interface."""
    try:
        import uvicorn  # noqa: F401
    except ImportError:
        print("\nInstalling web dependencies...")
        venv_pip = os.path.join(PROJECT_ROOT, "venv", "bin", "pip")
        pip_cmd = venv_pip if os.path.exists(venv_pip) else "pip"
        subprocess.run(
            [pip_cmd, "install", "-q", "fastapi", "uvicorn[standard]", "jinja2", "websockets"],
            check=True,
        )

    port = getattr(args, "port", 3000)
    no_open = getattr(args, "no_open", False)

    print(f"\n⚓ Hoisting the SCUMM Bar flag on http://localhost:{port}")
    print("   Press Ctrl+C to abandon ship\n")

    if not no_open:
        import threading
        threading.Timer(1.5, lambda: webbrowser.open(f"http://localhost:{port}")).start()

    import uvicorn
    uvicorn.run(
        "web.server:app",
        host="127.0.0.1",
        port=port,
        log_level="warning",
        reload=False,
    )


def cmd_scan():
    """Run the scanner in CLI mode and print results."""
    import asyncio
    from web.scanner import scan, get_life_score

    config = load_config()
    vault_root = get_vault_root(config)

    print("\n🔍 Scanning the seven seas for buried treasure...\n")

    results = asyncio.run(scan(vault_root=vault_root, project_root=PROJECT_ROOT))

    sources = results.get("sources", [])
    if not sources:
        print("No islands on the horizon, Captain. Try running from yer home port.")
        return

    # Group by category
    by_category = {}
    for s in sources:
        cat = s.get("category", "other")
        by_category.setdefault(cat, []).append(s)

    total_records = 0
    for cat, items in sorted(by_category.items()):
        print(f"  {cat.upper()}")
        for s in items:
            grade = s.get("nomolo_grade", "?")
            icon = s.get("icon", "📁")
            name = s.get("name", "Unknown")
            status = s.get("status", "discovered")
            est = s.get("estimated_records", 0)
            total_records += est
            status_icon = "✅" if status == "already_collected" else "🔍" if status == "ready" else "📋"
            est_str = f" (~{est:,} records)" if est else ""
            print(f"    {status_icon} {icon} {name} [{grade}]{est_str}")
            if status != "already_collected":
                action = s.get("action", "")
                if action:
                    print(f"       → {action}")
        print()

    score_data = get_life_score(results)
    score = score_data.get("overall_score", 0)
    print(f"  🏆 Pirate Plunder Score: {score}/100")
    print(f"  💰 Total loot discovered: {total_records:,} pieces of plunder")
    print(f"  🍺 Tip: Run 'nomolo web' to visit the SCUMM Bar!\n")


def cmd_update():
    """Pull the latest version from GitHub and reinstall."""
    print("\n⚓ Patching the ship's hull...")

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
        print("Reloading the cannons...")
        subprocess.run(
            [venv_pip, "install", "-q", "-e", PROJECT_ROOT],
            check=False,
        )

    # Update zsh completions if the user has them set up
    comp_src = os.path.join(PROJECT_ROOT, "completions", "nomolo.zsh")
    comp_dst = os.path.expanduser("~/.zsh/completions/_nomolo")
    if os.path.exists(comp_src) and os.path.isdir(os.path.dirname(comp_dst)):
        import shutil
        shutil.copy2(comp_src, comp_dst)
        print("Updated zsh completions. Run 'exec zsh' to reload.")

    print("\n🎉 Ship's hull patched and polished! Ready to sail, Captain.")


def _ensure_search_deps():
    """Auto-install chromadb + sentence-transformers if missing."""
    try:
        import chromadb  # noqa: F401
        import sentence_transformers  # noqa: F401
    except ImportError:
        print("\n  Missing navigation instruments (chromadb, sentence-transformers).")
        print("  Forging them now — this is a one-time outfitting...\n")
        import subprocess
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install",
             "chromadb>=1.0.0", "sentence-transformers>=3.0.0"],
        )
        print("\n  Instruments ready! Continuing...\n")


def cmd_vectorize(args, config):
    """Vectorize vault data into ChromaDB for semantic search and build FTS index."""
    _ensure_search_deps()
    from core.vectordb import get_client, vectorize_all, get_status
    from core.search_engine import index_all as fts_index_all, get_fts_entry_count, _get_fts_db_path

    vault_root = config.get("vault_root", os.path.join(PROJECT_ROOT, "vaults"))
    vault_root = os.path.expanduser(vault_root)
    if not os.path.isabs(vault_root):
        vault_root = os.path.join(PROJECT_ROOT, vault_root)

    source = getattr(args, "source", None)
    force = getattr(args, "force", False)
    show_status = getattr(args, "status", False)

    print(f"\n  🏴‍☠️ NOMOLO Cartographer — Charting the Treasure Maps")
    print(f"  {'=' * 45}")
    print(f"  Vault root: {vault_root}")

    client = get_client(vault_root)

    if show_status:
        status = get_status(client, vault_root)
        if not status:
            print("  No treasure maps charted yet. Run 'nomolo vectorize' first, Captain.")
            return
        print()
        for s in sorted(status, key=lambda x: x["collection"]):
            pct = ""
            if s["vault_entries"] > 0:
                pct = f" ({s['vectorized'] / s['vault_entries'] * 100:.0f}%)"
            print(f"  {s['collection']}: {s['vectorized']:,} / {s['vault_entries']:,}{pct}")

        # Show FTS index status
        db_path = _get_fts_db_path(vault_root, config)
        fts_count = get_fts_entry_count(db_path)
        print(f"\n  FTS index: {fts_count:,} entries")
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

                    # Also build FTS index for Gmail vaults
                    print(f"\n  Building FTS keyword index...")
                    from core.search_engine import index_vault as fts_index_vault
                    for gd in gmail_dirs:
                        vault_path = os.path.join(vault_root, gd)
                        fts_index_vault(vault_path, gd, config)

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
        print(f"  Treasure maps charted! {total_new:,} new entries ({total_db:,} total in the atlas)")
    else:
        print(f"  No vaults to chart. Plunder some data first, Captain!")

    # Build FTS keyword index alongside vector index
    print(f"\n  Building FTS keyword index...")
    fts_results = fts_index_all(vault_root, config, source_filter)
    if fts_results:
        fts_new = sum(r["new"] for r in fts_results.values())
        db_path = _get_fts_db_path(vault_root, config)
        fts_total = get_fts_entry_count(db_path)
        print(f"  FTS index: {fts_new:,} new entries ({fts_total:,} total)")
    else:
        print(f"  FTS index: up to date")
    print()


def cmd_search(args, config):
    """Hybrid search across your vault data (BM25 + semantic + metadata boosting)."""
    _ensure_search_deps()
    from core.vectordb import get_client, get_full_entry
    from core.search_engine import hybrid_search

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
            print(f"  No vectorized data for '{source}'. Run 'nomolo vectorize {source}' first.")
            return

    results = hybrid_search(
        query, vault_root, client, config=config,
        n_results=n_results, collections=collections, year_filter=year,
    )

    if not results:
        print(f"\n  The seas are empty here, Captain. No results for: {query}")
        if source:
            print(f"  (searched only the {source} waters)")
        print()
        return

    print(f"\n  {len(results)} treasures found for: \"{query}\" (hybrid search)\n")

    for i, r in enumerate(results, 1):
        meta = r.get("metadata", {})
        score = r.get("combined_score", 0)
        collection = r.get("collection", "?")

        # Header line
        title = meta.get("subject") or meta.get("title") or "(untitled)"
        print(f"  {i}. [{score:.4f}] {title}")

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
            entry_id = r.get("entry_id", meta.get("entry_id", ""))
            full = get_full_entry(vault_root, vault_dir, entry_id)
            if full:
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
            snippet = r.get("snippet", "")
            if snippet:
                snippet_display = snippet[:200].replace("\n", " ")
                if len(snippet) > 200:
                    snippet_display += "..."
                print(f"     {snippet_display}")

        print()


def cmd_compress(args, config):
    """Compress vault JSONL files with Zstandard."""
    from core.vault import compress_vault, _find_jsonl_files

    vault_root = config.get("vault_root", os.path.join(PROJECT_ROOT, "vaults"))
    vault_root = os.path.expanduser(vault_root)
    if not os.path.isabs(vault_root):
        vault_root = os.path.join(PROJECT_ROOT, vault_root)

    source = getattr(args, "source", None)

    print(f"\n  🗜️ NOMOLO Hold Compressor — More Room for Grog!")
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
        print(f"No treasure vaults found at {vault_root}")
        print()
        print("Time to start plundering, Captain:")
        print("  nomolo setup gmail      Prepare to raid Gmail")
        print("  nomolo collect gmail    Plunder yer Gmail inbox")
        return

    entries_found = False
    print(f"\n\u2693 Treasure Hold: {vault_root}\n")

    grand_total = 0
    for entry in sorted(os.listdir(vault_root)):
        vault_path = os.path.join(vault_root, entry)
        if not os.path.isdir(vault_path):
            continue

        entries_found = True

        from core.vault import count_entries as _count_entries
        total_entries, jsonl_files = _count_entries(vault_path)
        grand_total += total_entries

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

        status = "\u2694\ufe0f Shipshape"
        if ghosts > 0:
            status = f"\U0001F3AF {ghosts} ghost IDs — the Sniper is tracking 'em"
        elif total_entries == 0:
            status = "\U0001F3DD\ufe0f Empty island"

        print(f"  \U0001F4DC {entry}")
        print(f"    Loot:      {total_entries:,} pieces across {jsonl_files} scrolls")
        print(f"    Logged:    {processed:,}")
        print(f"    Status:    {status}")
        print()

    if entries_found:
        print(f"  \U0001F4B0 Total plunder: {grand_total:,} pieces of loot")
        print(f"  \U0001F37A Visit the SCUMM Bar: nomolo web\n")
    else:
        print("  (the hold is empty, Captain)")
        print()
        print("  Set sail: nomolo setup gmail")


def get_version():
    """Read version from pyproject.toml."""
    pyproject = os.path.join(PROJECT_ROOT, "pyproject.toml")
    try:
        with open(pyproject, "r") as f:
            for line in f:
                line = line.strip()
                if line.startswith("version"):
                    # Parse: version = "0.1.0"
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
    except (OSError, IndexError):
        pass
    return "unknown"


def cmd_mcp_setup():
    """Auto-configure MCP server for Claude Desktop and/or Claude Code CLI."""
    mcp_server_path = os.path.join(PROJECT_ROOT, "mcp_server.py")
    venv_python = os.path.join(PROJECT_ROOT, ".venv", "bin", "python")

    if not os.path.exists(mcp_server_path):
        print(f"Error: MCP server not found at {mcp_server_path}")
        sys.exit(1)

    if not os.path.exists(venv_python):
        print(f"Error: Virtual environment not found at {venv_python}")
        print("Run: python3 -m venv .venv && .venv/bin/pip install -e '.[mcp]'")
        sys.exit(1)

    # --- Verify MCP server can start ---
    print("Checking the signal flags...")
    check = subprocess.run(
        [venv_python, "-c", "from mcp.server import Server; from core.vectordb import get_client; print('ok')"],
        capture_output=True, text=True, cwd=PROJECT_ROOT,
    )
    if check.returncode != 0:
        print("Error: MCP server imports failed.")
        print(check.stderr.strip())
        print("\nInstall MCP dependencies: .venv/bin/pip install -e '.[mcp]'")
        sys.exit(1)
    print("  Signal flags rigged and ready!")

    configured_any = False

    # --- Claude Desktop ---
    desktop_config_path = os.path.expanduser(
        "~/Library/Application Support/Claude/claude_desktop_config.json"
    )
    desktop_dir = os.path.dirname(desktop_config_path)

    if os.path.isdir(desktop_dir):
        print("\nClaude Desktop detected.")
        # Read existing config
        if os.path.exists(desktop_config_path):
            try:
                with open(desktop_config_path, "r") as f:
                    desktop_config = json.load(f)
            except (json.JSONDecodeError, OSError):
                desktop_config = {}
        else:
            desktop_config = {}

        mcp_servers = desktop_config.setdefault("mcpServers", {})
        new_entry = {
            "command": venv_python,
            "args": [mcp_server_path],
        }

        if "nomolo" in mcp_servers and mcp_servers["nomolo"] == new_entry:
            print("  Already configured in Claude Desktop.")
        else:
            mcp_servers["nomolo"] = new_entry
            with open(desktop_config_path, "w") as f:
                json.dump(desktop_config, f, indent=2)
            print("  Added Nomolo MCP server to Claude Desktop config.")
            print(f"  Config: {desktop_config_path}")
        configured_any = True
    else:
        print("\nClaude Desktop not detected (no config directory found).")

    # --- Claude Code CLI ---
    claude_bin = shutil.which("claude")
    if claude_bin:
        print("\nClaude Code CLI detected.")
        # Check if already configured
        list_result = subprocess.run(
            ["claude", "mcp", "list", "-s", "user"],
            capture_output=True, text=True,
        )
        if "nomolo" in list_result.stdout:
            print("  Already configured in Claude Code CLI.")
        else:
            add_result = subprocess.run(
                ["claude", "mcp", "add", "nomolo", "-s", "user", "--", venv_python, mcp_server_path],
                capture_output=True, text=True,
            )
            if add_result.returncode == 0:
                print("  Added Nomolo MCP server to Claude Code CLI (user scope).")
            else:
                print("  Warning: Failed to add MCP server to Claude Code CLI.")
                print(f"  {add_result.stderr.strip()}")
        configured_any = True
    else:
        print("\nClaude Code CLI not detected (claude command not found).")

    # --- Summary ---
    if configured_any:
        print("\nSignal flags hoisted! To activate:")
        if os.path.isdir(desktop_dir):
            print("  - Claude Desktop: restart the app")
        if claude_bin:
            print("  - Claude Code: start a new session")
    else:
        print("\nNo Claude vessels spotted on the horizon.")
        print("Install Claude Desktop or Claude Code CLI first, Captain.")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        prog="nomolo",
        description="🏴‍☠️ Nomolo — The Data Pirate's Vessel. Your life. Your data. Your hard drive.",
    )
    parser.add_argument(
        "--version", action="version",
        version=f"Nomolo v{get_version()}",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available orders, Captain")

    # nomolo setup
    setup_parser = subparsers.add_parser("setup", help="Prepare the gangplank for a raid target")
    setup_parser.add_argument("source", help="Raid target to set up (e.g., gmail)")

    # nomolo collect
    collect_parser = subparsers.add_parser("collect", help="Plunder data from a raid target")
    collect_parser.add_argument(
        "source",
        help="Raid target (gmail, contacts-google, contacts-linkedin, contacts-facebook, contacts-instagram)",
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

    # nomolo enrich
    enrich_parser = subparsers.add_parser(
        "enrich", help="Polish the plunder with extra metadata from the API"
    )
    enrich_parser.add_argument("source", help="Raid target (e.g., gmail)")
    enrich_parser.add_argument(
        "vault", nargs="?", default="Primary", help="Vault name (default: Primary)"
    )

    # nomolo clean
    clean_parser = subparsers.add_parser(
        "clean", help="Scrub the deck — RAG-optimized cleaning pass"
    )
    clean_parser.add_argument("source", help="Raid target (e.g., gmail)")
    clean_parser.add_argument(
        "vault", nargs="?", default="Primary", help="Vault name (default: Primary)"
    )

    # nomolo groom
    groom_parser = subparsers.add_parser(
        "groom", help="Sort the plunder (deduplicate, organize, detect ghost entries)"
    )
    groom_parser.add_argument("source", help="Raid target (e.g., gmail)")
    groom_parser.add_argument(
        "vault", nargs="?", default="Primary", help="Vault name (default: Primary)"
    )

    # nomolo vectorize
    vectorize_parser = subparsers.add_parser(
        "vectorize", help="Chart the treasure maps for semantic search"
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

    # nomolo search
    search_parser = subparsers.add_parser(
        "search", help="Search the seas — find treasures in yer vault"
    )
    search_parser.add_argument("query", help="What treasure to search for (natural language)")
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

    # nomolo compress
    compress_parser = subparsers.add_parser(
        "compress", help="Pack the hold tighter — compress vault files (~5x smaller)"
    )
    compress_parser.add_argument(
        "source", nargs="?", default=None,
        help="Vault to compress (gmail, contacts, etc.) — omit for all",
    )

    # nomolo status
    subparsers.add_parser("status", help="Check the ship's log — vault status overview")

    # nomolo update
    subparsers.add_parser("update", help="Patch the hull — pull the latest version from GitHub")

    # nomolo web
    web_parser = subparsers.add_parser(
        "web", help="Hoist the colors — launch the SCUMM Bar (web interface)"
    )
    web_parser.add_argument(
        "--port", "-p", type=int, default=3000,
        help="Port to run on (default: 3000)",
    )
    web_parser.add_argument(
        "--no-open", action="store_true",
        help="Don't auto-open browser",
    )

    # nomolo scan
    subparsers.add_parser("scan", help="Scan the horizon for raid targets (CLI mode)")

    # nomolo mcp
    mcp_parser = subparsers.add_parser("mcp", help="MCP server — rig the signal flags for Claude")
    mcp_subparsers = mcp_parser.add_subparsers(dest="mcp_command", help="MCP orders")
    mcp_subparsers.add_parser("setup", help="Rig the signal flags for Claude Desktop / Claude Code")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        print()
        print("Set sail, Captain:")
        print("  nomolo setup gmail               Prepare to raid Gmail")
        print("  nomolo collect gmail             Plunder yer Gmail inbox")
        print("  nomolo setup contacts-google     Prepare to raid Google Contacts")
        print("  nomolo collect contacts-google   Plunder yer Google Contacts")
        print("  nomolo collect contacts-linkedin ~/Downloads/Connections.csv")
        print("  nomolo collect contacts-facebook ~/Downloads/facebook-export/")
        print("  nomolo collect contacts-instagram ~/Downloads/instagram-export/")
        print("  nomolo enrich gmail              Polish the plunder with extra metadata")
        print("  nomolo clean gmail               Scrub the deck (RAG cleaning)")
        print("  nomolo groom gmail               Sort the plunder")
        print("  nomolo vectorize                 Chart the treasure maps for search")
        print("  nomolo vectorize gmail           Chart Gmail's map only")
        print("  nomolo search 'your query'       Search the seas for treasure")
        print("  nomolo search 'query' -s gmail   Search Gmail waters only")
        print("  nomolo web                       Hoist the colors (SCUMM Bar)")
        print("  nomolo scan                      Scan the horizon for raid targets")
        print("  nomolo status                    Check the ship's log")
        print("  nomolo update                    Patch the hull")
        print("  nomolo mcp setup                 Rig the signal flags for Claude")
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

    if args.command == "web":
        cmd_web(args)
        return

    if args.command == "scan":
        cmd_scan()
        return

    if args.command == "mcp":
        if getattr(args, "mcp_command", None) == "setup":
            cmd_mcp_setup()
        else:
            print("Usage: nomolo mcp setup")
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
