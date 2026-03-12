#!/usr/bin/env python3
"""
WHID CLI — WhatHaveIDone command-line interface.

Usage:
    whid setup gmail         Guided setup (opens browser, finds credentials)
    whid collect gmail       Export your Gmail
    whid groom gmail         Deduplicate and sort
    whid status              See your vaults
    whid update              Pull latest version from GitHub
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
    vault_root = os.path.expanduser(config.get("vault_root", "~/Documents/WHID_Vaults"))

    parent = os.path.dirname(vault_root)
    if not os.path.exists(parent):
        print(f"\nError: Vault root parent directory does not exist: {parent}")
        print(f"Check vault_root in config.yaml: {config.get('vault_root', '~/Documents/WHID_Vaults')}")
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
        os.path.expanduser("~/Downloads/credentials.json"),
        os.path.expanduser("~/Desktop/credentials.json"),
    ]

    # Also check for client_secret_*.json in Downloads and Desktop (Google's default name)
    for folder in ["~/Downloads", "~/Desktop"]:
        folder_path = os.path.expanduser(folder)
        if os.path.exists(folder_path):
            search_paths.extend(
                sorted(
                    glob.glob(os.path.join(folder_path, "client_secret_*.json")),
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


def cmd_setup(args, config):
    """Guided setup for a collector."""
    if args.source != "gmail":
        print(f"Setup not available for '{args.source}' yet.")
        print("Available: gmail")
        sys.exit(1)

    target = os.path.join(PROJECT_ROOT, "credentials.json")
    token = os.path.join(PROJECT_ROOT, "token.json")

    print("\n" + "=" * 50)
    print("  WHID Gmail Setup")
    print("=" * 50)

    # Step 0: Check if already set up
    if os.path.exists(target) and os.path.exists(token):
        print("\nGmail is already set up!")
        print(f"  Credentials: {target}")
        print(f"  Token: {token}")
        print("\nRun 'whid collect gmail' to start downloading.")
        return

    # Step 1: Check for existing credentials
    if os.path.exists(target):
        print(f"\nCredentials found: {target}")
    else:
        print("\nStep 1: Get Google OAuth credentials")
        print("-" * 40)

        found = _find_credentials_file()
        if found:
            print(f"\nFound credentials file: {found}")
            answer = _prompt("Use this file? (y/n)", "y")
            if answer.lower() in ("y", "yes", ""):
                shutil.copy2(found, target)
                print(f"Copied to {target}")
            else:
                found = None

        if not found and not os.path.exists(target):
            print("\nI'll open Google Cloud Console in your browser.")
            print("Follow these steps:\n")
            print("  1. Create a project (or select existing)")
            print("  2. Enable the Gmail API")
            print("  3. Go to Credentials > Create > OAuth Client ID > Desktop App")
            print("  4. Download the JSON file")
            print()
            print("  The file will be named 'client_secret_<something>.json'")

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
            else:
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
                        # User gave a directory — search inside it
                        candidates = glob.glob(os.path.join(path, "client_secret_*.json"))
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
                            print("Look for 'client_secret_<something>.json' or 'credentials.json'\n")
                            continue

                    if not os.path.exists(path):
                        print(f"File not found: {path}\n")
                        continue

                    if not os.path.isfile(path):
                        print(f"Not a file: {path}\n")
                        continue

                    # Validate it's a real Google credentials file
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
                    break

    # Step 2: Authenticate
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

    # Step 3: Test connection
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

    # Done
    print("\n" + "=" * 50)
    print("  Setup complete!")
    print("=" * 50)
    print(f"\n  Run:  whid collect gmail")
    print(f"  Data: ~/Documents/WHID_Vaults/Gmail_Primary/")
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

        run_export(vault_name=args.vault, config=config)

    else:
        print(f"\nError: Unknown source '{args.source}'")
        print()
        print("Available collectors:")
        print("  gmail    Export your Gmail inbox")
        print()
        print("Coming soon: whatsapp, gdrive, telegram")
        sys.exit(1)


def cmd_groom(args, config):
    """Groom a vault."""
    from core.groomer import groom_vault

    vault_root = get_vault_root(config)

    if args.source == "gmail":
        vault_path = os.path.join(vault_root, f"Gmail_{args.vault}")
    else:
        vault_path = os.path.join(vault_root, args.source)

    if not os.path.exists(vault_path):
        print(f"\nError: Vault not found: {vault_path}")
        print()
        if args.source == "gmail":
            print("Run 'whid collect gmail' first to create your vault.")
        else:
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

    print("\nUpdated successfully!")


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

        total_entries = 0
        jsonl_files = 0
        for root, _dirs, files in os.walk(vault_path):
            for f in files:
                if f.endswith(".jsonl"):
                    jsonl_files += 1
                    try:
                        with open(os.path.join(root, f), "r") as fh:
                            total_entries += sum(1 for _ in fh)
                    except (OSError, PermissionError) as e:
                        logger.warning("Could not read %s: %s", os.path.join(root, f), e)

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
    collect_parser.add_argument("source", help="Data source (e.g., gmail)")
    collect_parser.add_argument(
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

    # whid status
    subparsers.add_parser("status", help="Show vault status overview")

    # whid update
    subparsers.add_parser("update", help="Pull the latest version from GitHub")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        print()
        print("Quick start:")
        print("  whid setup gmail      Guided setup (first time)")
        print("  whid collect gmail    Export your Gmail inbox")
        print("  whid groom gmail      Deduplicate and sort")
        print("  whid status           See your vaults")
        print("  whid update           Pull latest version")
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
    elif args.command == "groom":
        cmd_groom(args, config)
    elif args.command == "status":
        cmd_status(args, config)


if __name__ == "__main__":
    main()
