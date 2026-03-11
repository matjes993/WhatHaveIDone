#!/usr/bin/env python3
"""
WHID CLI — WhatHaveIDone command-line interface.

Usage:
    whid collect gmail [vault_name]
    whid groom gmail [vault_name]
    whid status
"""

import argparse
import logging
import os
import sys

import yaml

CONFIG_LOCATIONS = [
    os.path.join(os.getcwd(), "config.yaml"),
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml"),
    os.path.expanduser("~/.config/whid/config.yaml"),
]

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
    logger.info("Searched: %s", ", ".join(CONFIG_LOCATIONS))
    return {}


def get_vault_root(config):
    vault_root = os.path.expanduser(config.get("vault_root", "~/Documents/WHID_Vaults"))

    # Check if parent directory exists (e.g., if pointing to external drive)
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


def cmd_collect(args, config):
    """Run a collector."""
    if args.source == "gmail":
        config = validate_gmail_config(config)

        # Check for credentials.json before importing (faster feedback)
        creds_file = config.get("gmail", {}).get("credentials_file", "credentials.json")
        if not os.path.exists(creds_file):
            print(f"\nError: Google OAuth credentials not found: {os.path.abspath(creds_file)}")
            print()
            print("To get credentials.json:")
            print("  1. Go to https://console.cloud.google.com")
            print("  2. Create a project (or select existing)")
            print("  3. Enable the Gmail API (Library > search 'Gmail API')")
            print("  4. Go to APIs & Services > Credentials")
            print("  5. Create Credentials > OAuth Client ID > Desktop App")
            print("  6. Download the JSON and save as: credentials.json")
            print(f"  7. Place it here: {os.path.abspath('.')}/credentials.json")
            print()
            print("For detailed instructions, see: docs/GOOGLE_SETUP.md")
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


def cmd_status(args, config):
    """Show vault status."""
    vault_root = get_vault_root(config)

    if not os.path.exists(vault_root):
        print(f"No vaults found at {vault_root}")
        print()
        print("Get started:")
        print("  whid collect gmail    Export your Gmail inbox")
        return

    entries_found = False
    print(f"Vault root: {vault_root}\n")

    for entry in sorted(os.listdir(vault_root)):
        vault_path = os.path.join(vault_root, entry)
        if not os.path.isdir(vault_path):
            continue

        entries_found = True

        # Count JSONL entries
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

        # Check processed IDs
        processed_log = os.path.join(vault_path, "processed_ids.txt")
        processed = 0
        if os.path.exists(processed_log):
            try:
                with open(processed_log, "r") as f:
                    processed = sum(1 for line in f if line.strip())
            except (OSError, PermissionError) as e:
                logger.warning("Could not read processed log: %s", e)

        # Check for ghosts
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
        print("  Get started: whid collect gmail")


def main():
    parser = argparse.ArgumentParser(
        prog="whid",
        description="WhatHaveIDone — Your life. Your data. Your hard drive.",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

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

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        print()
        print("Quick start:")
        print("  whid collect gmail    Export your Gmail inbox")
        print("  whid groom gmail      Deduplicate and sort")
        print("  whid status           See your vaults")
        sys.exit(0)

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    config = load_config()

    if args.command == "collect":
        cmd_collect(args, config)
    elif args.command == "groom":
        cmd_groom(args, config)
    elif args.command == "status":
        cmd_status(args, config)


if __name__ == "__main__":
    main()
