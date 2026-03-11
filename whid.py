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


def load_config():
    for path in CONFIG_LOCATIONS:
        if os.path.exists(path):
            with open(path, "r") as f:
                return yaml.safe_load(f)
    return {}


def get_vault_root(config):
    return os.path.expanduser(config.get("vault_root", "~/Documents/WHID_Vaults"))


def cmd_collect(args, config):
    """Run a collector."""
    if args.source == "gmail":
        from collectors.gmail_collector import run_export

        run_export(vault_name=args.vault, config=config)
    else:
        print(f"Unknown source: {args.source}")
        print("Available collectors: gmail")
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
        print(f"Vault not found: {vault_path}")
        print("Run 'whid collect gmail' first to create it.")
        sys.exit(1)

    groom_vault(vault_path)


def cmd_status(args, config):
    """Show vault status."""
    vault_root = get_vault_root(config)

    if not os.path.exists(vault_root):
        print(f"No vaults found at {vault_root}")
        return

    print(f"Vault root: {vault_root}\n")

    for entry in sorted(os.listdir(vault_root)):
        vault_path = os.path.join(vault_root, entry)
        if not os.path.isdir(vault_path):
            continue

        # Count JSONL entries
        total_entries = 0
        jsonl_files = 0
        for root, _dirs, files in os.walk(vault_path):
            for f in files:
                if f.endswith(".jsonl"):
                    jsonl_files += 1
                    with open(os.path.join(root, f), "r") as fh:
                        total_entries += sum(1 for _ in fh)

        # Check processed IDs
        processed_log = os.path.join(vault_path, "processed_ids.txt")
        processed = 0
        if os.path.exists(processed_log):
            with open(processed_log, "r") as f:
                processed = sum(1 for line in f if line.strip())

        # Check for ghosts
        missing_log = os.path.join(vault_path, "missing_ids.txt")
        ghosts = 0
        if os.path.exists(missing_log):
            with open(missing_log, "r") as f:
                ghosts = sum(1 for line in f if line.strip())

        status = "OK"
        if ghosts > 0:
            status = f"GHOSTS ({ghosts} missing)"

        print(f"  {entry}")
        print(f"    Entries:   {total_entries:,} across {jsonl_files} files")
        print(f"    Processed: {processed:,}")
        print(f"    Status:    {status}")
        print()


def main():
    parser = argparse.ArgumentParser(
        prog="whid",
        description="WhatHaveIDone — Your life. Your data. Your hard drive.",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # whid collect
    collect_parser = subparsers.add_parser("collect", help="Collect data from a source")
    collect_parser.add_argument("source", help="Data source (e.g., gmail)")
    collect_parser.add_argument("vault", nargs="?", default="Primary", help="Vault name (default: Primary)")

    # whid groom
    groom_parser = subparsers.add_parser("groom", help="Groom a vault (deduplicate, sort, detect ghosts)")
    groom_parser.add_argument("source", help="Data source (e.g., gmail)")
    groom_parser.add_argument("vault", nargs="?", default="Primary", help="Vault name (default: Primary)")

    # whid status
    subparsers.add_parser("status", help="Show vault status overview")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
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
