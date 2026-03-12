# Troubleshooting NOMOLO

## Installation issues

### "pip install -e ." fails

Make sure you're in a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate    # macOS/Linux
# or: venv\Scripts\activate  # Windows
pip install -e .
```

### "command not found: nomolo"

The `nomolo` command is only available inside the virtual environment:

```bash
source venv/bin/activate
nomolo --help
```

If you installed with `pip install -e .` and it still doesn't work, check that the venv's `bin` directory is on your PATH:

```bash
which nomolo
```

### Python version errors

NOMOLO requires Python 3.9+. Check your version:

```bash
python3 --version
```

## Gmail collector issues

See [GOOGLE_SETUP.md](GOOGLE_SETUP.md) for detailed OAuth setup and Gmail-specific errors.

### "Nothing new to process — vault is up to date"

This means all your messages have already been downloaded. If you think messages are missing:

1. Run `nomolo groom gmail` to check for gaps
2. Run `nomolo collect gmail` again — if ghosts are found, Sniper mode activates automatically
3. Check `nomolo status` to see entry counts

### Collection was interrupted

No problem. NOMOLO tracks progress in `processed_ids.txt`. Just run `nomolo collect gmail` again and it picks up where it left off.

### Some messages failed

After a run with failures, NOMOLO tells you how many failed. To recover:

```bash
nomolo groom gmail       # detects missing records
nomolo collect gmail     # recovers them via Sniper mode
```

Failures are usually caused by rate limiting (transient) or deleted messages (permanent). Check the extraction log for details:

```bash
cat vaults/Gmail_Primary/extraction.log
```

## Groomer issues

### "Vault not found"

```
Error: Vault not found: .../vaults/Gmail_Primary
```

You need to run `nomolo collect gmail` first to create the vault.

### Corrupted JSONL files

If the groomer reports skipped entries, your JSONL files may have corruption (e.g., from an interrupted write). The groomer safely skips bad entries and keeps the rest.

To investigate:

```bash
# Find which files have issues — look for the warnings in the log
nomolo groom gmail 2>&1 | grep "Skipping"
```

The groomer uses atomic writes, so corruption from NOMOLO itself is very unlikely. If you see it, it may be a disk issue.

## Config issues

### "config.yaml has invalid YAML syntax"

NOMOLO tells you the exact line and column of the error. Common mistakes:

```yaml
# Wrong — tabs are not allowed in YAML
gmail:
	max_workers: 10

# Correct — use spaces
gmail:
  max_workers: 10
```

```yaml
# Wrong — missing space after colon
vault_root:vaults

# Correct
vault_root: vaults
```

Use [yamlchecker.com](https://yamlchecker.com) to validate your config.

### External drive / NAS path

If you store vaults on an external drive:

```yaml
vault_root: /Volumes/MyDrive/my_vaults
```

Make sure the drive is mounted before running NOMOLO. If it's not, you'll see:

```
Error: Vault root parent directory does not exist: /Volumes/MyDrive
Hint: Is your external drive connected?
```

## General issues

### How much disk space do I need?

Rough estimates for Gmail:
- 10,000 emails: ~200 MB
- 50,000 emails: ~1 GB
- 100,000 emails: ~2 GB

These are approximate — emails with lots of text will be larger.

### Where is my data?

By default: `vaults/` inside the project folder.

Check `config.yaml` for your `vault_root` setting, or run:

```bash
nomolo status
```

### Can I move my vaults?

Yes. Move the folder and update `vault_root` in `config.yaml`:

```yaml
vault_root: /new/path/to/my_vaults
```

### How do I start over?

To re-download everything for a vault:

```bash
rm -rf vaults/Gmail_Primary
nomolo collect gmail
```

To remove all NOMOLO data:

```bash
rm -rf vaults/
rm token.json
```
