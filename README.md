<p align="center">
  <h1 align="center">Nomolo</h1>
  <p align="center"><strong>No More Loss. Your life. Your data. Your hard drive.</strong></p>
  <p align="center">The open-source framework for recovering, owning, and understanding your personal digital history.</p>
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a> &bull;
  <a href="#how-it-works">How It Works</a> &bull;
  <a href="#roadmap">Roadmap</a> &bull;
  <a href="#contributing">Contributing</a>
</p>

---

## The Problem

Your life is scattered across cloud services you don't control. Emails in Gmail. Conversations in WhatsApp. Photos in iCloud. Notes in Notion. Each platform holds a piece of your history — and none of them talk to each other.

If a service shuts down, changes its terms, or locks you out — that piece of your life disappears.

**Nomolo (NOMOLO)** recovers it. It pulls your data out of proprietary silos and into local, structured, AI-ready archives that you own forever.

## What It Does

NOMOLO is a **modular data archeology suite**. Each data source gets its own collector plugin. A universal groomer keeps everything clean. A self-healing mechanism makes sure nothing gets lost.

```
You ──> NOMOLO ──> Your Vault (local JSONL files, organized by date)
                    │
                    ├── Gmail_Primary/
                    │   ├── 2020/
                    │   ├── 2021/
                    │   ├── ...
                    │   └── 2025/
                    │
                    ├── WhatsApp_Personal/   (coming soon)
                    │
                    └── ... more collectors
```

**Your vault is yours.** Plain text. Grep-able. AI-ready. No databases, no proprietary formats, no vendor lock-in.

## Current Status

### Collectors

| Collector | Source | Status |
|-----------|--------|--------|
| **Gmail** | Gmail API (Batch) | **Live** — ~1000 msgs/sec |
| **Google Contacts** | People API | **Live** |
| **Google Calendar** | Calendar API / ICS | **Live** |
| **Books** | Goodreads / Audible CSV | **Live** |
| **YouTube** | Google Takeout JSON | **Live** |
| **Music** | Spotify JSON export | **Live** |
| **Finance** | PayPal / bank CSV | **Live** |
| **Shopping** | Amazon CSV | **Live** |
| **Notes** | Markdown / text files | **Live** |
| **Podcasts** | DB / CSV exports | **Live** |
| **Health** | Apple Health XML | **Live** |
| **Browser** | Chrome history (local) | **Live** |
| **Maps** | Google Maps Takeout | **Live** |
| **LinkedIn** | CSV export | **Live** |
| **Facebook** | JSON export | **Live** |
| **Instagram** | JSON export | **Live** |
| **WhatsApp** | _Next up_ | Planned |

### Features

| Feature | Status |
|---------|--------|
| Universal Groomer (dedup, sort, self-heal) | **Live** |
| Zstandard compression (~5x smaller vaults) | **Live** |
| Semantic vector search (ChromaDB + local embeddings) | **Live** |
| Claude Desktop integration (MCP server) | **Live** |
| Zsh tab completion | **Live** |

## How It Works

### 1. Collect

Each collector authenticates with a cloud service (or reads a local export) and pulls your data into a local vault as `.jsonl` files, organized by year and month.

```bash
nomolo collect gmail               # Gmail via API
nomolo collect contacts-google     # Google Contacts via API
nomolo collect calendar            # Google Calendar via API
nomolo collect books-goodreads export.csv   # Goodreads CSV
nomolo collect youtube takeout.json         # YouTube Takeout
```

### 2. Groom

The universal groomer deduplicates entries, sorts them chronologically, and validates structure — for any vault, from any collector.

```bash
nomolo groom gmail
```

### 3. Self-Heal (The Sniper)

After grooming, Nomolo compares what's on disk against what was previously collected. If records are missing (corrupted file, interrupted run, disk error), it writes a `missing_ids.txt` file. On the next collection run, the collector automatically enters **Sniper mode** — recovering only those specific records instead of scanning everything again.

```
Groomer detects gaps ──> missing_ids.txt ──> Collector recovers them ──> Clean vault
```

### 4. Vectorize & Search

Nomolo builds a local semantic search index over your vault data using ChromaDB and sentence-transformers. No API keys needed — everything runs on your machine.

```bash
nomolo vectorize              # index all vaults
nomolo search "tax documents from 2024"
nomolo search "meeting with John" -s gmail
```

### 5. Ask Claude (MCP Integration)

Connect Nomolo to Claude Desktop via the [Model Context Protocol](https://modelcontextprotocol.io). Claude can then search your personal data directly — emails, calendar, contacts, everything — using natural language.

```json
{
  "mcpServers": {
    "nomolo": {
      "command": "python3",
      "args": ["/path/to/Nomolo/mcp_server.py"]
    }
  }
}
```

Once connected, just ask Claude: *"Find emails about the Berlin trip"* or *"Who do I know at Google?"*

## Architecture

```
Nomolo/
├── nomolo.py               # CLI entry point
├── mcp_server.py           # MCP server for Claude Desktop
├── config.yaml             # All settings in one place
├── install.sh              # One-line installer
├── collectors/             # One plugin per data source
│   ├── gmail_collector.py  #   Gmail (API)
│   ├── google_contacts.py  #   Google Contacts (API)
│   ├── calendar_collector.py # Google Calendar (API / ICS)
│   ├── books.py            #   Goodreads / Audible (CSV)
│   ├── youtube.py          #   YouTube (Takeout JSON)
│   ├── music.py            #   Spotify (JSON)
│   ├── finance.py          #   PayPal / bank (CSV)
│   ├── health.py           #   Apple Health (XML)
│   └── ...                 #   + browser, maps, notes, podcasts, shopping, socials
├── core/
│   ├── groomer.py          # Universal dedup, sort, and Sniper logic
│   ├── vectordb.py         # ChromaDB semantic search engine
│   ├── vault.py            # Vault read/write (JSONL + Zstandard)
│   ├── cleaner.py          # RAG-optimized text cleaning
│   └── auth.py             # Google OAuth helper
├── completions/
│   └── nomolo.zsh          # Zsh tab completion
└── vaults/                 # Your data (gitignored, stays local)
    ├── Gmail_Primary/
    │   ├── 2024/
    │   │   ├── 01_January.jsonl.zst
    │   │   └── 02_February.jsonl.zst
    │   ├── _unknown/
    │   ├── processed_ids.txt
    │   └── missing_ids.txt
    ├── Contacts_Google/
    ├── Calendar/
    ├── Books/
    └── .vectordb/           # ChromaDB index (auto-generated)
```

## Quick Start

### Prerequisites

- Python 3.9+
- A Google account

### Install & Setup

```bash
git clone https://github.com/matjes993/nomolo.git
./Nomolo/install.sh
source ~/.zshrc   # (only needed once, to pick up the new PATH)
nomolo setup gmail
```

That's it. The install script handles Python, dependencies, and adds `nomolo` to your PATH. The setup wizard walks you through Google credentials.

The setup wizard:
1. Opens Google Cloud Console for you
2. Finds the downloaded credentials file automatically
3. Signs you in to Google (read-only access)
4. Tests the connection and shows your message count

### Run

```bash
nomolo collect gmail        # download your inbox
nomolo groom gmail          # deduplicate and sort
nomolo compress gmail       # compress vault files (~5x smaller)
nomolo vectorize            # build semantic search index
nomolo search "query"       # search your data
nomolo status               # see what you've got
nomolo update               # pull latest version
```

Your data is saved to `vaults/` inside the project folder (gitignored — never pushed to GitHub).

Multiple accounts? Just use different vault names:

```bash
nomolo collect gmail Work
nomolo collect gmail Personal
```

### Updating

```bash
nomolo update
```

That's it — works from any directory. Pulls the latest code and reinstalls dependencies if needed.

### Configuration

All settings live in `config.yaml`:

```yaml
# Store vaults anywhere — home dir, external drive, NAS, etc.
vault_root: vaults                # relative to project root, or use an absolute path

gmail:
  max_workers: 10       # Parallel batch workers
  batch_size: 100       # Messages per batch API call (max 100)
  page_size: 500        # Messages per listing page
  scope: https://www.googleapis.com/auth/gmail.readonly
  credentials_file: credentials.json
  token_file: token.json
```

## Design Principles

| Principle | What it means |
|-----------|---------------|
| **Read-only** | Your cloud data is never modified. Gmail uses `readonly` scope. |
| **Append-only collection** | New data is appended during collection, never overwritten. |
| **Atomic grooming** | Vault files are written via temp files — a crash mid-groom can't corrupt your data. |
| **Modular** | Each source is a standalone collector. The groomer works with any JSONL vault. |
| **No lock-in** | Plain `.jsonl` files. Read them with Python, `jq`, `grep`, or feed them to an LLM. |

## Roadmap

- [x] Gmail Collector with Batch API
- [x] Google Contacts, Calendar, Books, YouTube, Music, Finance, Shopping, Notes, Podcasts, Health, Browser, Maps, LinkedIn, Facebook, Instagram collectors
- [x] Universal Groomer with Sniper self-healing
- [x] Zstandard vault compression
- [x] Semantic vector search (ChromaDB)
- [x] Claude Desktop MCP integration
- [x] Zsh tab completion
- [ ] **WhatsApp Collector** (next)
- [ ] Google Drive / Docs export
- [ ] Telegram export
- [ ] iCloud Photos metadata
- [ ] Notion export
- [ ] Personal AI training pipeline (fine-tune on your own data)

## Adding a New Collector

Create a file in `collectors/`. A collector should:

1. Authenticate with the source API or parse an export file
2. Write records as JSONL to `Vaults/<Source>_<VaultName>/`
3. Track processed IDs in `processed_ids.txt`

The groomer handles deduplication and sorting automatically for any vault that follows this convention.

## Documentation

| Doc | Description |
|-----|-------------|
| [Google OAuth Setup](docs/GOOGLE_SETUP.md) | Step-by-step guide to get `credentials.json` |
| [Troubleshooting](docs/TROUBLESHOOTING.md) | Common errors and how to fix them |
| [Contributing](.github/CONTRIBUTING.md) | How to contribute to NOMOLO |

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](.github/CONTRIBUTING.md) for guidelines.

Whether it's a new collector, a bug fix, or documentation — all help is appreciated.

## About

The idea behind Nomolo was born in 2014 — the conviction that people should own their digital lives, not rent them from cloud platforms. The technology wasn't there yet. A decade later, it is.

<!-- TODO: Write the full origin story, motivation, and moonshot vision. -->

## License

MIT — see [LICENSE](LICENSE) for details.

---

<p align="center">
  <strong>Your data has a story. NOMOLO helps you read it.</strong>
</p>
