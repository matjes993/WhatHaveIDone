# Nomolo Architecture — Knowledge Graph & Scroll System

## Three Pillars

```
        ┌─────────┐
        │   WEB   │  ← eyes (display, interact, gamify)
        └────┬────┘
             │ queries + user input
        ┌────┴────┐
        │  AGENT  │  ← brain (reason, enrich, warn, learn)
        └────┬────┘
             │ reads + writes
        ┌────┴────┐
        │  CORE   │  ← bones (store, index, serve)
        └─────────┘
```

Dependency rule: `web/` → `agent/` → `core/`. Never reverse.

- **Core** — pure data engine. No LLM calls, no opinions. Vault, knowledge graph, search, scroll engine, collectors.
- **Agent** — all LLM reasoning. Enrichment, entity resolution (ambiguous cases), hypothesis resolution, scroll review, insight generation. Every call metered.
- **Web** — gamified UI. Templates, CSS, JS, RPG layer. Never calls an LLM directly.

## Three Storage Layers

```
JSONL Vault          →  permanent, raw, source of truth (text files)
     ↓ (build from)
SQLite Graph         →  structured understanding, relationships, provenance (one .db file)
     ↓ (index into)
ChromaDB Vectors     →  semantic search capability (rebuildable)
```

**Design principle:** The JSONL vault is the single source of truth. Everything else is a derived, rebuildable index.

## Knowledge Graph Schema (SQLite)

### entities
| Column | Type | Description |
|--------|------|-------------|
| id | TEXT (UUID) | Stable entity identifier |
| type | TEXT | person, organization, place, event, message, file, bookmark, note, account |
| properties | JSON | All fields for this entity type |
| created_at | DATETIME | When created in system |
| updated_at | DATETIME | Last modification |

### relationships
| Column | Type | Description |
|--------|------|-------------|
| id | TEXT (UUID) | Relationship identifier |
| type | TEXT | KNOWS, WORKS_AT, SENT, RECEIVED, ATTENDED, LOCATED_AT, MENTIONS, TAGGED |
| source_id | TEXT | From entity |
| target_id | TEXT | To entity |
| properties | JSON | Role, context, etc. |
| valid_from | DATETIME | When true in real world |
| valid_to | DATETIME | When stopped being true (NULL = current) |
| recorded_at | DATETIME | When we learned this |
| superseded_at | DATETIME | When we replaced this record (NULL = current) |

### provenance
| Column | Type | Description |
|--------|------|-------------|
| id | TEXT | Provenance record ID |
| target_type | TEXT | entity, relationship, property |
| target_id | TEXT | What this provenance is about |
| source_name | TEXT | gmail, apple_contacts, whatsapp, etc. |
| source_record_id | TEXT | Original vault entry ID |
| source_field | TEXT | Which field this fact came from |
| confidence | REAL | 1.0 = direct import, lower = inferred |
| derivation | TEXT | NULL for imports, "entity_resolution" for merges, etc. |
| ingested_at | DATETIME | When ingested |

### identifiers
| Column | Type | Description |
|--------|------|-------------|
| entity_id | TEXT | Which entity |
| system | TEXT | isbn, upc, ean, doi, imdb, spotify, asin, orcid, wikidata |
| value | TEXT | The actual identifier |
| verified | BOOLEAN | Confirmed or inferred? |

### annotations (append-only)
| Column | Type | Description |
|--------|------|-------------|
| id | TEXT | Annotation ID |
| target_id | TEXT | Record/entity annotated |
| field | TEXT | What field was added/derived |
| value | JSON | The annotation value |
| by_type | TEXT | snippet, model, user, system |
| by_id | TEXT | "community/alice/isbn-enricher@2.1.0" or "claude-haiku-4-5" or "matthias" |
| by_version | TEXT | Version of the scroll or model |
| cost_tokens | INTEGER | 0 for scrolls |
| cost_usd | REAL | 0 for scrolls |
| created_at | DATETIME | When annotation was made |
| pipeline_step | TEXT | extraction, resolution, enrichment, scoring, detection |
| trigger | TEXT | on_ingest, scheduled, user_request, scroll:isbn-detector |
| parent_ids | JSON | Which prior annotations fed into this one |
| status | TEXT | active, superseded, removed |

### hypotheses
| Column | Type | Description |
|--------|------|-------------|
| id | TEXT | Hypothesis ID |
| type | TEXT | identity_merge, missing_link, data_gap, unlinked_entity, anomaly |
| entity_ids | JSON | Involved entities |
| confidence | REAL | 0.0-1.0 |
| evidence | JSON | What triggered this |
| status | TEXT | open, confirmed, denied, expired |
| resolution | TEXT | auto, llm, user, new_data, scroll |
| created_at | DATETIME | |
| resolved_at | DATETIME | |

## Entity Resolution — 3-Tier Pipeline

1. **Deterministic** — exact match on email, phone, URL. Instant merge, confidence 1.0.
2. **Probabilistic** — Jaro-Winkler on names with blocking on first letter. Auto-merge >0.95, review 0.7-0.95.
3. **Graph-based** — co-occurrence patterns (shared events, email threads). Boosts merge probability.

## Canonical Record Types

Every data source adapter maps to these canonical types:

| Type | Key Fields |
|------|-----------|
| Person | name, emails[], phones[], orgs[], photo_url |
| Organization | name, domain, type |
| Message | subject, body, from, to[], cc[], date, thread_id |
| Event | title, start, end, location, attendees[], recurrence |
| Place | name, lat, lng, address |
| File | name, path, mime_type, size, created, modified |
| Bookmark | url, title, tags[], created |
| Note | title, body, created, modified |
| Account | provider, username, email |

Adding a new source = writing one adapter. Zero changes to graph builder or entity resolution.

## Scrolls — Community Plugin System

Scrolls are community-created deterministic code that enhances every pipeline step at zero token cost.

### Scroll Types

| Type | Category | What It Does |
|------|----------|-------------|
| Plunder Scroll | Collector | Extracts data from a source |
| Cipher Scroll | Parser/Normalizer | Decodes raw data into structure |
| Cartographer Scroll | Resolver | Maps entities across sources |
| Alchemist Scroll | Enricher | Derives new fields from existing data |
| Lookout Scroll | Detector | Spots anomalies, patterns, risks |
| Compactor Scroll | Compressor | Deduplicates, summarizes, compacts |
| Appraiser Scroll | Scorer | Rates importance, relevance, quality |

### Safety Model — Two Tiers

**Safe Scrolls:** Pure computation, restricted imports (`json`, `re`, `datetime`, `collections`, `hashlib`, `urllib.parse`). No network access. Run without review.

**Power Scrolls:** External dependencies, network access (domain-allowlisted in manifest). Require agent code review + static analysis + user approval.

### Scroll Contract

```python
@dataclass
class SnippetInput:
    record: dict          # current record (read-only)
    context: dict         # graph neighbors (read-only)

@dataclass
class SnippetOutput:
    annotations: dict     # new fields to ADD
    relationships: list   # new relationships to CREATE
    hypotheses: list      # suspected patterns to FLAG
    confidence: float     # how confident
```

Scrolls can NEVER: delete, modify existing data, access filesystem, access network (safe tier), import restricted modules.

### Scroll Identity

Format: `{author}/{slug}@{version}` — e.g. `community/alice/isbn-enricher@2.1.0`

Slug: lowercase, hyphens, max 40 chars.

### Scroll Chaining

Scrolls declare dependencies via `requires_annotations` in manifest. The engine runs in waves:
1. Run scrolls with no dependencies
2. Check which annotations were created
3. Run scrolls whose requirements are now satisfied
4. Repeat until no more can fire

### Marketplace ("The Archive")

Submit → agent + static analysis review → listed as Unverified → community installs + votes → earns Trusted/Legendary badges.

Author ranks: Scribe (1 scroll) → Cartographer (5, avg 4.0+) → Sage (15, 1K+ installs) → Archmage (50, 10K+ installs, 4.5+ avg).

## Agent Layer

All LLM calls live in `agent/`. Involved at every pipeline step (optional, metered):

- **Enricher** — fills gaps scrolls couldn't handle
- **Resolver** — resolves ambiguous entity merges
- **Hypotheses** — generates + auto-resolves hypotheses
- **Anomalies** — detects problems, privacy risks, data gaps
- **Insights** — deep analysis, trends, patterns
- **Advisor** — proactive suggestions (archive, forget, connect)
- **Scroll reviewer** — reviews submitted scrolls for safety
- **Scroll generator** — learns patterns → drafts new scrolls (flywheel)
- **Metering** — tracks token cost per operation
- **Router** — model selection (Haiku/Sonnet/Opus by task complexity)

## Machine Adaptivity

Auto-detect hardware at startup → performance tier:

| Setting | Lite (8GB) | Standard (16GB) | Heavy (32GB+) |
|---------|-----------|-----------------|---------------|
| Embedding model | MiniLM-L6 (384d) | MiniLM-L6 (384d) | nomic-embed (768d) |
| Batch size | 100 | 500 | 2000 |
| Concurrent scrolls | 2 | 4 | 8 |
| Multimodal | Off | OCR only | Full |

User can override. Re-embedding on upgrade is an explicit action.

## Portability

Moving to a new machine:
1. Copy JSONL vault folder → data is there
2. First startup detects new hardware tier
3. Rebuilds SQLite knowledge graph (identical)
4. Rebuilds vector embeddings at appropriate dimensions

## Module Structure

```
core/
├── vault/                     # JSONL raw archive
├── knowledge/                 # Knowledge graph
│   ├── schema.py              # Entity types, relationship types, canonical records
│   ├── graph_store.py         # SQLite CRUD (entities, relationships, provenance, identifiers)
│   ├── resolver.py            # Deterministic entity resolution (no LLM)
│   ├── temporal.py            # Bitemporal query helpers
│   ├── identifiers.py         # External ID systems (ISBN, UPC, DOI, etc.)
│   └── forgetter.py           # Cascade deletion + forgetting log
├── scrolls/                   # Scroll engine
│   ├── sandbox.py             # Safe execution environment
│   ├── runner.py              # Reactive pipeline: run scrolls, chain by dependencies
│   ├── annotations.py         # Append-only annotation layer with full provenance
│   ├── registry.py            # Installed scrolls, versions, ordering
│   ├── marketplace.py         # Fetch/install/update from The Archive
│   └── manifest.py            # Manifest parser + validator
├── search/                    # FTS5 + ChromaDB
├── machine_profile.py         # Auto-detect hardware, set performance tier
├── dead_letter.py             # Quarantine for failed records
├── schema_version.py          # Read-time schema adaptation
├── quality.py                 # Data quality metrics
└── collectors/                # Raw data extraction

agent/
├── enricher.py                # LLM-powered extraction cleanup
├── resolver.py                # Probabilistic + LLM entity resolution
├── hypotheses.py              # Generate + auto-resolve hypotheses
├── anomalies.py               # Detect problems, privacy risks
├── insights.py                # Deep analysis, patterns, trends
├── advisor.py                 # Proactive suggestions
├── learnings.py               # Local + shareable learning store
├── scroll_reviewer.py         # Reviews submitted scrolls for safety
├── scroll_generator.py        # Learns patterns → drafts new scrolls
├── metering.py                # Token cost tracking
├── router.py                  # Model selection by task
└── telemetry.py               # Anonymous aggregate stats (opt-in)

web/
├── server.py
├── rpg.py
├── templates/
│   ├── archive.html           # The Archive — browse/install/rate scrolls
│   └── ...
└── static/
```

## ETL Best Practices

- Dead letter queue for failed records
- Schema versioning (read-time adaptation)
- Data quality metrics (freshness, completeness, accuracy, coverage)
- Checkpointing for resumable collection runs
- Idempotent ingestion (source_name + source_id as dedup key)
- Backpressure (configurable batch sizes based on machine tier)
