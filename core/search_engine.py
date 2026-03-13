"""
NOMOLO Hybrid Search Engine
Combines BM25 keyword search (SQLite FTS5) + vector semantic search (ChromaDB)
+ metadata boosting + temporal reranking via Reciprocal Rank Fusion.

This is the primary search interface. It produces significantly better results
than pure vector search alone by fusing lexical and semantic signals.
"""

import json
import logging
import os
import re
import sqlite3
import statistics
import threading
from datetime import datetime

from core.vault import read_all_entries, count_entries
from core.vectordb import (
    detect_embedding_field,
    search as vector_search,
    EMBEDDING_FIELDS,
)

logger = logging.getLogger("nomolo.search")

# Default FTS database location (relative to vault_root)
DEFAULT_FTS_DIR = ".searchdb"
DEFAULT_FTS_FILE = "fts.sqlite3"

# RRF constant (standard value from the original RRF paper)
RRF_K = 60

# BM25 column weights: subject and from_field weighted 3x higher than body
# FTS5 column order: entry_id(U), collection(U), source(U), subject, from_field,
#                    to_field, date_str(U), title, tags, body
# Only non-UNINDEXED columns get weights: subject, from_field, to_field, title, tags, body
BM25_WEIGHTS = "3.0 3.0 1.0 2.0 1.5 1.0"

# FTS5 special operators that need escaping in user queries
_FTS5_SPECIAL = re.compile(r'\b(AND|OR|NOT|NEAR)\b', re.IGNORECASE)
_FTS5_CHARS = re.compile(r'["\(\)\*\:\^]')

# Temporal keywords for reranking
_TEMPORAL_ASC = {"first", "earliest", "oldest", "original", "initially"}
_TEMPORAL_DESC = {"last", "latest", "recent", "newest", "most recent", "recently"}

# Common English words to exclude from name detection
_COMMON_WORDS = {
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "shall", "can", "need", "must", "about",
    "from", "with", "for", "not", "but", "what", "all", "when", "who",
    "how", "where", "which", "their", "there", "this", "that", "these",
    "those", "then", "than", "they", "them", "and", "any", "some", "each",
    "email", "emails", "message", "messages", "contact", "contacts",
    "note", "notes", "invoice", "invoices", "receipt", "receipts",
    "order", "orders", "payment", "payments", "first", "last", "latest",
    "recent", "newest", "oldest", "earliest", "between", "after", "before",
    "sent", "received", "wrote", "said", "asked", "told", "called",
    "meeting", "meetings", "event", "events", "book", "books", "song",
    "songs", "video", "videos", "podcast", "podcasts", "search", "find",
}

# Thread-local storage for SQLite connections
_local = threading.local()


# ---------------------------------------------------------------------------
# SQLite FTS5 Database Setup
# ---------------------------------------------------------------------------

def _get_fts_db_path(vault_root, config=None):
    """Resolve the FTS database file path."""
    config = config or {}
    search_cfg = config.get("search", {})
    fts_dir = search_cfg.get("fts_dir", os.path.join(vault_root, DEFAULT_FTS_DIR))
    fts_dir = os.path.expanduser(fts_dir)
    if not os.path.isabs(fts_dir):
        fts_dir = os.path.join(vault_root, fts_dir)
    return os.path.join(fts_dir, DEFAULT_FTS_FILE)


def _get_connection(db_path):
    """
    Get a thread-local SQLite connection. Creates tables if needed.
    Each thread gets its own connection for thread safety.
    """
    key = f"conn_{db_path}"
    conn = getattr(_local, key, None)
    if conn is None:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        _ensure_tables(conn)
        setattr(_local, key, conn)
    return conn


def _ensure_tables(conn):
    """Create FTS5 and metadata tables if they don't exist."""
    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS entries_fts USING fts5(
            entry_id UNINDEXED,
            collection UNINDEXED,
            source UNINDEXED,
            subject,
            from_field,
            to_field,
            date_str UNINDEXED,
            title,
            tags,
            body,
            tokenize='porter unicode61'
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entry_meta (
            entry_id TEXT PRIMARY KEY,
            collection TEXT,
            source TEXT,
            date_str TEXT,
            year INTEGER,
            month INTEGER,
            subject TEXT,
            from_field TEXT,
            to_field TEXT,
            title TEXT,
            tags TEXT
        )
    """)
    conn.commit()


# ---------------------------------------------------------------------------
# FTS5 Indexing
# ---------------------------------------------------------------------------

def index_vault(vault_path, vault_dir_name, config=None):
    """
    Index a single vault directory into the FTS5 database.
    Reads all JSONL entries, inserts into FTS5 + metadata table.
    Skips entries already indexed (check by entry_id).

    Returns (new_count, skipped_count).
    """
    config = config or {}
    vault_root = os.path.dirname(vault_path)
    db_path = _get_fts_db_path(vault_root, config)
    conn = _get_connection(db_path)

    # Determine embedding field for this vault type
    embedding_field = detect_embedding_field(vault_dir_name)
    if not embedding_field:
        logger.debug("No embedding field mapping for vault: %s", vault_dir_name)
        return 0, 0

    # Normalize collection name to match ChromaDB convention
    collection = vault_dir_name.lower().replace(" ", "_")
    collection = "".join(c if c.isalnum() or c == "_" else "_" for c in collection)
    if len(collection) < 3:
        collection = collection + "___"

    # Get existing entry IDs to skip
    existing = set()
    try:
        cursor = conn.execute(
            "SELECT entry_id FROM entry_meta WHERE collection = ?", (collection,)
        )
        existing = {row[0] for row in cursor.fetchall()}
    except Exception:
        pass

    new_count = 0
    skipped_count = 0
    batch_fts = []
    batch_meta = []
    BATCH = 500

    for entry in read_all_entries(vault_path):
        entry_id = entry.get("id", "")
        if not entry_id:
            skipped_count += 1
            continue

        if entry_id in existing:
            skipped_count += 1
            continue

        # Extract fields
        embed_text = entry.get(embedding_field, "")
        if not embed_text or not embed_text.strip():
            skipped_count += 1
            continue

        subject = str(entry.get("subject", ""))[:500]
        from_field = str(entry.get("from", ""))[:200]
        to_field = str(entry.get("to", ""))[:200]
        date_str = str(entry.get("date", ""))[:50]
        title = str(entry.get("title", ""))[:500]
        year = entry.get("year", 0)
        month = entry.get("month", 0)

        tags_raw = entry.get("tags", [])
        if isinstance(tags_raw, list):
            tags = " ".join(str(t) for t in tags_raw[:20])
        else:
            tags = str(tags_raw)[:200]

        # Use the embedding text as the body for full-text search
        body = embed_text[:10000]  # Cap body size for FTS

        batch_fts.append((
            entry_id, collection, vault_dir_name,
            subject, from_field, to_field, date_str,
            title, tags, body,
        ))
        batch_meta.append((
            entry_id, collection, vault_dir_name,
            date_str, year, month,
            subject, from_field, to_field, title, tags,
        ))

        new_count += 1

        if len(batch_fts) >= BATCH:
            _flush_batch(conn, batch_fts, batch_meta)
            print(f"\r    Indexing {vault_dir_name}: {new_count:,} entries...", end="", flush=True)
            batch_fts = []
            batch_meta = []

    # Flush remaining
    if batch_fts:
        _flush_batch(conn, batch_fts, batch_meta)

    if new_count > 0:
        print(f"\r    Indexing {vault_dir_name}: {new_count:,} entries... done")

    return new_count, skipped_count


def _flush_batch(conn, batch_fts, batch_meta):
    """Insert a batch of entries into FTS5 and metadata tables."""
    conn.executemany(
        """INSERT INTO entries_fts
           (entry_id, collection, source, subject, from_field, to_field,
            date_str, title, tags, body)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        batch_fts,
    )
    conn.executemany(
        """INSERT OR IGNORE INTO entry_meta
           (entry_id, collection, source, date_str, year, month,
            subject, from_field, to_field, title, tags)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        batch_meta,
    )
    conn.commit()


def index_all(vault_root, config=None, source_filter=None):
    """
    Index all vault directories into the FTS5 database.
    Same pattern as vectordb.vectorize_all().

    Returns dict of {vault_dir: {new, skipped}}.
    """
    config = config or {}
    if not os.path.isdir(vault_root):
        print(f"  Vault root not found: {vault_root}")
        return {}

    entries = sorted(os.listdir(vault_root))
    results = {}

    for entry in entries:
        vault_path = os.path.join(vault_root, entry)
        if not os.path.isdir(vault_path):
            continue
        if entry.startswith("."):
            continue

        # Apply filter if given
        if source_filter and entry != source_filter:
            continue

        # Check if this vault type has an embedding field mapping
        if not detect_embedding_field(entry):
            logger.debug("Skipping %s — no embedding field mapping", entry)
            continue

        # Check if vault has any entries
        total, _ = count_entries(vault_path)
        if total == 0:
            continue

        new_count, skipped = index_vault(vault_path, entry, config)
        results[entry] = {"new": new_count, "skipped": skipped}

        if new_count > 0:
            db_path = _get_fts_db_path(vault_root, config)
            total_indexed = get_fts_entry_count(db_path)
            print(f"    New: {new_count:,} | Total in FTS: {total_indexed:,}")
        elif skipped > 0:
            print(f"    {entry}: up to date")

    return results


# ---------------------------------------------------------------------------
# FTS5 Query Escaping
# ---------------------------------------------------------------------------

def _escape_fts_query(query):
    """
    Escape a user query for safe use in FTS5 MATCH.
    - Wraps each token in double quotes to escape special chars
    - Preserves multi-word phrase searching
    """
    if not query or not query.strip():
        return '""'

    # Remove FTS5 special characters
    cleaned = _FTS5_CHARS.sub(" ", query)
    # Remove FTS5 operators used as bare words
    cleaned = _FTS5_SPECIAL.sub("", cleaned)

    # Split into tokens and quote each one
    tokens = cleaned.split()
    if not tokens:
        return '""'

    # Quote each token individually so FTS5 treats them as literals
    quoted = " ".join(f'"{t}"' for t in tokens if t.strip())
    return quoted if quoted else '""'


def _is_name_like(query):
    """
    Detect if a query looks like it contains a person's name.
    Heuristic: short (1-3 words), has capitalized words that aren't common English.
    """
    words = query.strip().split()
    if len(words) > 4:
        return False

    capitalized = [w for w in words if w[0:1].isupper() and w.lower() not in _COMMON_WORDS]
    return len(capitalized) >= 1


# ---------------------------------------------------------------------------
# BM25 Search
# ---------------------------------------------------------------------------

def bm25_search(db_path, query, n_results=50, collection_filter=None, year_filter=None):
    """
    Search the FTS5 index using BM25 ranking.

    Returns list of dicts: {entry_id, collection, source, bm25_score, metadata}
    """
    conn = _get_connection(db_path)
    escaped_query = _escape_fts_query(query)

    if not escaped_query or escaped_query == '""':
        return []

    results = []

    # Main BM25 search across all indexed columns
    sql = """
        SELECT
            entries_fts.entry_id,
            entries_fts.collection,
            entries_fts.source,
            bm25(entries_fts, 3.0, 3.0, 1.0, 2.0, 1.5, 1.0) AS rank,
            m.date_str, m.year, m.month, m.subject, m.from_field,
            m.to_field, m.title, m.tags
        FROM entries_fts
        JOIN entry_meta m ON entries_fts.entry_id = m.entry_id
        WHERE entries_fts MATCH ?
    """
    params = [escaped_query]

    if collection_filter:
        placeholders = ",".join("?" for _ in collection_filter)
        sql += f" AND entries_fts.collection IN ({placeholders})"
        params.extend(collection_filter)

    if year_filter:
        sql += " AND m.year = ?"
        params.append(year_filter)

    sql += " ORDER BY rank LIMIT ?"
    params.append(n_results)

    try:
        cursor = conn.execute(sql, params)
        seen = set()
        for row in cursor.fetchall():
            eid = row[0]
            if eid in seen:
                continue
            seen.add(eid)
            results.append({
                "entry_id": eid,
                "collection": row[1],
                "source": row[2],
                "bm25_score": row[3],  # BM25 scores are negative; lower = better
                "metadata": {
                    "date": row[4],
                    "year": row[5],
                    "month": row[6],
                    "subject": row[7],
                    "from": row[8],
                    "to": row[9],
                    "title": row[10],
                    "tags": row[11],
                    "source": row[2],
                    "entry_id": eid,
                },
            })
    except Exception as e:
        logger.warning("BM25 search failed: %s (query: %s)", e, escaped_query)

    # If query looks like a name, also do a targeted name search
    if _is_name_like(query) and len(results) < n_results:
        _add_name_matches(conn, query, results, n_results, collection_filter, year_filter)

    return results


def _add_name_matches(conn, query, results, n_results, collection_filter, year_filter):
    """
    Supplement BM25 results with targeted from_field/to_field/subject matches
    when the query looks like a person's name.
    """
    existing_ids = {r["entry_id"] for r in results}

    # Search specifically in from_field and to_field columns
    for column in ["from_field", "to_field", "subject"]:
        escaped = _escape_fts_query(query)
        col_query = f"{column}: {escaped}"

        sql = """
            SELECT
                entries_fts.entry_id,
                entries_fts.collection,
                entries_fts.source,
                bm25(entries_fts, 3.0, 3.0, 1.0, 2.0, 1.5, 1.0) AS rank,
                m.date_str, m.year, m.month, m.subject, m.from_field,
                m.to_field, m.title, m.tags
            FROM entries_fts
            JOIN entry_meta m ON entries_fts.entry_id = m.entry_id
            WHERE entries_fts MATCH ?
        """
        params = [col_query]

        if collection_filter:
            placeholders = ",".join("?" for _ in collection_filter)
            sql += f" AND entries_fts.collection IN ({placeholders})"
            params.extend(collection_filter)

        if year_filter:
            sql += " AND m.year = ?"
            params.append(year_filter)

        sql += " ORDER BY rank LIMIT ?"
        params.append(n_results)

        try:
            cursor = conn.execute(sql, params)
            for row in cursor.fetchall():
                eid = row[0]
                if eid in existing_ids:
                    continue
                existing_ids.add(eid)
                # Boost the score for name matches (make rank more negative = better)
                results.append({
                    "entry_id": eid,
                    "collection": row[1],
                    "source": row[2],
                    "bm25_score": row[3] * 1.5,  # Boost name matches
                    "metadata": {
                        "date": row[4],
                        "year": row[5],
                        "month": row[6],
                        "subject": row[7],
                        "from": row[8],
                        "to": row[9],
                        "title": row[10],
                        "tags": row[11],
                        "source": row[2],
                        "entry_id": eid,
                    },
                })
        except Exception as e:
            logger.debug("Name search on %s failed: %s", column, e)


# ---------------------------------------------------------------------------
# Reciprocal Rank Fusion (RRF)
# ---------------------------------------------------------------------------

def _compute_rrf_scores(bm25_results, vector_results):
    """
    Merge BM25 and vector search results using Reciprocal Rank Fusion.
    score = sum(1 / (k + rank)) for each result across both lists.

    Returns dict of entry_id -> {rrf_score, metadata, collection, source}.
    """
    scores = {}

    # BM25 results are already sorted by rank (best first)
    for rank, r in enumerate(bm25_results, 1):
        eid = r["entry_id"]
        if eid not in scores:
            scores[eid] = {
                "rrf_score": 0.0,
                "metadata": r.get("metadata", {}),
                "collection": r.get("collection", ""),
                "source": r.get("source", ""),
                "bm25_rank": rank,
            }
        scores[eid]["rrf_score"] += 1.0 / (RRF_K + rank)
        scores[eid]["bm25_rank"] = rank

    # Vector results sorted by distance (lower = better)
    for rank, r in enumerate(vector_results, 1):
        meta = r.get("metadata", {})
        eid = meta.get("entry_id", r.get("id", ""))
        if not eid:
            continue

        if eid not in scores:
            scores[eid] = {
                "rrf_score": 0.0,
                "metadata": meta,
                "collection": r.get("collection", ""),
                "source": meta.get("source", ""),
            }
        scores[eid]["rrf_score"] += 1.0 / (RRF_K + rank)

        # Merge metadata — vector results may have richer metadata
        for k, v in meta.items():
            if v and not scores[eid]["metadata"].get(k):
                scores[eid]["metadata"][k] = v

        # Store the snippet from vector search
        if r.get("document"):
            scores[eid]["snippet"] = r["document"][:800]
        scores[eid]["vector_rank"] = rank

    return scores


# ---------------------------------------------------------------------------
# Metadata Boosting
# ---------------------------------------------------------------------------

def _extract_query_names(query):
    """
    Extract potential names or meaningful terms from the query for metadata boosting.
    Returns list of lowercase terms to match against metadata fields.
    """
    words = query.strip().split()
    terms = []
    for w in words:
        clean = re.sub(r'[^\w]', '', w)
        if not clean:
            continue
        if clean.lower() not in _COMMON_WORDS and len(clean) > 1:
            terms.append(clean.lower())
    return terms


def _apply_metadata_boost(scores, query):
    """
    Boost scores for results whose metadata fields contain query terms.
    If from_field, to_field, subject, or title contains a query term,
    boost score by 1.5x.
    """
    terms = _extract_query_names(query)
    if not terms:
        return

    for eid, data in scores.items():
        meta = data.get("metadata", {})
        boosted = False

        for field in ["from", "to", "subject", "title"]:
            field_val = str(meta.get(field, "")).lower()
            if not field_val:
                continue
            for term in terms:
                if term in field_val:
                    boosted = True
                    break
            if boosted:
                break

        if boosted:
            data["rrf_score"] *= 1.5


# ---------------------------------------------------------------------------
# Temporal Reranking
# ---------------------------------------------------------------------------

def _detect_temporal_intent(query):
    """
    Detect if the query has temporal ordering intent.
    Returns "asc", "desc", or None.
    """
    query_lower = query.lower()

    for phrase in _TEMPORAL_DESC:
        if phrase in query_lower:
            return "desc"

    for phrase in _TEMPORAL_ASC:
        if phrase in query_lower:
            return "asc"

    return None


def _parse_date_for_sort(date_str):
    """Try to parse a date string into a datetime for sorting."""
    if not date_str:
        return None

    # Try common formats
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y-%m-%dT%H:%M:%S%z",
        "%a, %d %b %Y %H:%M:%S %z",
    ):
        try:
            return datetime.strptime(date_str[:26], fmt)
        except (ValueError, TypeError):
            continue

    # Try extracting just year-month-day
    match = re.search(r'(\d{4})-(\d{1,2})-(\d{1,2})', str(date_str))
    if match:
        try:
            return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except ValueError:
            pass

    return None


def _apply_temporal_reranking(ranked_results, query):
    """
    Rerank results based on temporal intent in the query.
    Only applies temporal sort among results with score > median score,
    so irrelevant old entries don't rank first.
    """
    direction = _detect_temporal_intent(query)
    if not direction:
        return ranked_results

    if len(ranked_results) < 2:
        return ranked_results

    # Find median score
    all_scores = [r["combined_score"] for r in ranked_results]
    median_score = statistics.median(all_scores)

    # Split into above-median (eligible for temporal sort) and below-median
    above = []
    below = []
    for r in ranked_results:
        if r["combined_score"] >= median_score:
            above.append(r)
        else:
            below.append(r)

    # Sort above-median results by date
    def date_key(r):
        meta = r.get("metadata", {})
        dt = _parse_date_for_sort(meta.get("date", ""))
        if dt is None:
            # Use year/month if available
            year = meta.get("year", 0)
            month = meta.get("month", 0)
            if year:
                return datetime(year, month or 1, 1)
            # No date info — push to end
            return datetime(9999, 1, 1) if direction == "asc" else datetime(1, 1, 1)
        return dt

    reverse = direction == "desc"
    above.sort(key=date_key, reverse=reverse)

    return above + below


# ---------------------------------------------------------------------------
# Hybrid Search (main entry point)
# ---------------------------------------------------------------------------

def hybrid_search(
    query,
    vault_root,
    chroma_client,
    config=None,
    n_results=10,
    collections=None,
    year_filter=None,
    sort_by="relevance",
):
    """
    Hybrid search combining BM25 + vector search + metadata boosting + temporal reranking.

    Args:
        query: Natural language search query.
        vault_root: Base path containing vault directories.
        chroma_client: ChromaDB client instance.
        config: Optional config dict.
        n_results: Number of results to return.
        collections: List of collection names to search, or None for all.
        year_filter: Optional year integer to filter by.
        sort_by: "relevance" (default), "date_asc", or "date_desc".

    Returns list of dicts with:
        entry_id, collection, source, combined_score, metadata, snippet
    """
    config = config or {}
    db_path = _get_fts_db_path(vault_root, config)
    fetch_n = max(50, n_results * 5)  # Fetch more to allow reranking

    # Step 1: BM25 keyword search
    bm25_results = []
    if os.path.exists(db_path):
        try:
            bm25_results = bm25_search(
                db_path, query, n_results=fetch_n,
                collection_filter=collections, year_filter=year_filter,
            )
        except Exception as e:
            logger.warning("BM25 search failed, falling back to vector-only: %s", e)

    # Step 2: ChromaDB vector search
    where_filter = None
    if year_filter:
        where_filter = {"year": year_filter}

    vector_results = []
    try:
        vector_results = vector_search(
            chroma_client, query, collections=collections,
            n_results=fetch_n, where_filter=where_filter, config=config,
        )
    except Exception as e:
        logger.warning("Vector search failed, falling back to BM25-only: %s", e)

    # If both failed, return empty
    if not bm25_results and not vector_results:
        return []

    # Step 3: Reciprocal Rank Fusion
    merged = _compute_rrf_scores(bm25_results, vector_results)

    # Step 4: Metadata boosting
    _apply_metadata_boost(merged, query)

    # Step 5: Sort by RRF score
    ranked = []
    for eid, data in merged.items():
        ranked.append({
            "entry_id": eid,
            "collection": data.get("collection", ""),
            "source": data.get("source", ""),
            "combined_score": data["rrf_score"],
            "metadata": data.get("metadata", {}),
            "snippet": data.get("snippet", ""),
        })

    ranked.sort(key=lambda x: x["combined_score"], reverse=True)

    # Step 6: Temporal reranking (from query intent or explicit sort_by)
    if sort_by == "date_asc":
        ranked = _apply_temporal_reranking(ranked, "earliest " + query)
    elif sort_by == "date_desc":
        ranked = _apply_temporal_reranking(ranked, "latest " + query)
    elif sort_by == "relevance":
        ranked = _apply_temporal_reranking(ranked, query)

    return ranked[:n_results]


# ---------------------------------------------------------------------------
# Status / Index Management
# ---------------------------------------------------------------------------

def get_fts_entry_count(db_path):
    """Return the total number of entries in the FTS index."""
    if not os.path.exists(db_path):
        return 0
    try:
        conn = _get_connection(db_path)
        cursor = conn.execute("SELECT COUNT(*) FROM entry_meta")
        return cursor.fetchone()[0]
    except Exception:
        return 0


def get_fts_status(db_path):
    """
    Return status info for the FTS index.
    Returns dict with: total_entries, collections (dict of collection -> count).
    """
    if not os.path.exists(db_path):
        return {"total_entries": 0, "collections": {}}

    try:
        conn = _get_connection(db_path)
        cursor = conn.execute("SELECT COUNT(*) FROM entry_meta")
        total = cursor.fetchone()[0]

        cursor = conn.execute(
            "SELECT collection, COUNT(*) FROM entry_meta GROUP BY collection"
        )
        collections = {row[0]: row[1] for row in cursor.fetchall()}

        return {"total_entries": total, "collections": collections}
    except Exception:
        return {"total_entries": 0, "collections": {}}


def rebuild_index(vault_root, config=None):
    """
    Drop and recreate the FTS index, then re-index all vaults.
    Returns the result of index_all().
    """
    config = config or {}
    db_path = _get_fts_db_path(vault_root, config)

    if os.path.exists(db_path):
        # Close any existing connections on this thread
        key = f"conn_{db_path}"
        conn = getattr(_local, key, None)
        if conn:
            conn.close()
            setattr(_local, key, None)

        os.remove(db_path)
        logger.info("Removed existing FTS database: %s", db_path)

    print("  Rebuilding FTS index from scratch...")
    return index_all(vault_root, config)
