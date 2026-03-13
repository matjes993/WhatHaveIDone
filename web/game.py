"""
NOMOLO Gamification Engine
Achievements, scoring, fun-facts quiz generation, and progress tracking.
Turns the data-hoarding grind into something people actually want to do.
"""

import json
import logging
import os
import random
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger("nomolo.game")

# ---------------------------------------------------------------------------
# Vault I/O helpers (local thin wrappers so game.py stays self-contained
# when core.vault is not on sys.path, e.g. inside the web worker)
# ---------------------------------------------------------------------------

try:
    from core.vault import _open_jsonl, _find_jsonl_files, count_entries, read_all_entries
except ImportError:
    # Minimal fallback so the module can be imported standalone for testing.
    try:
        import zstandard as zstd
        _HAS_ZSTD = True
    except ImportError:
        _HAS_ZSTD = False

    def _open_jsonl(file_path: str):
        if file_path.endswith(".zst"):
            if not _HAS_ZSTD:
                logger.warning("Cannot read %s — install zstandard", file_path)
                return None
            import io
            dctx = zstd.ZstdDecompressor()
            fh = open(file_path, "rb")
            reader = dctx.stream_reader(fh)
            return io.TextIOWrapper(reader, encoding="utf-8")
        return open(file_path, "r", encoding="utf-8")

    def _find_jsonl_files(vault_path: str):
        for root, _dirs, files in os.walk(vault_path):
            for f in sorted(files):
                if f.endswith(".jsonl") or f.endswith(".jsonl.zst"):
                    yield os.path.join(root, f)

    def count_entries(vault_path: str) -> tuple[int, int]:
        total = 0
        num_files = 0
        for fp in _find_jsonl_files(vault_path):
            num_files += 1
            try:
                fh = _open_jsonl(fp)
                if fh is None:
                    continue
                with fh:
                    for line in fh:
                        if line.strip():
                            total += 1
            except (OSError, PermissionError):
                pass
        return total, num_files

    def read_all_entries(vault_path: str):
        for fp in _find_jsonl_files(vault_path):
            try:
                fh = _open_jsonl(fp)
                if fh is None:
                    continue
                with fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            yield json.loads(line)
                        except json.JSONDecodeError:
                            pass
            except (OSError, PermissionError):
                pass


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_GAME_FILE = ".nomolo_game.json"

# All sources Nomolo can collect, used for completion tracking.
ALL_SOURCES = [
    "gmail", "contacts-google", "contacts-linkedin", "contacts-facebook",
    "contacts-instagram", "youtube", "music", "books", "podcasts",
    "browser", "health", "calendar", "maps", "notes", "shopping",
    "finance", "text-stream",
]

# Sources that count as "social platforms" for the Social Butterfly achievement.
SOCIAL_SOURCES = {
    "contacts-linkedin", "contacts-facebook", "contacts-instagram",
    "youtube", "text-stream",
}

# Sources that represent dead/legacy platforms (Ghost Hunter achievement).
DEAD_PLATFORMS = {"myspace", "google-plus", "vine", "friendster", "orkut"}

# Messaging sources (Conversation Keeper achievement).
MESSAGING_SOURCES = {"text-stream", "gmail"}

# Map vault directory names to friendly source names.
_DIR_TO_SOURCE: dict[str, str] = {
    "gmail": "gmail",
    "contacts_google": "contacts-google",
    "contacts_linkedin": "contacts-linkedin",
    "contacts_facebook": "contacts-facebook",
    "contacts_instagram": "contacts-instagram",
    "youtube": "youtube",
    "music": "music",
    "books": "books",
    "podcasts": "podcasts",
    "browser": "browser",
    "health": "health",
    "calendar": "calendar",
    "maps": "maps",
    "notes": "notes",
    "shopping": "shopping",
    "finance": "finance",
    "text_stream": "text-stream",
}


# ---------------------------------------------------------------------------
# Achievement definitions
# ---------------------------------------------------------------------------

@dataclass
class Achievement:
    id: str
    name: str
    description: str
    icon: str
    category: str
    trigger_condition: str
    unlocked: bool = False
    unlocked_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        d = asdict(self)
        if d["unlocked_at"] is not None:
            d["unlocked_at"] = d["unlocked_at"].isoformat()
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "Achievement":
        ua = data.get("unlocked_at")
        if ua and isinstance(ua, str):
            data["unlocked_at"] = datetime.fromisoformat(ua)
        return cls(**data)


def _default_achievements() -> list[Achievement]:
    return [
        Achievement(
            id="first_steps",
            name="First Steps",
            description="Complete your first import",
            icon="\U0001f463",  # footprints
            category="onboarding",
            trigger_condition="sources_collected >= 1",
        ),
        Achievement(
            id="email_archaeologist",
            name="Email Archaeologist",
            description="Collect 10,000+ emails",
            icon="\U0001f4e7",  # e-mail
            category="volume",
            trigger_condition="email_count >= 10000",
        ),
        Achievement(
            id="social_butterfly",
            name="Social Butterfly",
            description="Import data from 3+ social platforms",
            icon="\U0001f98b",  # butterfly
            category="breadth",
            trigger_condition="social_platforms >= 3",
        ),
        Achievement(
            id="time_traveler",
            name="Time Traveler",
            description="Your data spans 10+ years",
            icon="\u231b",  # hourglass
            category="depth",
            trigger_condition="time_span_years >= 10",
        ),
        Achievement(
            id="total_recall",
            name="Total Recall",
            description="Achieve a 90%+ completeness score",
            icon="\U0001f9e0",  # brain
            category="completeness",
            trigger_condition="completion_percentage >= 90",
        ),
        Achievement(
            id="digital_hoarder",
            name="Digital Hoarder",
            description="Accumulate 100,000+ total records",
            icon="\U0001f4e6",  # package
            category="volume",
            trigger_condition="total_records >= 100000",
        ),
        Achievement(
            id="ghost_hunter",
            name="Ghost Hunter",
            description="Recover data from a dead or legacy platform",
            icon="\U0001f47b",  # ghost
            category="exploration",
            trigger_condition="has_dead_platform",
        ),
        Achievement(
            id="life_cartographer",
            name="Life Cartographer",
            description="Build a knowledge graph with 50+ people",
            icon="\U0001f5fa",  # world map
            category="graph",
            trigger_condition="unique_people >= 50",
        ),
        Achievement(
            id="speed_runner",
            name="Speed Runner",
            description="Complete 3 imports in your first session",
            icon="\u26a1",  # lightning
            category="onboarding",
            trigger_condition="first_session_imports >= 3",
        ),
        Achievement(
            id="night_owl",
            name="Night Owl",
            description="Import data between 2 AM and 5 AM",
            icon="\U0001f989",  # owl
            category="fun",
            trigger_condition="imported_at_night",
        ),
        Achievement(
            id="archivist",
            name="Archivist",
            description="All vaults groomed and deduplicated",
            icon="\U0001f4da",  # books
            category="maintenance",
            trigger_condition="all_vaults_groomed",
        ),
        Achievement(
            id="search_master",
            name="Search Master",
            description="Perform 50+ searches across your vaults",
            icon="\U0001f50d",  # magnifier
            category="usage",
            trigger_condition="search_count >= 50",
        ),
        Achievement(
            id="data_detective",
            name="Data Detective",
            description="Find data older than 2010",
            icon="\U0001f575",  # detective
            category="depth",
            trigger_condition="has_pre_2010_data",
        ),
        Achievement(
            id="globe_trotter",
            name="Globe Trotter",
            description="Data from 5+ countries detected",
            icon="\U0001f30d",  # globe
            category="breadth",
            trigger_condition="countries >= 5",
        ),
        Achievement(
            id="conversation_keeper",
            name="Conversation Keeper",
            description="Import messaging or chat data",
            icon="\U0001f4ac",  # speech bubble
            category="breadth",
            trigger_condition="has_messaging_data",
        ),
    ]


# ---------------------------------------------------------------------------
# Persistent game state
# ---------------------------------------------------------------------------

def _game_path(vault_root: str) -> str:
    return os.path.join(vault_root, _GAME_FILE)


def _load_game_state(vault_root: str) -> dict:
    path = _game_path(vault_root)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Corrupt game state at %s, resetting: %s", path, e)
    return {
        "achievements": {},
        "search_count": 0,
        "first_session_imports": 0,
        "first_session_ts": None,
        "activity_days": [],
        "fun_facts_cache": {},
        "quests_completed": [],
    }


def _save_game_state(vault_root: str, state: dict) -> None:
    path = _game_path(vault_root)
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, default=str)
        os.replace(tmp, path)
    except OSError as e:
        logger.error("Failed to save game state: %s", e)


# ---------------------------------------------------------------------------
# Vault scanning helpers
# ---------------------------------------------------------------------------

def _scan_vaults(vault_root: str) -> dict[str, dict]:
    """
    Scan all vault directories. Returns a dict keyed by directory name with:
      entries, files, source, earliest, latest
    """
    results: dict[str, dict] = {}
    if not os.path.isdir(vault_root):
        return results

    for entry in sorted(os.listdir(vault_root)):
        vault_path = os.path.join(vault_root, entry)
        if not os.path.isdir(vault_path) or entry.startswith("."):
            continue
        total, num_files = count_entries(vault_path)
        if total == 0 and num_files == 0:
            continue
        results[entry] = {
            "entries": total,
            "files": num_files,
            "source": _DIR_TO_SOURCE.get(entry, entry),
            "path": vault_path,
        }
    return results


def _sample_entries(vault_path: str, max_samples: int = 500) -> list[dict]:
    """
    Random-sample entries from a vault for fast analysis.
    Reads a reservoir sample without loading everything into memory.
    """
    reservoir: list[dict] = []
    count = 0
    for fp in _find_jsonl_files(vault_path):
        try:
            fh = _open_jsonl(fp)
            if fh is None:
                continue
            with fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    count += 1
                    if len(reservoir) < max_samples:
                        reservoir.append(entry)
                    else:
                        idx = random.randint(0, count - 1)
                        if idx < max_samples:
                            reservoir[idx] = entry
        except (OSError, PermissionError):
            continue
    return reservoir


def _extract_date(entry: dict) -> Optional[datetime]:
    """Try to pull a datetime from common entry fields."""
    for key in ("date", "timestamp", "created_at", "sent_at", "time", "datetime"):
        val = entry.get(key)
        if not val:
            continue
        if isinstance(val, (int, float)):
            try:
                return datetime.fromtimestamp(val, tz=timezone.utc)
            except (ValueError, OSError):
                continue
        if isinstance(val, str):
            # Try ISO first
            try:
                return datetime.fromisoformat(val)
            except ValueError:
                pass
            # RFC-2822 style
            for fmt in (
                "%a, %d %b %Y %H:%M:%S %z",
                "%d %b %Y %H:%M:%S %z",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d",
            ):
                try:
                    return datetime.strptime(val, fmt)
                except ValueError:
                    continue
    return None


def _extract_sender(entry: dict) -> Optional[str]:
    """Pull a person/sender name from an entry."""
    for key in ("from", "sender", "author", "name", "contact_name", "artist"):
        val = entry.get(key)
        if val and isinstance(val, str):
            # Strip email addresses: "Alice <alice@x.com>" -> "Alice"
            if "<" in val:
                val = val.split("<")[0].strip()
            if val:
                return val
    return None


def _extract_country(entry: dict) -> Optional[str]:
    """Try to detect a country from an entry."""
    for key in ("country", "location_country", "country_code"):
        val = entry.get(key)
        if val and isinstance(val, str) and len(val) <= 60:
            return val
    # Check nested location dicts
    loc = entry.get("location")
    if isinstance(loc, dict):
        for key in ("country", "country_code"):
            val = loc.get(key)
            if val and isinstance(val, str):
                return val
    return None


# ---------------------------------------------------------------------------
# Achievement evaluation
# ---------------------------------------------------------------------------

def evaluate_achievements(
    vault_root: str,
    scan_results: Optional[dict] = None,
) -> list[Achievement]:
    """
    Evaluate all achievements against current vault state.
    Returns the full list with unlocked status updated.
    Persists newly unlocked achievements to game state.
    """
    if scan_results is None:
        scan_results = _scan_vaults(vault_root)

    state = _load_game_state(vault_root)
    achievements = _default_achievements()

    # Restore previously unlocked achievements
    for ach in achievements:
        saved = state.get("achievements", {}).get(ach.id)
        if saved and saved.get("unlocked"):
            ach.unlocked = True
            ua = saved.get("unlocked_at")
            if ua:
                try:
                    ach.unlocked_at = datetime.fromisoformat(ua)
                except (ValueError, TypeError):
                    ach.unlocked_at = None

    # Compute metrics
    total_records = sum(v["entries"] for v in scan_results.values())
    sources_collected = len(scan_results)
    collected_sources = {v["source"] for v in scan_results.values()}
    social_count = len(collected_sources & SOCIAL_SOURCES)
    has_dead = bool(collected_sources & DEAD_PLATFORMS)
    has_messaging = bool(collected_sources & MESSAGING_SOURCES)

    # Sample across all vaults for date/people/country analysis
    all_dates: list[datetime] = []
    people: set[str] = set()
    countries: set[str] = set()

    for vault_dir, info in scan_results.items():
        samples = _sample_entries(info["path"], max_samples=300)
        for entry in samples:
            dt = _extract_date(entry)
            if dt:
                all_dates.append(dt)
            sender = _extract_sender(entry)
            if sender:
                people.add(sender.lower())
            country = _extract_country(entry)
            if country:
                countries.add(country.lower())

    time_span_years = 0.0
    has_pre_2010 = False
    if all_dates:
        earliest = min(all_dates)
        latest = max(all_dates)
        time_span_years = (latest - earliest).days / 365.25
        has_pre_2010 = earliest.year < 2010

    email_count = scan_results.get("gmail", {}).get("entries", 0)

    # Check groomed status
    all_groomed = True
    for vault_dir, info in scan_results.items():
        groomed_marker = os.path.join(info["path"], ".groomed")
        if not os.path.exists(groomed_marker):
            all_groomed = False
            break

    now = datetime.now(tz=timezone.utc)
    is_night = 2 <= now.hour < 5

    search_count = state.get("search_count", 0)
    first_session_imports = state.get("first_session_imports", 0)

    completion_pct = (sources_collected / len(ALL_SOURCES) * 100) if ALL_SOURCES else 0

    # Metric map for evaluation
    metrics: dict[str, Any] = {
        "sources_collected": sources_collected,
        "email_count": email_count,
        "social_platforms": social_count,
        "time_span_years": time_span_years,
        "completion_percentage": completion_pct,
        "total_records": total_records,
        "has_dead_platform": has_dead,
        "unique_people": len(people),
        "first_session_imports": first_session_imports,
        "imported_at_night": is_night,
        "all_vaults_groomed": all_groomed and sources_collected > 0,
        "search_count": search_count,
        "has_pre_2010_data": has_pre_2010,
        "countries": len(countries),
        "has_messaging_data": has_messaging,
    }

    newly_unlocked: list[Achievement] = []

    for ach in achievements:
        if ach.unlocked:
            continue
        if _check_condition(ach.trigger_condition, metrics):
            ach.unlocked = True
            ach.unlocked_at = now
            newly_unlocked.append(ach)

    # Persist
    if newly_unlocked:
        for ach in newly_unlocked:
            state.setdefault("achievements", {})[ach.id] = {
                "unlocked": True,
                "unlocked_at": ach.unlocked_at.isoformat() if ach.unlocked_at else None,
            }
        _save_game_state(vault_root, state)

    return achievements


def _check_condition(condition: str, metrics: dict[str, Any]) -> bool:
    """
    Evaluate a trigger condition string like 'email_count >= 10000' or 'has_dead_platform'.
    """
    condition = condition.strip()

    # Boolean conditions (no operator)
    if condition in metrics:
        return bool(metrics[condition])

    # Comparison conditions
    for op in (">=", "<=", "==", ">", "<"):
        if op in condition:
            parts = condition.split(op, 1)
            if len(parts) == 2:
                key = parts[0].strip()
                try:
                    threshold = float(parts[1].strip())
                except ValueError:
                    return False
                val = metrics.get(key, 0)
                if isinstance(val, bool):
                    val = int(val)
                if op == ">=":
                    return val >= threshold
                if op == "<=":
                    return val <= threshold
                if op == "==":
                    return val == threshold
                if op == ">":
                    return val > threshold
                if op == "<":
                    return val < threshold
            break

    return False


# ---------------------------------------------------------------------------
# Fun Facts Engine
# ---------------------------------------------------------------------------

@dataclass
class FunFact:
    question: str
    options: list[str]
    correct_index: int
    fun_response: str
    source: str
    category: str

    def to_dict(self) -> dict:
        return asdict(self)


def generate_fun_facts(vault_root: str, sources: Optional[list[str]] = None) -> list[dict]:
    """
    Generate multiple-choice quiz questions from collected data.
    Returns a list of fact dicts ready for the frontend.
    Uses random sampling so it stays fast even on massive vaults.
    """
    state = _load_game_state(vault_root)

    # Check cache freshness (regenerate every 6 hours)
    cache = state.get("fun_facts_cache", {})
    cache_ts = cache.get("generated_at")
    if cache_ts:
        try:
            age = time.time() - float(cache_ts)
            if age < 6 * 3600 and cache.get("facts"):
                return cache["facts"]
        except (ValueError, TypeError):
            pass

    scan = _scan_vaults(vault_root)
    if sources:
        scan = {k: v for k, v in scan.items() if v["source"] in sources}

    facts: list[dict] = []

    for vault_dir, info in scan.items():
        samples = _sample_entries(info["path"], max_samples=1000)
        if not samples:
            continue
        vault_facts = _generate_vault_facts(samples, info["source"], info["entries"])
        facts.extend(vault_facts)

    # Shuffle and cap
    random.shuffle(facts)

    # Cache results
    state["fun_facts_cache"] = {
        "generated_at": time.time(),
        "facts": facts,
    }
    _save_game_state(vault_root, state)

    return facts


def _generate_vault_facts(
    samples: list[dict],
    source_name: str,
    total_entries: int,
) -> list[dict]:
    """Generate 3-5 fun fact questions from sampled entries for one vault."""
    facts: list[dict] = []

    dates: list[datetime] = []
    senders: list[str] = []
    years: list[int] = []
    weekdays: list[int] = []
    months: list[int] = []
    hours: list[int] = []

    for entry in samples:
        dt = _extract_date(entry)
        if dt:
            dates.append(dt)
            years.append(dt.year)
            weekdays.append(dt.weekday())
            months.append(dt.month)
            hours.append(dt.hour)
        sender = _extract_sender(entry)
        if sender:
            senders.append(sender)

    # --- Fact: busiest year ---
    if years:
        year_counts = Counter(years)
        if len(year_counts) >= 2:
            busiest_year = year_counts.most_common(1)[0][0]
            options = _make_year_options(busiest_year, list(year_counts.keys()))
            correct_idx = options.index(str(busiest_year))
            facts.append(FunFact(
                question=f"Which year did you have the most {source_name} activity?",
                options=options,
                correct_index=correct_idx,
                fun_response=f"You were on fire in {busiest_year}! "
                             f"We found ~{year_counts[busiest_year]} records from that year.",
                source=source_name,
                category="frequency",
            ).to_dict())

    # --- Fact: busiest day of week ---
    _DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    if weekdays:
        day_counts = Counter(weekdays)
        busiest_day = day_counts.most_common(1)[0][0]
        day_options = random.sample(_DAY_NAMES, min(4, len(_DAY_NAMES)))
        if _DAY_NAMES[busiest_day] not in day_options:
            day_options[random.randint(0, len(day_options) - 1)] = _DAY_NAMES[busiest_day]
        random.shuffle(day_options)
        facts.append(FunFact(
            question=f"What day of the week are you most active in {source_name}?",
            options=day_options,
            correct_index=day_options.index(_DAY_NAMES[busiest_day]),
            fun_response=f"Looks like {_DAY_NAMES[busiest_day]} is your power day!",
            source=source_name,
            category="timing",
        ).to_dict())

    # --- Fact: top sender / contact ---
    if senders and len(set(senders)) >= 2:
        sender_counts = Counter(senders)
        top_sender = sender_counts.most_common(1)[0][0]
        other_senders = [s for s, _ in sender_counts.most_common(10) if s != top_sender]
        options = [top_sender]
        options.extend(random.sample(other_senders, min(3, len(other_senders))))
        while len(options) < 4:
            options.append(f"Someone #{len(options) + 1}")
        random.shuffle(options)
        facts.append(FunFact(
            question=f"Who appears most frequently in your {source_name} data?",
            options=options,
            correct_index=options.index(top_sender),
            fun_response=f"{top_sender} is your #1! They appeared "
                         f"{sender_counts[top_sender]} times in our sample.",
            source=source_name,
            category="people",
        ).to_dict())

    # --- Fact: total unique contacts ---
    if senders:
        unique_count = len(set(senders))
        # Extrapolate from sample to total
        extrapolated = int(unique_count * (total_entries / max(len(samples), 1)))
        correct = str(extrapolated)
        wrong = _make_numeric_options(extrapolated)
        options = [correct] + wrong[:3]
        random.shuffle(options)
        facts.append(FunFact(
            question=f"Approximately how many unique people are in your {source_name} data?",
            options=options,
            correct_index=options.index(correct),
            fun_response=f"Your {source_name} connects you to roughly {extrapolated:,} people!",
            source=source_name,
            category="people",
        ).to_dict())

    # --- Fact: earliest record ---
    if dates:
        earliest = min(dates)
        formatted = earliest.strftime("%B %Y")
        all_years = sorted(set(d.year for d in dates))
        year_options = [formatted]
        for y in all_years:
            candidate = f"January {y}"
            if candidate != formatted and len(year_options) < 4:
                year_options.append(candidate)
        while len(year_options) < 4:
            fake_year = earliest.year + random.randint(-3, 5)
            candidate = f"June {fake_year}"
            if candidate not in year_options:
                year_options.append(candidate)
        random.shuffle(year_options)
        facts.append(FunFact(
            question=f"When is the earliest record in your {source_name} archive?",
            options=year_options,
            correct_index=year_options.index(formatted),
            fun_response=f"Your {source_name} history goes all the way back to {formatted}!",
            source=source_name,
            category="milestone",
        ).to_dict())

    # --- Fact: busiest month ---
    _MONTH_NAMES = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
    ]
    if months and len(set(months)) >= 2:
        month_counts = Counter(months)
        busiest_month = month_counts.most_common(1)[0][0]
        month_options = random.sample(_MONTH_NAMES, min(4, len(_MONTH_NAMES)))
        if _MONTH_NAMES[busiest_month - 1] not in month_options:
            month_options[random.randint(0, len(month_options) - 1)] = _MONTH_NAMES[busiest_month - 1]
        random.shuffle(month_options)
        facts.append(FunFact(
            question=f"Which month were you most active in {source_name}?",
            options=month_options,
            correct_index=month_options.index(_MONTH_NAMES[busiest_month - 1]),
            fun_response=f"{_MONTH_NAMES[busiest_month - 1]} was your peak month!",
            source=source_name,
            category="timing",
        ).to_dict())

    # --- Fact: busiest hour ---
    if hours and len(set(hours)) >= 3:
        hour_counts = Counter(hours)
        busiest_hour = hour_counts.most_common(1)[0][0]
        hour_label = f"{busiest_hour}:00"
        wrong_hours = [f"{h}:00" for h in random.sample(range(24), min(6, 24)) if h != busiest_hour]
        options = [hour_label] + wrong_hours[:3]
        random.shuffle(options)
        facts.append(FunFact(
            question=f"What hour of the day are you most active in {source_name}?",
            options=options,
            correct_index=options.index(hour_label),
            fun_response=f"You're a {hour_label} warrior! That's your peak hour.",
            source=source_name,
            category="timing",
        ).to_dict())

    # Return 3-5 facts max per vault
    random.shuffle(facts)
    return facts[:5]


def _make_year_options(correct_year: int, available_years: list[int]) -> list[str]:
    """Build 4 year options including the correct one."""
    options = {str(correct_year)}
    candidates = [str(y) for y in available_years if y != correct_year]
    random.shuffle(candidates)
    for c in candidates:
        if len(options) >= 4:
            break
        options.add(c)
    while len(options) < 4:
        fake = correct_year + random.choice([-3, -2, -1, 1, 2, 3])
        options.add(str(fake))
    result = list(options)
    random.shuffle(result)
    return result


def _make_numeric_options(correct: int) -> list[str]:
    """Generate 3 plausible wrong numeric answers."""
    wrong = set()
    for mult in [0.3, 0.6, 1.5, 2.2, 3.0, 0.1]:
        candidate = int(correct * mult)
        if candidate != correct and candidate > 0:
            wrong.add(str(candidate))
    return list(wrong)[:3]


# ---------------------------------------------------------------------------
# Progress Tracking
# ---------------------------------------------------------------------------

def get_progress(vault_root: str, scan_results: Optional[dict] = None) -> dict:
    """
    Compute overall progress metrics.
    scan_results: optional pre-computed dict from _scan_vaults.
    """
    if scan_results is None:
        scan_results = _scan_vaults(vault_root)

    state = _load_game_state(vault_root)

    total_records = sum(v["entries"] for v in scan_results.values())
    sources_collected = len(scan_results)
    sources_available = len(ALL_SOURCES)
    completion_pct = round(sources_collected / sources_available * 100, 1) if sources_available else 0

    # Compute time span by sampling dates
    all_dates: list[datetime] = []
    for vault_dir, info in scan_results.items():
        samples = _sample_entries(info["path"], max_samples=200)
        for entry in samples:
            dt = _extract_date(entry)
            if dt:
                all_dates.append(dt)

    earliest: Optional[datetime] = None
    latest: Optional[datetime] = None
    time_span_str = "N/A"
    if all_dates:
        earliest = min(all_dates)
        latest = max(all_dates)
        span_days = (latest - earliest).days
        span_years = span_days / 365.25
        if span_years >= 1:
            time_span_str = f"{span_years:.1f} years"
        elif span_days >= 30:
            time_span_str = f"{span_days // 30} months"
        else:
            time_span_str = f"{span_days} days"

    # Timeline gaps: find months with no data
    timeline_gaps: list[dict] = []
    if all_dates and len(all_dates) > 10:
        month_set: set[tuple[int, int]] = set()
        for dt in all_dates:
            month_set.add((dt.year, dt.month))
        if earliest and latest:
            current_year = earliest.year
            current_month = earliest.month
            gap_start: Optional[tuple[int, int]] = None
            while (current_year, current_month) <= (latest.year, latest.month):
                if (current_year, current_month) not in month_set:
                    if gap_start is None:
                        gap_start = (current_year, current_month)
                else:
                    if gap_start is not None:
                        timeline_gaps.append({
                            "from": f"{gap_start[0]}-{gap_start[1]:02d}",
                            "to": f"{current_year}-{current_month:02d}",
                        })
                        gap_start = None
                # Advance month
                current_month += 1
                if current_month > 12:
                    current_month = 1
                    current_year += 1
            if gap_start is not None:
                timeline_gaps.append({
                    "from": f"{gap_start[0]}-{gap_start[1]:02d}",
                    "to": f"{latest.year}-{latest.month:02d}",
                })

    # Next quest suggestion
    next_quest = _suggest_next_action(scan_results, sources_collected, total_records)

    # Streak: consecutive days with activity
    activity_days = state.get("activity_days", [])
    streak = _compute_streak(activity_days)

    return {
        "total_records": total_records,
        "sources_collected": sources_collected,
        "sources_available": sources_available,
        "completion_percentage": completion_pct,
        "time_span": time_span_str,
        "earliest_date": earliest.isoformat() if earliest else None,
        "latest_date": latest.isoformat() if latest else None,
        "timeline_gaps": timeline_gaps[:10],  # Cap for readability
        "next_quest": next_quest,
        "streak": streak,
    }


def _suggest_next_action(scan_results: dict, sources_collected: int, total_records: int) -> str:
    """Pick the most valuable next step for the user."""
    collected_sources = {v["source"] for v in scan_results.values()}

    if sources_collected == 0:
        return "Import your first data source! Try: nomolo setup gmail"

    # Prioritize high-value missing sources
    priority_sources = [
        ("gmail", "Set up Gmail to import your email history"),
        ("contacts-google", "Import your Google Contacts for a richer people graph"),
        ("calendar", "Add your calendar to map your time"),
        ("browser", "Import browser history to see your web footprint"),
        ("youtube", "Connect YouTube to track your watch history"),
        ("books", "Import your reading history"),
        ("music", "Add your music listening data"),
        ("maps", "Import location history"),
        ("health", "Add health data for a complete life picture"),
    ]

    for source, suggestion in priority_sources:
        if source not in collected_sources:
            return suggestion

    # All main sources collected
    has_groomed = all(
        os.path.exists(os.path.join(v["path"], ".groomed"))
        for v in scan_results.values()
    )
    if not has_groomed:
        return "Run 'nomolo groom' to deduplicate and optimize your vaults"

    return "You're doing great! Try searching your data: nomolo search 'your query'"


def _compute_streak(activity_days: list[str]) -> int:
    """Compute consecutive-day streak ending today or yesterday."""
    if not activity_days:
        return 0

    try:
        dates = sorted(set(datetime.fromisoformat(d).date() for d in activity_days), reverse=True)
    except (ValueError, TypeError):
        return 0

    today = datetime.now(tz=timezone.utc).date()
    if not dates or (today - dates[0]).days > 1:
        return 0

    streak = 1
    for i in range(1, len(dates)):
        if (dates[i - 1] - dates[i]).days == 1:
            streak += 1
        else:
            break
    return streak


def record_activity(vault_root: str) -> None:
    """Record that the user was active today (for streak tracking)."""
    state = _load_game_state(vault_root)
    today = datetime.now(tz=timezone.utc).date().isoformat()
    days = state.get("activity_days", [])
    if today not in days:
        days.append(today)
        # Keep last 365 days
        if len(days) > 365:
            days = days[-365:]
        state["activity_days"] = days
        _save_game_state(vault_root, state)


def record_search(vault_root: str) -> None:
    """Increment the search counter (for Search Master achievement)."""
    state = _load_game_state(vault_root)
    state["search_count"] = state.get("search_count", 0) + 1
    _save_game_state(vault_root, state)


def record_import(vault_root: str) -> None:
    """Record an import event (for Speed Runner achievement)."""
    state = _load_game_state(vault_root)
    now = time.time()

    if state.get("first_session_ts") is None:
        state["first_session_ts"] = now

    # "First session" = within 2 hours of first ever import
    first_ts = state["first_session_ts"]
    if isinstance(first_ts, (int, float)) and (now - first_ts) < 7200:
        state["first_session_imports"] = state.get("first_session_imports", 0) + 1

    _save_game_state(vault_root, state)


# ---------------------------------------------------------------------------
# Quest System
# ---------------------------------------------------------------------------

@dataclass
class Quest:
    id: str
    title: str
    description: str
    difficulty: int  # 1-5 stars
    xp_reward: int
    estimated_time: str
    action: str
    completed: bool = False

    def to_dict(self) -> dict:
        return asdict(self)


def get_quests(
    scan_results: Optional[dict] = None,
    progress: Optional[dict] = None,
    vault_root: Optional[str] = None,
) -> list[dict]:
    """
    Generate a prioritized list of quests based on current state.
    Ordered: easiest first, highest value, builds on existing data.
    """
    if vault_root and scan_results is None:
        scan_results = _scan_vaults(vault_root)
    if scan_results is None:
        scan_results = {}

    state = _load_game_state(vault_root) if vault_root else {}
    completed_ids = set(state.get("quests_completed", []))
    collected_sources = {v["source"] for v in scan_results.values()}
    total_records = sum(v["entries"] for v in scan_results.values())

    quests: list[Quest] = []

    # --- Onboarding quests ---
    if "gmail" not in collected_sources:
        quests.append(Quest(
            id="setup_gmail",
            title="Connect Your Gmail",
            description="Import your email archive — usually the richest data source.",
            difficulty=2,
            xp_reward=500,
            estimated_time="5 min setup, 1-4 hours import",
            action="nomolo setup gmail",
            completed="setup_gmail" in completed_ids,
        ))

    if "contacts-google" not in collected_sources:
        quests.append(Quest(
            id="setup_contacts_google",
            title="Import Google Contacts",
            description="Pull in your contact list to build your people graph.",
            difficulty=1,
            xp_reward=300,
            estimated_time="2 min",
            action="nomolo setup contacts-google",
            completed="setup_contacts_google" in completed_ids,
        ))

    if "contacts-linkedin" not in collected_sources:
        quests.append(Quest(
            id="import_linkedin",
            title="Import LinkedIn Connections",
            description="Download your LinkedIn data export and import your professional network.",
            difficulty=2,
            xp_reward=400,
            estimated_time="10 min (includes LinkedIn export wait)",
            action="nomolo collect contacts-linkedin <file>",
            completed="import_linkedin" in completed_ids,
        ))

    if "browser" not in collected_sources:
        quests.append(Quest(
            id="import_browser",
            title="Import Browser History",
            description="See your web footprint across time.",
            difficulty=1,
            xp_reward=300,
            estimated_time="2 min",
            action="nomolo collect browser",
            completed="import_browser" in completed_ids,
        ))

    if "youtube" not in collected_sources:
        quests.append(Quest(
            id="import_youtube",
            title="Import YouTube History",
            description="Find out what you really watched all those hours.",
            difficulty=2,
            xp_reward=350,
            estimated_time="5 min",
            action="nomolo collect youtube",
            completed="import_youtube" in completed_ids,
        ))

    if "calendar" not in collected_sources:
        quests.append(Quest(
            id="import_calendar",
            title="Import Your Calendar",
            description="Map how you spent your time.",
            difficulty=2,
            xp_reward=350,
            estimated_time="3 min",
            action="nomolo collect calendar",
            completed="import_calendar" in completed_ids,
        ))

    if "books" not in collected_sources:
        quests.append(Quest(
            id="import_books",
            title="Import Reading History",
            description="Track your intellectual journey across the years.",
            difficulty=1,
            xp_reward=250,
            estimated_time="3 min",
            action="nomolo collect books",
            completed="import_books" in completed_ids,
        ))

    if "music" not in collected_sources:
        quests.append(Quest(
            id="import_music",
            title="Import Music Listening Data",
            description="Discover your music taste evolution.",
            difficulty=2,
            xp_reward=300,
            estimated_time="5 min",
            action="nomolo collect music",
            completed="import_music" in completed_ids,
        ))

    if "maps" not in collected_sources:
        quests.append(Quest(
            id="import_maps",
            title="Import Location History",
            description="See everywhere you've been.",
            difficulty=2,
            xp_reward=400,
            estimated_time="5 min",
            action="nomolo collect maps",
            completed="import_maps" in completed_ids,
        ))

    if "health" not in collected_sources:
        quests.append(Quest(
            id="import_health",
            title="Import Health Data",
            description="Add your health and fitness records.",
            difficulty=3,
            xp_reward=500,
            estimated_time="10 min",
            action="nomolo collect health",
            completed="import_health" in completed_ids,
        ))

    if "notes" not in collected_sources:
        quests.append(Quest(
            id="import_notes",
            title="Import Your Notes",
            description="Bring in your thoughts and ideas.",
            difficulty=1,
            xp_reward=250,
            estimated_time="3 min",
            action="nomolo collect notes",
            completed="import_notes" in completed_ids,
        ))

    if "finance" not in collected_sources:
        quests.append(Quest(
            id="import_finance",
            title="Import Financial Data",
            description="Track your spending patterns over time.",
            difficulty=3,
            xp_reward=400,
            estimated_time="10 min",
            action="nomolo collect finance",
            completed="import_finance" in completed_ids,
        ))

    if "shopping" not in collected_sources:
        quests.append(Quest(
            id="import_shopping",
            title="Import Shopping History",
            description="See what you bought and when.",
            difficulty=2,
            xp_reward=300,
            estimated_time="5 min",
            action="nomolo collect shopping",
            completed="import_shopping" in completed_ids,
        ))

    if "podcasts" not in collected_sources:
        quests.append(Quest(
            id="import_podcasts",
            title="Import Podcast History",
            description="Track the shows that shaped your thinking.",
            difficulty=1,
            xp_reward=250,
            estimated_time="3 min",
            action="nomolo collect podcasts",
            completed="import_podcasts" in completed_ids,
        ))

    # --- Maintenance quests (only if user has data) ---
    if total_records > 0:
        has_ungroomed = any(
            not os.path.exists(os.path.join(v["path"], ".groomed"))
            for v in scan_results.values()
        )
        if has_ungroomed:
            quests.append(Quest(
                id="groom_vaults",
                title="Groom Your Vaults",
                description="Deduplicate and sort your data for faster searches.",
                difficulty=1,
                xp_reward=200,
                estimated_time="2-10 min",
                action="nomolo groom",
                completed="groom_vaults" in completed_ids,
            ))

        quests.append(Quest(
            id="first_search",
            title="Search Your Memory",
            description="Try a semantic search across all your collected data.",
            difficulty=1,
            xp_reward=100,
            estimated_time="1 min",
            action="nomolo search 'your first memory query'",
            completed="first_search" in completed_ids,
        ))

        quests.append(Quest(
            id="vectorize",
            title="Vectorize for AI Search",
            description="Enable semantic search by vectorizing your vaults.",
            difficulty=2,
            xp_reward=400,
            estimated_time="5-30 min",
            action="nomolo vectorize",
            completed="vectorize" in completed_ids,
        ))

    # --- Social quests ---
    social_collected = collected_sources & SOCIAL_SOURCES
    if len(social_collected) < 3 and len(social_collected) >= 1:
        missing_social = SOCIAL_SOURCES - collected_sources
        if missing_social:
            example = next(iter(missing_social))
            quests.append(Quest(
                id="social_butterfly_quest",
                title="Expand Your Social Footprint",
                description=f"Import {3 - len(social_collected)} more social platform(s) "
                            f"to unlock the Social Butterfly achievement.",
                difficulty=2,
                xp_reward=350,
                estimated_time="10 min",
                action=f"nomolo collect {example}",
                completed="social_butterfly_quest" in completed_ids,
            ))

    # Sort: completed last, then by difficulty ascending, then XP descending
    quests.sort(key=lambda q: (q.completed, q.difficulty, -q.xp_reward))

    return [q.to_dict() for q in quests]


def complete_quest(vault_root: str, quest_id: str) -> None:
    """Mark a quest as completed."""
    state = _load_game_state(vault_root)
    completed = state.get("quests_completed", [])
    if quest_id not in completed:
        completed.append(quest_id)
        state["quests_completed"] = completed
        _save_game_state(vault_root, state)


# ---------------------------------------------------------------------------
# XP & Level (derived from achievements + quests)
# ---------------------------------------------------------------------------

def get_xp_summary(vault_root: str) -> dict:
    """Calculate total XP and level from achievements and completed quests."""
    state = _load_game_state(vault_root)
    achievements = evaluate_achievements(vault_root)

    xp = 0
    # Each achievement is worth 100 XP
    for ach in achievements:
        if ach.unlocked:
            xp += 100

    # Quest XP
    completed_quests = set(state.get("quests_completed", []))
    all_quests = get_quests(vault_root=vault_root)
    for q in all_quests:
        if q["id"] in completed_quests:
            xp += q["xp_reward"]

    # Level formula: each level requires 500 XP more than the last
    level = 0
    xp_for_next = 500
    remaining = xp
    while remaining >= xp_for_next:
        remaining -= xp_for_next
        level += 1
        xp_for_next = 500 + level * 200

    return {
        "total_xp": xp,
        "level": level,
        "xp_to_next_level": xp_for_next - remaining,
        "xp_in_current_level": remaining,
        "achievements_unlocked": sum(1 for a in achievements if a.unlocked),
        "achievements_total": len(achievements),
        "quests_completed": len(completed_quests),
    }


# ---------------------------------------------------------------------------
# Convenience: full dashboard payload
# ---------------------------------------------------------------------------

def get_game_dashboard(vault_root: str) -> dict:
    """
    Return the complete gamification dashboard in one call.
    Used by the web frontend to render the game tab.
    """
    scan = _scan_vaults(vault_root)
    achievements = evaluate_achievements(vault_root, scan_results=scan)
    progress = get_progress(vault_root, scan_results=scan)
    quests = get_quests(scan_results=scan, progress=progress, vault_root=vault_root)
    xp = get_xp_summary(vault_root)

    # Record today's activity
    record_activity(vault_root)

    return {
        "achievements": [a.to_dict() for a in achievements],
        "progress": progress,
        "quests": quests,
        "xp": xp,
    }
