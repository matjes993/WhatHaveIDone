"""Schema for scroll manifests and quality metrics."""
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import time


class ScrollTier(Enum):
    """Safety tiers for scrolls."""
    SAFE = "safe"        # Pure computation, no I/O — runs without review
    POWER = "power"      # Network, file system, dependencies — needs review


@dataclass
class ScrollManifest:
    """Metadata describing a scroll (extraction recipe)."""
    id: str                          # unique ID like "gmail_imap_v2"
    name: str                        # human-readable name
    version: str                     # semver
    author: str                      # author name or handle
    description: str                 # what this scroll does
    tier: ScrollTier                 # safe or power
    target_source: str               # what it extracts from (e.g. "gmail", "chrome_bookmarks")
    output_vault_dir: str            # where it writes (e.g. "Gmail_Primary")
    requires: list[str] = field(default_factory=list)    # pip dependencies
    permissions: list[str] = field(default_factory=list)  # "network", "filesystem", "oauth"
    min_nomolo_version: str = "0.1.0"

    @classmethod
    def from_dict(cls, d: dict) -> "ScrollManifest":
        d = dict(d)
        d["tier"] = ScrollTier(d.get("tier", "power"))
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

    def to_dict(self) -> dict:
        d = {k: getattr(self, k) for k in self.__dataclass_fields__}
        d["tier"] = self.tier.value
        return d


@dataclass
class ScrollMetrics:
    """Quality metrics recorded after each scroll run."""
    scroll_id: str
    run_at: float = field(default_factory=time.time)

    # Speed: how fast the extraction ran
    duration_seconds: float = 0.0
    records_per_second: float = 0.0

    # Human annoyance: manual steps required (0 = fully automatic)
    manual_steps: int = 0           # OAuth screens, FDA toggles, file uploads
    human_wait_seconds: float = 0.0 # time human had to wait/act

    # Data richness: completeness of extraction
    records_extracted: int = 0
    fields_per_record: float = 0.0  # average fields populated
    unique_field_names: int = 0     # how many distinct fields
    has_timestamps: bool = False
    has_relationships: bool = False  # links between records

    # Storage efficiency
    bytes_per_record: float = 0.0
    total_bytes: int = 0
    compression_ratio: float = 1.0  # if using zstd etc.

    # Overall quality score (computed)
    @property
    def quality_score(self) -> float:
        """Composite score: higher is better. Max ~100."""
        speed = min(self.records_per_second / 100, 10)  # cap at 10
        annoyance = max(0, 10 - self.manual_steps * 3)  # fewer steps = better
        richness = min(self.fields_per_record * 2, 10)   # more fields = better
        extra = (5 if self.has_timestamps else 0) + (5 if self.has_relationships else 0)
        efficiency = min(10, 10000 / max(self.bytes_per_record, 1))  # smaller = better
        return speed + annoyance + richness + extra + efficiency

    def to_dict(self) -> dict:
        d = {k: getattr(self, k) for k in self.__dataclass_fields__}
        d["quality_score"] = self.quality_score
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "ScrollMetrics":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})
