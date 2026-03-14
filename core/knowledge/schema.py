"""
Nomolo Knowledge Graph — Schema Definitions

Canonical types, entity types, relationship types, and data structures
that form the foundation of the knowledge graph.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class EntityType(str, Enum):
    PERSON = "person"
    ORGANIZATION = "organization"
    PLACE = "place"
    EVENT = "event"
    MESSAGE = "message"
    FILE = "file"
    BOOKMARK = "bookmark"
    NOTE = "note"
    ACCOUNT = "account"


class RelationshipType(str, Enum):
    KNOWS = "knows"
    WORKS_AT = "works_at"
    SENT = "sent"
    RECEIVED = "received"
    ATTENDED = "attended"
    LOCATED_AT = "located_at"
    MENTIONS = "mentions"
    TAGGED = "tagged"
    MEMBER_OF = "member_of"
    OWNS = "owns"
    CREATED = "created"
    REPLIED_TO = "replied_to"
    RELATED_TO = "related_to"


class HypothesisType(str, Enum):
    IDENTITY_MERGE = "identity_merge"
    MISSING_LINK = "missing_link"
    DATA_GAP = "data_gap"
    UNLINKED_ENTITY = "unlinked_entity"
    ANOMALY = "anomaly"


class HypothesisStatus(str, Enum):
    OPEN = "open"
    CONFIRMED = "confirmed"
    DENIED = "denied"
    EXPIRED = "expired"


class ResolutionMethod(str, Enum):
    AUTO = "auto"
    LLM = "llm"
    USER = "user"
    NEW_DATA = "new_data"
    SCROLL = "scroll"


class AnnotationStatus(str, Enum):
    ACTIVE = "active"
    SUPERSEDED = "superseded"
    REMOVED = "removed"


class ProvenanceByType(str, Enum):
    SCROLL = "scroll"
    MODEL = "model"
    USER = "user"
    SYSTEM = "system"


class PipelineStep(str, Enum):
    EXTRACTION = "extraction"
    RESOLUTION = "resolution"
    ENRICHMENT = "enrichment"
    SCORING = "scoring"
    DETECTION = "detection"
    COMPRESSION = "compression"


class IdentifierSystem(str, Enum):
    ISBN = "isbn"
    UPC = "upc"
    EAN = "ean"
    DOI = "doi"
    IMDB = "imdb"
    SPOTIFY = "spotify"
    ASIN = "asin"
    ORCID = "orcid"
    WIKIDATA = "wikidata"
    URL = "url"
    EMAIL = "email"
    PHONE = "phone"
    CUSTOM = "custom"


# ---------------------------------------------------------------------------
# Core Data Structures
# ---------------------------------------------------------------------------

def _new_id() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    return datetime.utcnow()


@dataclass
class Entity:
    id: str = field(default_factory=_new_id)
    type: EntityType = EntityType.PERSON
    properties: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=_now)
    updated_at: datetime = field(default_factory=_now)

    def get(self, key: str, default: Any = None) -> Any:
        return self.properties.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self.properties[key] = value
        self.updated_at = _now()


@dataclass
class Relationship:
    id: str = field(default_factory=_new_id)
    type: RelationshipType = RelationshipType.RELATED_TO
    source_id: str = ""
    target_id: str = ""
    properties: dict[str, Any] = field(default_factory=dict)
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    recorded_at: datetime = field(default_factory=_now)
    superseded_at: datetime | None = None


@dataclass
class Provenance:
    id: str = field(default_factory=_new_id)
    target_type: str = "entity"  # entity | relationship | property
    target_id: str = ""
    source_name: str = ""  # gmail, apple_contacts, whatsapp, etc.
    source_record_id: str = ""  # original vault entry ID
    source_field: str | None = None
    confidence: float = 1.0
    derivation: str | None = None  # null=direct, "entity_resolution", etc.
    ingested_at: datetime = field(default_factory=_now)


@dataclass
class ExternalIdentifier:
    entity_id: str = ""
    system: IdentifierSystem = IdentifierSystem.CUSTOM
    value: str = ""
    verified: bool = False


@dataclass
class Annotation:
    id: str = field(default_factory=_new_id)
    target_id: str = ""
    field_name: str = ""
    value: Any = None
    by_type: ProvenanceByType = ProvenanceByType.SYSTEM
    by_id: str = ""  # scroll id, model name, or user id
    by_version: str | None = None
    cost_tokens: int = 0
    cost_usd: float = 0.0
    created_at: datetime = field(default_factory=_now)
    pipeline_step: PipelineStep = PipelineStep.ENRICHMENT
    trigger: str = "on_ingest"
    parent_ids: list[str] = field(default_factory=list)
    status: AnnotationStatus = AnnotationStatus.ACTIVE


@dataclass
class Hypothesis:
    id: str = field(default_factory=_new_id)
    type: HypothesisType = HypothesisType.MISSING_LINK
    entity_ids: list[str] = field(default_factory=list)
    confidence: float = 0.5
    evidence: dict[str, Any] = field(default_factory=dict)
    status: HypothesisStatus = HypothesisStatus.OPEN
    resolution: ResolutionMethod | None = None
    created_at: datetime = field(default_factory=_now)
    resolved_at: datetime | None = None


@dataclass
class ForgettingRecord:
    id: str = field(default_factory=_new_id)
    action: str = ""  # "forget_entity", "disconnect_source", "user_delete"
    target_description: str = ""  # human-readable what was deleted
    entities_removed: int = 0
    relationships_removed: int = 0
    annotations_removed: int = 0
    reason: str = ""
    created_at: datetime = field(default_factory=_now)


# ---------------------------------------------------------------------------
# Canonical Record — the stable extraction interface
# ---------------------------------------------------------------------------

@dataclass
class CanonicalRecord:
    """
    The universal intermediate format between source adapters and the graph
    builder. Every data source maps its raw data to this format. The graph
    builder is source-agnostic — it only sees CanonicalRecords.

    Adding a new data source = writing one adapter that outputs these.
    Zero changes to graph builder or entity resolution.
    """
    record_type: EntityType = EntityType.PERSON
    source_name: str = ""  # "gmail", "apple_contacts", "whatsapp"
    source_id: str = ""  # original record ID in the source
    data: dict[str, Any] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)  # original for debugging
    fetched_at: datetime = field(default_factory=_now)
    schema_version: int = 1

    @property
    def dedup_key(self) -> str:
        return f"{self.source_name}:{self.source_id}"


# ---------------------------------------------------------------------------
# Canonical field specs per entity type
# ---------------------------------------------------------------------------

CANONICAL_FIELDS: dict[EntityType, list[str]] = {
    EntityType.PERSON: [
        "name", "given_name", "family_name", "emails", "phones",
        "organizations", "photo_url", "nicknames", "addresses",
        "birthdays", "urls", "im_clients", "relations",
    ],
    EntityType.ORGANIZATION: [
        "name", "domain", "org_type", "description", "location",
    ],
    EntityType.PLACE: [
        "name", "latitude", "longitude", "address", "place_type",
    ],
    EntityType.EVENT: [
        "title", "description", "start", "end", "location",
        "attendees", "recurrence", "status",
    ],
    EntityType.MESSAGE: [
        "subject", "body", "sender", "recipients", "cc",
        "date", "thread_id", "attachments", "is_automated",
    ],
    EntityType.FILE: [
        "name", "path", "mime_type", "size_bytes", "created", "modified",
        "media_type",  # image | audio | video | pdf | document
    ],
    EntityType.BOOKMARK: [
        "url", "title", "tags", "created", "description",
    ],
    EntityType.NOTE: [
        "title", "body", "created", "modified", "tags",
    ],
    EntityType.ACCOUNT: [
        "provider", "username", "email", "url",
    ],
}
