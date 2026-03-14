"""
Nomolo Knowledge Graph — Entity Resolution

3-tier deterministic entity resolution. No LLM calls.
Tier 1: Exact match on strong identifiers (email, phone, URL)
Tier 2: Probabilistic name matching (Jaro-Winkler with blocking)
Tier 3: Graph-based co-occurrence boosting
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Any

from core.knowledge.graph_store import GraphStore
from core.knowledge.schema import (
    Entity,
    EntityType,
    HypothesisType,
    Hypothesis,
    IdentifierSystem,
    RelationshipType,
)


@dataclass
class MergeCandidate:
    entity_id: str
    confidence: float
    match_reasons: list[str] = field(default_factory=list)
    tier: int = 1


# ---------------------------------------------------------------------------
# String similarity
# ---------------------------------------------------------------------------

def jaro_similarity(s1: str, s2: str) -> float:
    if s1 == s2:
        return 1.0
    if not s1 or not s2:
        return 0.0

    len_s1, len_s2 = len(s1), len(s2)
    match_distance = max(len_s1, len_s2) // 2 - 1
    if match_distance < 0:
        match_distance = 0

    s1_matches = [False] * len_s1
    s2_matches = [False] * len_s2
    matches = 0
    transpositions = 0

    for i in range(len_s1):
        start = max(0, i - match_distance)
        end = min(i + match_distance + 1, len_s2)
        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    k = 0
    for i in range(len_s1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1

    jaro = (
        matches / len_s1 + matches / len_s2 + (matches - transpositions / 2) / matches
    ) / 3
    return jaro


def jaro_winkler_similarity(s1: str, s2: str, prefix_weight: float = 0.1) -> float:
    jaro = jaro_similarity(s1, s2)
    prefix_len = 0
    for i in range(min(4, min(len(s1), len(s2)))):
        if s1[i] == s2[i]:
            prefix_len += 1
        else:
            break
    return jaro + prefix_len * prefix_weight * (1 - jaro)


def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = name.strip().lower()
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r"\s+", " ", name)
    return name


def normalize_email(email: str) -> str:
    if not email:
        return ""
    return email.strip().lower()


def normalize_phone(phone: str) -> str:
    if not phone:
        return ""
    return re.sub(r"[^\d+]", "", phone)


def _extract_emails(entity: Entity) -> set[str]:
    emails = set()
    props = entity.properties
    for key in ("emails", "email"):
        val = props.get(key)
        if isinstance(val, list):
            for item in val:
                if isinstance(item, dict):
                    e = item.get("value") or item.get("email")
                    if e:
                        emails.add(normalize_email(e))
                elif isinstance(item, str):
                    emails.add(normalize_email(item))
        elif isinstance(val, str) and val:
            emails.add(normalize_email(val))
    return emails


def _extract_phones(entity: Entity) -> set[str]:
    phones = set()
    props = entity.properties
    for key in ("phones", "phone", "phone_numbers"):
        val = props.get(key)
        if isinstance(val, list):
            for item in val:
                if isinstance(item, dict):
                    p = item.get("value") or item.get("phone")
                    if p:
                        phones.add(normalize_phone(p))
                elif isinstance(item, str):
                    phones.add(normalize_phone(item))
        elif isinstance(val, str) and val:
            phones.add(normalize_phone(val))
    return phones


def _extract_name(entity: Entity) -> str:
    props = entity.properties
    name = props.get("name", "")
    if not name:
        given = props.get("given_name", "")
        family = props.get("family_name", "")
        name = f"{given} {family}".strip()
    return normalize_name(name)


def _extract_org_names(entity: Entity) -> set[str]:
    orgs = set()
    props = entity.properties
    val = props.get("organizations")
    if isinstance(val, list):
        for item in val:
            if isinstance(item, dict):
                n = item.get("name")
                if n:
                    orgs.add(normalize_name(n))
            elif isinstance(item, str):
                orgs.add(normalize_name(item))
    name = props.get("name")
    if name and entity.type == EntityType.ORGANIZATION:
        orgs.add(normalize_name(name))
    return orgs


# ---------------------------------------------------------------------------
# Entity Resolver
# ---------------------------------------------------------------------------

class EntityResolver:
    """
    Resolves incoming entities against existing graph entities.
    Returns merge candidates with confidence scores.
    """

    # Thresholds
    AUTO_MERGE_THRESHOLD = 0.95
    REVIEW_THRESHOLD = 0.70
    NAME_SIMILARITY_THRESHOLD = 0.85

    def __init__(self, store: GraphStore):
        self.store = store

    def find_matches(
        self,
        entity: Entity,
        candidate_types: list[EntityType] | None = None,
    ) -> list[MergeCandidate]:
        """Find matching entities across all tiers."""
        if candidate_types is None:
            candidate_types = [entity.type]

        candidates: dict[str, MergeCandidate] = {}

        # Tier 1: Deterministic
        self._tier1_exact_match(entity, candidates)

        # Tier 2: Probabilistic (only for person/org types)
        if entity.type in (EntityType.PERSON, EntityType.ORGANIZATION):
            self._tier2_probabilistic(entity, candidates, candidate_types)

        # Tier 3: Graph-based co-occurrence
        self._tier3_cooccurrence(entity, candidates)

        result = sorted(candidates.values(), key=lambda c: c.confidence, reverse=True)
        return result

    def should_auto_merge(self, candidate: MergeCandidate) -> bool:
        return candidate.confidence >= self.AUTO_MERGE_THRESHOLD

    def should_review(self, candidate: MergeCandidate) -> bool:
        return self.REVIEW_THRESHOLD <= candidate.confidence < self.AUTO_MERGE_THRESHOLD

    def create_merge_hypothesis(
        self, entity_id: str, candidate: MergeCandidate
    ) -> Hypothesis:
        return Hypothesis(
            type=HypothesisType.IDENTITY_MERGE,
            entity_ids=[entity_id, candidate.entity_id],
            confidence=candidate.confidence,
            evidence={
                "match_reasons": candidate.match_reasons,
                "tier": candidate.tier,
            },
        )

    def merge_entities(self, keep_id: str, merge_id: str) -> None:
        """Merge merge_id into keep_id. Transfers all relationships and provenance."""
        store = self.store
        keep = store.get_entity(keep_id)
        merge = store.get_entity(merge_id)
        if not keep or not merge:
            return

        # Transfer relationships
        rels = store.get_relationships(merge_id, current_only=False)
        for rel in rels:
            new_source = keep_id if rel.source_id == merge_id else rel.source_id
            new_target = keep_id if rel.target_id == merge_id else rel.target_id
            if new_source == new_target:
                continue  # skip self-loops
            from core.knowledge.schema import Relationship as RelSchema
            new_rel = RelSchema(
                type=rel.type,
                source_id=new_source,
                target_id=new_target,
                properties=rel.properties,
                valid_from=rel.valid_from,
                valid_to=rel.valid_to,
            )
            store.create_relationship(new_rel)

        # Transfer provenance
        provs = store.get_provenance(merge_id)
        for prov in provs:
            from core.knowledge.schema import Provenance as ProvSchema
            new_prov = ProvSchema(
                target_type=prov.target_type,
                target_id=keep_id,
                source_name=prov.source_name,
                source_record_id=prov.source_record_id,
                source_field=prov.source_field,
                confidence=prov.confidence,
                derivation="entity_resolution",
            )
            store.add_provenance(new_prov)

        # Transfer identifiers
        idents = store.get_identifiers(merge_id)
        for ident in idents:
            from core.knowledge.schema import ExternalIdentifier
            new_ident = ExternalIdentifier(
                entity_id=keep_id,
                system=ident.system,
                value=ident.value,
                verified=ident.verified,
            )
            store.add_identifier(new_ident)

        # Merge properties (keep existing, add missing)
        for key, value in merge.properties.items():
            if key not in keep.properties:
                keep.properties[key] = value
            elif isinstance(keep.properties[key], list) and isinstance(value, list):
                existing_strs = {str(v) for v in keep.properties[key]}
                for v in value:
                    if str(v) not in existing_strs:
                        keep.properties[key].append(v)
        store.update_entity(keep)

        # Delete merged entity
        store.delete_entity(merge_id)

    # -----------------------------------------------------------------------
    # Tier 1: Exact match on strong identifiers
    # -----------------------------------------------------------------------

    def _tier1_exact_match(
        self, entity: Entity, candidates: dict[str, MergeCandidate]
    ) -> None:
        emails = _extract_emails(entity)
        for email in emails:
            entity_ids = self.store.find_entities_by_identifier(
                IdentifierSystem.EMAIL, email
            )
            for eid in entity_ids:
                if eid == entity.id:
                    continue
                if eid not in candidates:
                    candidates[eid] = MergeCandidate(
                        entity_id=eid, confidence=1.0, tier=1
                    )
                candidates[eid].match_reasons.append(f"exact_email:{email}")
                candidates[eid].confidence = 1.0

        phones = _extract_phones(entity)
        for phone in phones:
            entity_ids = self.store.find_entities_by_identifier(
                IdentifierSystem.PHONE, phone
            )
            for eid in entity_ids:
                if eid == entity.id:
                    continue
                if eid not in candidates:
                    candidates[eid] = MergeCandidate(
                        entity_id=eid, confidence=1.0, tier=1
                    )
                candidates[eid].match_reasons.append(f"exact_phone:{phone}")
                candidates[eid].confidence = 1.0

    # -----------------------------------------------------------------------
    # Tier 2: Probabilistic name matching
    # -----------------------------------------------------------------------

    def _tier2_probabilistic(
        self,
        entity: Entity,
        candidates: dict[str, MergeCandidate],
        candidate_types: list[EntityType],
    ) -> None:
        name = _extract_name(entity)
        if not name or len(name) < 2:
            return

        # Blocking: first letter of the name
        first_char = name[0]

        for etype in candidate_types:
            existing = self.store.find_entities(entity_type=etype, limit=10000)
            for existing_entity in existing:
                if existing_entity.id == entity.id:
                    continue
                if existing_entity.id in candidates:
                    continue

                existing_name = _extract_name(existing_entity)
                if not existing_name:
                    continue

                # Blocking check
                if existing_name[0] != first_char:
                    continue

                sim = jaro_winkler_similarity(name, existing_name)
                if sim < self.NAME_SIMILARITY_THRESHOLD:
                    continue

                # Boost if same org
                entity_orgs = _extract_org_names(entity)
                existing_orgs = _extract_org_names(existing_entity)
                org_overlap = entity_orgs & existing_orgs
                if org_overlap:
                    sim = min(sim + 0.05, 1.0)

                candidate = MergeCandidate(
                    entity_id=existing_entity.id,
                    confidence=sim,
                    match_reasons=[f"name_similarity:{sim:.3f}"],
                    tier=2,
                )
                if org_overlap:
                    candidate.match_reasons.append(
                        f"shared_org:{','.join(org_overlap)}"
                    )
                candidates[existing_entity.id] = candidate

    # -----------------------------------------------------------------------
    # Tier 3: Graph-based co-occurrence
    # -----------------------------------------------------------------------

    def _tier3_cooccurrence(
        self, entity: Entity, candidates: dict[str, MergeCandidate]
    ) -> None:
        """Boost candidates that share relationships with the entity."""
        if not entity.id:
            return

        entity_rels = self.store.get_relationships(entity.id, current_only=True)
        if not entity_rels:
            return

        entity_neighbors = set()
        for rel in entity_rels:
            other = rel.target_id if rel.source_id == entity.id else rel.source_id
            entity_neighbors.add(other)

        for cid, candidate in list(candidates.items()):
            if candidate.confidence >= 1.0:
                continue  # already certain

            cand_rels = self.store.get_relationships(cid, current_only=True)
            cand_neighbors = set()
            for rel in cand_rels:
                other = rel.target_id if rel.source_id == cid else rel.source_id
                cand_neighbors.add(other)

            shared = entity_neighbors & cand_neighbors
            if len(shared) >= 3:
                boost = min(len(shared) * 0.03, 0.15)
                candidate.confidence = min(candidate.confidence + boost, 0.99)
                candidate.match_reasons.append(
                    f"shared_connections:{len(shared)}"
                )
                candidate.tier = max(candidate.tier, 3)
