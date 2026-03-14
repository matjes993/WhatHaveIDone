"""Enricher — autonomous data quality improvement.

Runs enrichment passes over the knowledge graph, generating hypotheses
and filling gaps. Uses LLM for ambiguous cases, deterministic rules for
clear ones.
"""
import logging
import re
from typing import Optional

logger = logging.getLogger("nomolo.agent.enricher")


class Enricher:
    """Autonomous data quality engine."""

    def __init__(self, knowledge_engine=None):
        self.engine = knowledge_engine
        self._passes = [
            self._extract_names_from_emails,
            self._detect_duplicate_contacts,
            self._link_cross_source_entities,
            self._fill_missing_timestamps,
        ]

    def run_all(self) -> dict:
        """Run all enrichment passes. Returns summary of changes."""
        if not self.engine:
            return {"error": "No knowledge engine configured"}

        results = {}
        for pass_fn in self._passes:
            name = pass_fn.__name__.lstrip("_")
            try:
                result = pass_fn()
                results[name] = result
                logger.info("Enrichment pass %s: %s", name, result)
            except Exception as e:
                results[name] = {"error": str(e)}
                logger.warning("Enrichment pass %s failed: %s", name, e)

        return results

    def _extract_names_from_emails(self) -> dict:
        """Extract probable names from email addresses like john.doe@company.com."""
        from core.knowledge.schema import EntityType, IdentifierSystem

        updated = 0
        people = self.engine.find_entities(EntityType.PERSON, limit=10000)
        for person in people:
            name = person.properties.get("name", "")
            if name and not name.startswith("?"):
                continue  # Already has a real name

            # Try to extract from email identifiers
            identifiers = self.engine.get_identifiers(person.id)
            for ident in identifiers:
                if ident.system == IdentifierSystem.EMAIL:
                    extracted = self._name_from_email(ident.value)
                    if extracted:
                        person.properties["name"] = extracted
                        self.engine.update_entity(person)
                        updated += 1
                        break

        return {"updated": updated}

    def _detect_duplicate_contacts(self) -> dict:
        """Find contacts that likely refer to the same person."""
        from core.knowledge.schema import EntityType, HypothesisType

        people = self.engine.find_entities(EntityType.PERSON, limit=10000)
        hypotheses_created = 0

        # Group by normalized name
        name_groups: dict[str, list] = {}
        for person in people:
            name = person.properties.get("name", "")
            if not name or name == "?":
                continue
            normalized = self._normalize_name(name)
            if normalized:
                name_groups.setdefault(normalized, []).append(person)

        # Create hypotheses for groups with multiple entities
        for normalized, group in name_groups.items():
            if len(group) < 2:
                continue
            # Check if hypothesis already exists
            existing = self.engine.get_open_hypotheses(limit=1000)
            entity_ids = {p.id for p in group}
            already_exists = any(
                set(h.entity_ids) == entity_ids for h in existing
            )
            if already_exists:
                continue

            self.engine.create_hypothesis(
                hypothesis_type=HypothesisType.DUPLICATE,
                entity_ids=list(entity_ids),
                confidence=0.7,
                evidence=f"Same normalized name: '{normalized}' across {len(group)} entities",
            )
            hypotheses_created += 1

        return {"hypotheses_created": hypotheses_created}

    def _link_cross_source_entities(self) -> dict:
        """Link entities that appear in multiple sources via shared identifiers."""
        # This is handled by entity resolution during ingestion,
        # but we can catch cases that were missed
        return {"status": "deferred_to_resolution"}

    def _fill_missing_timestamps(self) -> dict:
        """Fill missing timestamps from related entities."""
        return {"status": "deferred"}

    @staticmethod
    def _name_from_email(email: str) -> Optional[str]:
        """Extract a probable name from an email address."""
        local = email.split("@")[0]
        # Remove common prefixes/suffixes
        for pattern in ["noreply", "no-reply", "info", "support", "admin",
                        "contact", "hello", "team", "mail", "help"]:
            if local.lower() == pattern:
                return None

        # Split on dots, dashes, underscores
        parts = re.split(r"[._\-+]", local)
        if len(parts) >= 2:
            # john.doe -> John Doe
            name = " ".join(p.capitalize() for p in parts if len(p) > 1 and p.isalpha())
            if name and len(name) > 3:
                return name
        return None

    @staticmethod
    def _normalize_name(name: str) -> str:
        """Normalize a name for comparison."""
        # Remove titles, special chars
        name = re.sub(r"^(Mr\.?|Mrs\.?|Ms\.?|Dr\.?|Prof\.?)\s+", "", name, flags=re.I)
        parts = name.lower().split()
        parts = [p for p in parts if len(p) > 1]
        return " ".join(sorted(parts))
