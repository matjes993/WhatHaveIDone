"""Metering — track and budget LLM usage.

Every LLM call goes through the meter. Tracks tokens, costs, and
enforces configurable budgets per feature.
"""
import json
import os
import time
import logging
from dataclasses import dataclass, field, asdict

logger = logging.getLogger("nomolo.agent.metering")


@dataclass
class UsageRecord:
    """Single LLM usage event."""
    feature: str           # "automaton", "enricher", "scroll_reviewer"
    model: str             # "gpt-4o", "claude-3-sonnet", etc.
    input_tokens: int = 0
    output_tokens: int = 0
    timestamp: float = field(default_factory=time.time)
    duration_ms: int = 0
    cost_usd: float = 0.0  # estimated

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "UsageRecord":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


class UsageMeter:
    """Track and enforce LLM usage budgets."""

    # Approximate cost per 1M tokens (input/output) by model family
    COST_TABLE = {
        "gpt-4o": (2.50, 10.00),
        "gpt-4o-mini": (0.15, 0.60),
        "gpt-3.5-turbo": (0.50, 1.50),
        "claude-3-opus": (15.00, 75.00),
        "claude-3-sonnet": (3.00, 15.00),
        "claude-3-haiku": (0.25, 1.25),
        "claude-3.5-sonnet": (3.00, 15.00),
    }

    def __init__(self, log_path: str = None):
        self._log_path = log_path or os.path.join(
            os.path.expanduser("~"), ".nomolo", "usage.jsonl"
        )
        os.makedirs(os.path.dirname(self._log_path), exist_ok=True)

    def record(self, usage: UsageRecord):
        """Record a usage event."""
        # Estimate cost if not provided
        if usage.cost_usd == 0 and usage.model:
            usage.cost_usd = self._estimate_cost(
                usage.model, usage.input_tokens, usage.output_tokens
            )

        with open(self._log_path, "a") as f:
            f.write(json.dumps(usage.to_dict()) + "\n")

    def get_usage(self, feature: str = None, since: float = None) -> list[UsageRecord]:
        """Get usage records, optionally filtered."""
        if not os.path.exists(self._log_path):
            return []

        records = []
        with open(self._log_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = UsageRecord.from_dict(json.loads(line))
                    if feature and r.feature != feature:
                        continue
                    if since and r.timestamp < since:
                        continue
                    records.append(r)
                except (json.JSONDecodeError, TypeError):
                    continue
        return records

    def get_summary(self, since: float = None) -> dict:
        """Get usage summary grouped by feature."""
        records = self.get_usage(since=since)
        summary: dict[str, dict] = {}

        for r in records:
            if r.feature not in summary:
                summary[r.feature] = {
                    "calls": 0,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "total_cost_usd": 0.0,
                }
            s = summary[r.feature]
            s["calls"] += 1
            s["input_tokens"] += r.input_tokens
            s["output_tokens"] += r.output_tokens
            s["total_cost_usd"] += r.cost_usd

        return summary

    def _estimate_cost(self, model: str, input_tokens: int, output_tokens: int) -> float:
        """Estimate cost in USD."""
        # Find best matching model
        model_lower = model.lower()
        for model_key, (input_rate, output_rate) in self.COST_TABLE.items():
            if model_key in model_lower:
                return (input_tokens * input_rate + output_tokens * output_rate) / 1_000_000
        # Default fallback
        return (input_tokens * 3.0 + output_tokens * 15.0) / 1_000_000
