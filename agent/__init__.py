"""Nomolo Agent Layer — the thinking that makes data useful.

Three responsibilities:
1. Enricher: autonomously improve vault data quality
2. Metering: track LLM usage and enforce budgets
3. Scroll Reviewer: evaluate community scrolls for safety
"""
from agent.enricher import Enricher
from agent.metering import UsageMeter, UsageRecord
from agent.reviewer import ScrollReviewer
