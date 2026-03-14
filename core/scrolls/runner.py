"""Execute scrolls in a sandboxed environment with metrics collection."""
import importlib.util
import json
import os
import sys
import time
from typing import Optional

from core.scrolls.schema import ScrollManifest, ScrollMetrics, ScrollTier
from core.scrolls.registry import get_scroll, save_scroll_metrics


def validate_scroll(scroll_id: str) -> list[str]:
    """Validate a scroll before running. Returns list of issues (empty = valid)."""
    issues = []
    manifest = get_scroll(scroll_id)
    if not manifest:
        return [f"Scroll not found: {scroll_id}"]

    scroll_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "scrolls", scroll_id
    )
    scroll_py = os.path.join(scroll_dir, "scroll.py")
    if not os.path.isfile(scroll_py):
        issues.append(f"Missing scroll.py in {scroll_dir}")

    # Check required function exists
    if os.path.isfile(scroll_py):
        with open(scroll_py) as f:
            content = f.read()
        if "def extract(" not in content:
            issues.append("scroll.py must define an extract() function")

    # Check dependencies
    for dep in manifest.requires:
        try:
            importlib.import_module(dep.split("[")[0])
        except ImportError:
            issues.append(f"Missing dependency: {dep}")

    return issues


def run_scroll(scroll_id: str, vault_root: str, **kwargs) -> ScrollMetrics:
    """Run a scroll and collect metrics.

    The scroll's extract() function receives:
        vault_root: str — where to write output
        **kwargs — additional arguments (e.g., credentials)

    Returns ScrollMetrics with quality measurements.
    """
    manifest = get_scroll(scroll_id)
    if not manifest:
        raise ValueError(f"Scroll not found: {scroll_id}")

    # Power scrolls need explicit approval (checked by caller)
    scroll_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "scrolls", scroll_id
    )
    scroll_py = os.path.join(scroll_dir, "scroll.py")

    # Load the scroll module
    spec = importlib.util.spec_from_file_location(f"scroll_{scroll_id}", scroll_py)
    module = importlib.util.module_from_spec(spec)

    # Measure execution
    start = time.time()
    spec.loader.exec_module(module)

    result = module.extract(vault_root=vault_root, **kwargs)
    duration = time.time() - start

    # Build metrics from result
    records = result.get("records_extracted", 0)
    total_bytes = result.get("total_bytes", 0)

    metrics = ScrollMetrics(
        scroll_id=scroll_id,
        duration_seconds=duration,
        records_per_second=records / max(duration, 0.001),
        manual_steps=result.get("manual_steps", 0),
        human_wait_seconds=result.get("human_wait_seconds", 0),
        records_extracted=records,
        fields_per_record=result.get("fields_per_record", 0),
        unique_field_names=result.get("unique_field_names", 0),
        has_timestamps=result.get("has_timestamps", False),
        has_relationships=result.get("has_relationships", False),
        bytes_per_record=total_bytes / max(records, 1),
        total_bytes=total_bytes,
        compression_ratio=result.get("compression_ratio", 1.0),
    )

    # Persist metrics
    save_scroll_metrics(metrics)

    return metrics
