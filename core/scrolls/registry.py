"""Registry for discovering, installing, and managing scrolls."""
import json
import os
from typing import Optional
from core.scrolls.schema import ScrollManifest, ScrollMetrics


SCROLLS_DIR = "scrolls"  # relative to project root


def _scrolls_root() -> str:
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(project_root, SCROLLS_DIR)


def list_scrolls() -> list[ScrollManifest]:
    """List all installed scrolls."""
    root = _scrolls_root()
    if not os.path.isdir(root):
        return []

    scrolls = []
    for name in sorted(os.listdir(root)):
        manifest_path = os.path.join(root, name, "manifest.json")
        if os.path.isfile(manifest_path):
            try:
                with open(manifest_path) as f:
                    data = json.load(f)
                scrolls.append(ScrollManifest.from_dict(data))
            except Exception:
                continue
    return scrolls


def get_scroll(scroll_id: str) -> Optional[ScrollManifest]:
    """Get a specific scroll by ID."""
    root = _scrolls_root()
    manifest_path = os.path.join(root, scroll_id, "manifest.json")
    if not os.path.isfile(manifest_path):
        return None
    with open(manifest_path) as f:
        return ScrollManifest.from_dict(json.load(f))


def install_scroll(source_path: str) -> ScrollManifest:
    """Install a scroll from a directory. Copies manifest and scroll.py."""
    manifest_path = os.path.join(source_path, "manifest.json")
    if not os.path.isfile(manifest_path):
        raise FileNotFoundError(f"No manifest.json in {source_path}")

    with open(manifest_path) as f:
        manifest = ScrollManifest.from_dict(json.load(f))

    dest = os.path.join(_scrolls_root(), manifest.id)
    os.makedirs(dest, exist_ok=True)

    # Copy manifest
    import shutil
    for fname in os.listdir(source_path):
        src = os.path.join(source_path, fname)
        if os.path.isfile(src):
            shutil.copy2(src, os.path.join(dest, fname))

    return manifest


def get_scroll_metrics(scroll_id: str) -> list[ScrollMetrics]:
    """Get historical metrics for a scroll."""
    root = _scrolls_root()
    metrics_path = os.path.join(root, scroll_id, "metrics.jsonl")
    if not os.path.isfile(metrics_path):
        return []

    metrics = []
    with open(metrics_path) as f:
        for line in f:
            line = line.strip()
            if line:
                metrics.append(ScrollMetrics.from_dict(json.loads(line)))
    return metrics


def save_scroll_metrics(metrics: ScrollMetrics):
    """Append metrics from a scroll run."""
    root = _scrolls_root()
    scroll_dir = os.path.join(root, metrics.scroll_id)
    os.makedirs(scroll_dir, exist_ok=True)
    metrics_path = os.path.join(scroll_dir, "metrics.jsonl")
    with open(metrics_path, "a") as f:
        f.write(json.dumps(metrics.to_dict()) + "\n")


def get_best_scroll_for_source(target_source: str) -> Optional[ScrollManifest]:
    """Find the highest-quality scroll for a given data source."""
    scrolls = [s for s in list_scrolls() if s.target_source == target_source]
    if not scrolls:
        return None

    # Rank by average quality score of last 5 runs
    best = None
    best_score = -1
    for scroll in scrolls:
        metrics = get_scroll_metrics(scroll.id)
        if not metrics:
            score = 0
        else:
            recent = metrics[-5:]
            score = sum(m.quality_score for m in recent) / len(recent)
        if score > best_score:
            best = scroll
            best_score = score

    return best
