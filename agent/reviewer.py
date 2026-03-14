"""Scroll Reviewer — evaluate community scrolls for safety.

Checks scroll code for dangerous patterns before allowing execution.
Safe scrolls (pure computation) are auto-approved. Power scrolls
(network, filesystem) require review.
"""
import ast
import logging
import os
from typing import Optional

from core.scrolls.schema import ScrollManifest, ScrollTier

logger = logging.getLogger("nomolo.agent.reviewer")

# Dangerous patterns that should flag a scroll for manual review
DANGEROUS_IMPORTS = {
    "subprocess", "shutil", "ctypes", "signal",
    "multiprocessing", "socket", "ftplib", "smtplib",
    "xmlrpc", "pickle", "shelve", "tempfile",
}

DANGEROUS_BUILTINS = {
    "exec", "eval", "compile", "__import__",
    "globals", "locals", "getattr", "setattr", "delattr",
}

SAFE_NETWORK_MODULES = {
    "requests", "httpx", "aiohttp",
}

# Network-adjacent modules that are actually safe (no I/O)
SAFE_PARSE_MODULES = {
    "urllib.parse", "urllib.robotparser",
}


class ScrollReviewer:
    """Automated scroll safety reviewer."""

    def review(self, scroll_path: str) -> dict:
        """Review a scroll directory for safety.

        Returns:
            {
                "approved": bool,
                "tier": "safe" | "power",
                "issues": [...],
                "warnings": [...],
                "summary": "..."
            }
        """
        issues = []
        warnings = []

        # Check manifest
        manifest_path = os.path.join(scroll_path, "manifest.json")
        if not os.path.isfile(manifest_path):
            return {
                "approved": False,
                "tier": "unknown",
                "issues": ["No manifest.json found"],
                "warnings": [],
                "summary": "Missing manifest",
            }

        import json
        with open(manifest_path) as f:
            manifest = ScrollManifest.from_dict(json.load(f))

        # Check scroll.py exists
        scroll_py = os.path.join(scroll_path, "scroll.py")
        if not os.path.isfile(scroll_py):
            issues.append("No scroll.py found")
            return self._result(False, manifest.tier, issues, warnings)

        # Parse and analyze the code
        with open(scroll_py) as f:
            source = f.read()

        try:
            tree = ast.parse(source)
        except SyntaxError as e:
            issues.append(f"Syntax error: {e}")
            return self._result(False, manifest.tier, issues, warnings)

        # Check for dangerous patterns
        imports = self._extract_imports(tree)
        calls = self._extract_calls(tree)

        # Check dangerous imports
        for imp in imports:
            # Skip known-safe parsing modules (no I/O)
            if imp in SAFE_PARSE_MODULES:
                continue
            module = imp.split(".")[0]
            if module in DANGEROUS_IMPORTS:
                issues.append(f"Dangerous import: {imp}")
            elif module in SAFE_NETWORK_MODULES:
                if manifest.tier == ScrollTier.SAFE:
                    issues.append(
                        f"Network module '{imp}' requires POWER tier, not SAFE"
                    )
                else:
                    warnings.append(f"Uses network module: {imp}")

        # Check dangerous builtins
        for call in calls:
            if call in DANGEROUS_BUILTINS:
                issues.append(f"Dangerous builtin: {call}()")

        # Check for file system operations outside vault
        if "open(" in source and manifest.tier == ScrollTier.SAFE:
            warnings.append("Uses open() — verify it only reads input data")

        # Check that extract() function exists
        has_extract = any(
            isinstance(node, ast.FunctionDef) and node.name == "extract"
            for node in ast.walk(tree)
        )
        if not has_extract:
            issues.append("Missing extract() function")

        # Auto-approve safe scrolls with no issues
        approved = len(issues) == 0
        if manifest.tier == ScrollTier.SAFE and not approved:
            warnings.append("Scroll claims SAFE tier but has issues")

        return self._result(approved, manifest.tier, issues, warnings)

    def _extract_imports(self, tree: ast.AST) -> list[str]:
        """Extract all imported module names."""
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.append(node.module)
        return imports

    def _extract_calls(self, tree: ast.AST) -> list[str]:
        """Extract all function call names."""
        calls = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    calls.append(node.func.id)
                elif isinstance(node.func, ast.Attribute):
                    calls.append(node.func.attr)
        return calls

    @staticmethod
    def _result(approved, tier, issues, warnings) -> dict:
        tier_val = tier.value if isinstance(tier, ScrollTier) else str(tier)
        status = "APPROVED" if approved else "REJECTED"
        n_issues = len(issues)
        n_warnings = len(warnings)
        return {
            "approved": approved,
            "tier": tier_val,
            "issues": issues,
            "warnings": warnings,
            "summary": f"{status} ({n_issues} issues, {n_warnings} warnings)",
        }
