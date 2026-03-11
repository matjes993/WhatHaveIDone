"""Tests for whid.py — CLI interface logic."""

import json
import os
import subprocess
import sys

import pytest

from whid import load_config, get_vault_root, validate_gmail_config


class TestLoadConfig:
    def test_loads_valid_yaml(self, tmp_path, monkeypatch):
        config_file = tmp_path / "config.yaml"
        config_file.write_text("vault_root: /tmp/test\ngmail:\n  max_workers: 3\n")

        import whid
        monkeypatch.setattr(whid, "CONFIG_LOCATIONS", [str(config_file)])

        config = load_config()
        assert config["vault_root"] == "/tmp/test"
        assert config["gmail"]["max_workers"] == 3

    def test_returns_empty_on_missing(self, monkeypatch):
        import whid
        monkeypatch.setattr(whid, "CONFIG_LOCATIONS", ["/nonexistent/config.yaml"])

        config = load_config()
        assert config == {}


class TestGetVaultRoot:
    def test_expands_tilde(self):
        config = {"vault_root": "~/Documents/WHID_Vaults"}
        root = get_vault_root(config)
        assert "~" not in root
        assert "Documents/WHID_Vaults" in root

    def test_default_value(self):
        root = get_vault_root({})
        assert "WHID_Vaults" in root


class TestValidateGmailConfig:
    def test_valid_config_passes(self):
        config = {"gmail": {"max_workers": 5, "batch_size": 50}}
        result = validate_gmail_config(config)
        assert result == config

    def test_batch_size_over_100_exits(self):
        config = {"gmail": {"batch_size": 200}}
        with pytest.raises(SystemExit):
            validate_gmail_config(config)

    def test_zero_workers_exits(self):
        config = {"gmail": {"max_workers": 0}}
        with pytest.raises(SystemExit):
            validate_gmail_config(config)

    def test_negative_batch_exits(self):
        config = {"gmail": {"batch_size": -1}}
        with pytest.raises(SystemExit):
            validate_gmail_config(config)

    def test_empty_config_uses_defaults(self):
        config = {}
        result = validate_gmail_config(config)
        assert result == config  # no crash


class TestCLIHelp:
    def test_help_flag(self):
        project_root = os.path.dirname(os.path.dirname(__file__))
        whid_script = os.path.join(project_root, "whid.py")
        result = subprocess.run(
            [sys.executable, whid_script, "--help"],
            capture_output=True,
            text=True,
            cwd=project_root,
        )
        assert result.returncode == 0
        assert "WhatHaveIDone" in result.stdout
        assert "collect" in result.stdout
        assert "groom" in result.stdout
        assert "status" in result.stdout

    def test_no_args_shows_help(self):
        project_root = os.path.dirname(os.path.dirname(__file__))
        whid_script = os.path.join(project_root, "whid.py")
        result = subprocess.run(
            [sys.executable, whid_script],
            capture_output=True,
            text=True,
            cwd=project_root,
        )
        assert result.returncode == 0
        assert "Quick start" in result.stdout

    def test_unknown_source_exits(self):
        project_root = os.path.dirname(os.path.dirname(__file__))
        whid_script = os.path.join(project_root, "whid.py")
        result = subprocess.run(
            [sys.executable, whid_script, "collect", "fakesource"],
            capture_output=True,
            text=True,
            cwd=project_root,
        )
        assert result.returncode == 1
        assert "Unknown source" in result.stdout or "Unknown source" in result.stderr
