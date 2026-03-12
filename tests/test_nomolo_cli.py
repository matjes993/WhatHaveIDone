"""Tests for nomolo.py — CLI interface logic."""

import json
import os
import subprocess
import sys

import pytest

from nomolo import (
    load_config,
    get_vault_root,
    validate_gmail_config,
    _find_credentials_file,
    cmd_status,
    cmd_update,
    PROJECT_ROOT,
)


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------
class TestLoadConfig:
    def test_loads_valid_yaml(self, tmp_path, monkeypatch):
        config_file = tmp_path / "config.yaml"
        config_file.write_text("vault_root: /tmp/test\ngmail:\n  max_workers: 3\n")

        import nomolo
        monkeypatch.setattr(nomolo, "CONFIG_LOCATIONS", [str(config_file)])

        config = load_config()
        assert config["vault_root"] == "/tmp/test"
        assert config["gmail"]["max_workers"] == 3

    def test_returns_empty_on_missing(self, monkeypatch):
        import nomolo
        monkeypatch.setattr(nomolo, "CONFIG_LOCATIONS", ["/nonexistent/config.yaml"])

        config = load_config()
        assert config == {}

    def test_empty_config_file_returns_empty(self, tmp_path, monkeypatch):
        config_file = tmp_path / "config.yaml"
        config_file.write_text("")

        import nomolo
        monkeypatch.setattr(nomolo, "CONFIG_LOCATIONS", [str(config_file)])

        config = load_config()
        assert config == {}

    def test_invalid_yaml_exits(self, tmp_path, monkeypatch):
        config_file = tmp_path / "config.yaml"
        config_file.write_text("invalid: yaml: [broken\n")

        import nomolo
        monkeypatch.setattr(nomolo, "CONFIG_LOCATIONS", [str(config_file)])

        with pytest.raises(SystemExit):
            load_config()

    def test_first_found_config_wins(self, tmp_path, monkeypatch):
        config1 = tmp_path / "config1.yaml"
        config2 = tmp_path / "config2.yaml"
        config1.write_text("vault_root: /first\n")
        config2.write_text("vault_root: /second\n")

        import nomolo
        monkeypatch.setattr(nomolo, "CONFIG_LOCATIONS", [str(config1), str(config2)])

        config = load_config()
        assert config["vault_root"] == "/first"


# ---------------------------------------------------------------------------
# Vault root
# ---------------------------------------------------------------------------
class TestGetVaultRoot:
    def test_expands_tilde(self):
        config = {"vault_root": "~/my_vaults"}
        root = get_vault_root(config)
        assert "~" not in root
        assert "my_vaults" in root

    def test_default_value(self):
        root = get_vault_root({})
        assert root.endswith("vaults")

    def test_nonexistent_parent_exits(self):
        config = {"vault_root": "/nonexistent_parent_12345/vaults"}
        with pytest.raises(SystemExit):
            get_vault_root(config)

    def test_absolute_path_preserved(self):
        config = {"vault_root": "/tmp/nomolo_test_vaults"}
        root = get_vault_root(config)
        assert root == "/tmp/nomolo_test_vaults"


# ---------------------------------------------------------------------------
# Gmail config validation
# ---------------------------------------------------------------------------
class TestValidateGmailConfig:
    def test_valid_config_passes(self):
        config = {"gmail": {"max_workers": 5, "batch_size": 50}}
        result = validate_gmail_config(config)
        assert result == config

    def test_batch_size_over_100_exits(self):
        config = {"gmail": {"batch_size": 200}}
        with pytest.raises(SystemExit):
            validate_gmail_config(config)

    def test_batch_size_exactly_100_passes(self):
        config = {"gmail": {"batch_size": 100}}
        result = validate_gmail_config(config)
        assert result["gmail"]["batch_size"] == 100

    def test_zero_workers_exits(self):
        config = {"gmail": {"max_workers": 0}}
        with pytest.raises(SystemExit):
            validate_gmail_config(config)

    def test_negative_batch_exits(self):
        config = {"gmail": {"batch_size": -1}}
        with pytest.raises(SystemExit):
            validate_gmail_config(config)

    def test_negative_workers_exits(self):
        config = {"gmail": {"max_workers": -5}}
        with pytest.raises(SystemExit):
            validate_gmail_config(config)

    def test_string_workers_exits(self):
        config = {"gmail": {"max_workers": "five"}}
        with pytest.raises(SystemExit):
            validate_gmail_config(config)

    def test_string_batch_size_exits(self):
        config = {"gmail": {"batch_size": "fifty"}}
        with pytest.raises(SystemExit):
            validate_gmail_config(config)

    def test_empty_config_uses_defaults(self):
        config = {}
        result = validate_gmail_config(config)
        assert result == config  # no crash

    def test_empty_gmail_section_uses_defaults(self):
        config = {"gmail": {}}
        result = validate_gmail_config(config)
        assert result == config


# ---------------------------------------------------------------------------
# Find credentials file
# ---------------------------------------------------------------------------
class TestFindCredentialsFile:
    def test_finds_credentials_in_project_root(self, tmp_path, monkeypatch):
        creds = {"installed": {"client_id": "test"}}
        creds_file = tmp_path / "credentials.json"
        creds_file.write_text(json.dumps(creds))

        import nomolo
        monkeypatch.setattr(nomolo, "PROJECT_ROOT", str(tmp_path))

        result = _find_credentials_file()
        assert result == str(creds_file)

    def test_skips_invalid_json(self, tmp_path, monkeypatch):
        creds_file = tmp_path / "credentials.json"
        creds_file.write_text("not json")

        import nomolo
        monkeypatch.setattr(nomolo, "PROJECT_ROOT", str(tmp_path))

        # Should not find it since it's invalid JSON
        result = _find_credentials_file()
        # Result may be None or a different file
        assert result != str(creds_file)

    def test_skips_non_google_json(self, tmp_path, monkeypatch):
        creds_file = tmp_path / "credentials.json"
        creds_file.write_text(json.dumps({"not_google": True}))

        import nomolo
        monkeypatch.setattr(nomolo, "PROJECT_ROOT", str(tmp_path))

        result = _find_credentials_file()
        assert result != str(creds_file)

    def test_returns_none_when_nothing_found(self, tmp_path, monkeypatch):
        import nomolo
        monkeypatch.setattr(nomolo, "PROJECT_ROOT", str(tmp_path))
        # Also prevent searching Downloads
        monkeypatch.setenv("HOME", str(tmp_path))

        result = _find_credentials_file()
        assert result is None


# ---------------------------------------------------------------------------
# cmd_status
# ---------------------------------------------------------------------------
class TestCmdStatus:
    def test_no_vaults_directory(self, tmp_path, capsys):
        config = {"vault_root": str(tmp_path / "nonexistent")}
        args = type("Args", (), {"command": "status"})()
        cmd_status(args, config)
        output = capsys.readouterr().out
        assert "No vaults found" in output

    def test_empty_vault_root(self, tmp_path, capsys):
        vault_root = tmp_path / "vaults"
        vault_root.mkdir()
        config = {"vault_root": str(vault_root)}
        args = type("Args", (), {"command": "status"})()
        cmd_status(args, config)
        output = capsys.readouterr().out
        assert "no vaults yet" in output

    def test_vault_with_entries(self, tmp_path, capsys):
        vault_root = tmp_path / "vaults"
        gmail_vault = vault_root / "Gmail_Primary"
        year_dir = gmail_vault / "2024"
        year_dir.mkdir(parents=True)

        # Write some entries
        entries = [
            json.dumps({"id": f"msg{i}", "subject": f"Test {i}"})
            for i in range(5)
        ]
        (year_dir / "01_January.jsonl").write_text("\n".join(entries) + "\n")

        # Write processed_ids
        (gmail_vault / "processed_ids.txt").write_text(
            "\n".join(f"msg{i}" for i in range(5)) + "\n"
        )

        config = {"vault_root": str(vault_root)}
        args = type("Args", (), {"command": "status"})()
        cmd_status(args, config)
        output = capsys.readouterr().out
        assert "Gmail_Primary" in output
        assert "5" in output  # entries count
        assert "OK" in output

    def test_vault_with_ghosts(self, tmp_path, capsys):
        vault_root = tmp_path / "vaults"
        gmail_vault = vault_root / "Gmail_Test"
        gmail_vault.mkdir(parents=True)

        (gmail_vault / "processed_ids.txt").write_text("msg1\nmsg2\n")
        (gmail_vault / "missing_ids.txt").write_text("msg3\n")

        config = {"vault_root": str(vault_root)}
        args = type("Args", (), {"command": "status"})()
        cmd_status(args, config)
        output = capsys.readouterr().out
        assert "GHOSTS" in output


# ---------------------------------------------------------------------------
# cmd_update
# ---------------------------------------------------------------------------
class TestCmdUpdate:
    def test_update_runs_git_pull(self, monkeypatch):
        """cmd_update should call git pull --ff-only."""
        import subprocess as sp
        calls = []

        original_run = sp.run

        def mock_run(cmd, **kwargs):
            calls.append(cmd)
            result = type("Result", (), {
                "returncode": 0,
                "stdout": "Already up to date.\n",
                "stderr": "",
            })()
            return result

        monkeypatch.setattr(sp, "run", mock_run)
        cmd_update()

        assert any("git" in str(c) for c in calls)
        assert any("pull" in str(c) for c in calls)


# ---------------------------------------------------------------------------
# CLI subprocess tests
# ---------------------------------------------------------------------------
class TestCLIHelp:
    def test_help_flag(self):
        project_root = os.path.dirname(os.path.dirname(__file__))
        nomolo_script = os.path.join(project_root, "nomolo.py")
        result = subprocess.run(
            [sys.executable, nomolo_script, "--help"],
            capture_output=True,
            text=True,
            cwd=project_root,
        )
        assert result.returncode == 0
        assert "Nomolo" in result.stdout
        assert "collect" in result.stdout
        assert "groom" in result.stdout
        assert "status" in result.stdout
        assert "update" in result.stdout

    def test_no_args_shows_help(self):
        project_root = os.path.dirname(os.path.dirname(__file__))
        nomolo_script = os.path.join(project_root, "nomolo.py")
        result = subprocess.run(
            [sys.executable, nomolo_script],
            capture_output=True,
            text=True,
            cwd=project_root,
        )
        assert result.returncode == 0
        assert "Quick start" in result.stdout
        assert "nomolo update" in result.stdout

    def test_unknown_source_exits(self):
        project_root = os.path.dirname(os.path.dirname(__file__))
        nomolo_script = os.path.join(project_root, "nomolo.py")
        result = subprocess.run(
            [sys.executable, nomolo_script, "collect", "fakesource"],
            capture_output=True,
            text=True,
            cwd=project_root,
        )
        assert result.returncode == 1
        assert "Unknown source" in result.stdout or "Unknown source" in result.stderr

    def test_collect_without_credentials_exits(self):
        project_root = os.path.dirname(os.path.dirname(__file__))
        nomolo_script = os.path.join(project_root, "nomolo.py")
        result = subprocess.run(
            [sys.executable, nomolo_script, "collect", "gmail"],
            capture_output=True,
            text=True,
            cwd=project_root,
            env={**os.environ, "HOME": "/tmp/nomolo_test_no_creds"},
        )
        assert result.returncode == 1
        assert "not set up" in result.stdout or "setup" in result.stdout.lower()
