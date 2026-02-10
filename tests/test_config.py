from __future__ import annotations

from pathlib import Path

from mtg_tracker.config import load_config


def test_load_config_defaults(tmp_path: Path) -> None:
    config = load_config(tmp_path / "missing.yaml")

    assert config.state_backend == "local_path"
    assert config.logging_level == "INFO"


def test_env_override(monkeypatch, tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text("logging:\n  level: WARNING\n", encoding="utf-8")
    monkeypatch.setenv("MTG_TRACKER__LOGGING__LEVEL", "debug")

    config = load_config(path)

    assert config.logging_level == "DEBUG"
