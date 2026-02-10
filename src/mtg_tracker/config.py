"""Configuration loading from YAML-compatible JSON with environment overrides."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class TrackerConfig:
    """Runtime configuration values for the tracker."""

    raw: dict

    @property
    def backend_kind(self) -> str:
        return str(self.raw["state_backend"]["kind"])


def _apply_env_overrides(config: dict) -> dict:
    backend_kind = os.getenv("MTG_TRACKER_STATE_BACKEND")
    if backend_kind:
        config.setdefault("state_backend", {}).update({"kind": backend_kind})
    return config


def load_config(path: str | Path = "config.yaml") -> TrackerConfig:
    """Load config file and apply environment overrides."""
    config_path = Path(path)
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    return TrackerConfig(raw=_apply_env_overrides(raw))
