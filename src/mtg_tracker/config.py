"""Configuration loading with YAML + environment overrides."""

from __future__ import annotations

import copy
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import yaml  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - exercised only when dependency unavailable
    yaml = None

ENV_PREFIX = "MTG_TRACKER__"


@dataclass(frozen=True)
class Config:
    """Normalized application configuration."""

    raw: dict[str, Any]

    @property
    def logging_level(self) -> str:
        return str(self.raw.get("logging", {}).get("level", "INFO")).upper()

    @property
    def state_backend(self) -> str:
        return str(self.raw.get("state", {}).get("backend", "local_path"))


DEFAULT_CONFIG: dict[str, Any] = {
    "logging": {"level": "INFO"},
    "daily": {
        "state_days": 14,
        "windows": [1, 3, 7],
        "price_floor": 5.0,
        "pct_threshold": 0.20,
        "abs_min": 1.0,
        "pct_override": 0.50,
    },
    "state": {
        "backend": "local_path",
        "local_path": {
            "state_path": "state/state.parquet",
            "meta_path": "state/meta.json",
        },
        "github_release": {
            "repository": "",
            "tag": "state-latest",
            "state_asset_name": "state.parquet",
            "meta_asset_name": "meta.json",
        },
    },
}


def load_config(path: str | Path = "config.yaml") -> Config:
    """Load app config from a YAML file and apply env var overrides.

    Environment overrides use MTG_TRACKER__ with `__` as a nested separator.
    Example: MTG_TRACKER__LOGGING__LEVEL=DEBUG.
    """

    path = Path(path)
    data = copy.deepcopy(DEFAULT_CONFIG)
    if path.exists():
        loaded = _load_yaml(path)
        deep_merge(data, loaded)

    apply_env_overrides(data)
    return Config(raw=data)


def _load_yaml(path: Path) -> dict[str, Any]:
    if yaml is not None:
        with path.open("r", encoding="utf-8") as handle:
            return yaml.safe_load(handle) or {}

    return _parse_simple_yaml(path.read_text(encoding="utf-8"))


def _parse_simple_yaml(content: str) -> dict[str, Any]:
    """Parse a minimal YAML subset (nested maps with scalar values)."""

    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, root)]

    for raw_line in content.splitlines():
        line = raw_line.rstrip()
        if not line or line.lstrip().startswith("#"):
            continue

        indent = len(line) - len(line.lstrip(" "))
        key, _, raw_value = line.strip().partition(":")
        value = raw_value.strip()

        while indent <= stack[-1][0]:
            stack.pop()
        current = stack[-1][1]

        if value == "":
            child: dict[str, Any] = {}
            current[key] = child
            stack.append((indent, child))
        else:
            current[key] = parse_env_value(value.strip('"'))

    return root


def deep_merge(base: dict[str, Any], incoming: dict[str, Any]) -> None:
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            deep_merge(base[key], value)
        else:
            base[key] = value


def apply_env_overrides(config: dict[str, Any]) -> None:
    for key, raw_value in os.environ.items():
        if not key.startswith(ENV_PREFIX):
            continue

        dotted = key.removeprefix(ENV_PREFIX).lower().split("__")
        target = config
        for part in dotted[:-1]:
            target = target.setdefault(part, {})
        target[dotted[-1]] = parse_env_value(raw_value)


def parse_env_value(raw: str) -> Any:
    lowered = raw.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"

    for caster in (int, float):
        try:
            return caster(raw)
        except ValueError:
            pass

    return raw
