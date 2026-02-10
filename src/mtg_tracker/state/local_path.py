"""Local filesystem-backed state backend for Phase 0."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

StateRows = list[dict[str, Any]]


@dataclass(frozen=True)
class LocalPathSettings:
    """Configuration for the local state backend."""

    state_path: Path
    meta_path: Path


class LocalPathBackend:
    """Persist state rows and metadata in local files."""

    def __init__(self, settings: LocalPathSettings):
        self.settings = settings

    def load_state(self) -> tuple[StateRows, dict[str, Any]]:
        if self.settings.state_path.exists():
            with self.settings.state_path.open("r", encoding="utf-8") as handle:
                state_rows: StateRows = json.load(handle)
        else:
            state_rows = []

        if self.settings.meta_path.exists():
            with self.settings.meta_path.open("r", encoding="utf-8") as handle:
                meta = json.load(handle)
        else:
            meta = {}

        return state_rows, meta

    def save_state(self, state_df: StateRows, meta: dict[str, Any]) -> None:
        self.settings.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.settings.meta_path.parent.mkdir(parents=True, exist_ok=True)

        with self.settings.state_path.open("w", encoding="utf-8") as handle:
            json.dump(state_df, handle, indent=2, sort_keys=True)
        with self.settings.meta_path.open("w", encoding="utf-8") as handle:
            json.dump(meta, handle, indent=2, sort_keys=True)
