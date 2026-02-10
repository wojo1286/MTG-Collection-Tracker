from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class LocalPathSettings:
    state_path: Path
    meta_path: Path

    @staticmethod
    def from_dict(d: dict[str, Any]) -> "LocalPathSettings":
        state_path = Path(d.get("state_path", "state/state.parquet"))
        meta_path = Path(d.get("meta_path", "state/meta.json"))
        return LocalPathSettings(state_path=state_path, meta_path=meta_path)


class LocalPathBackend:
    """
    Phase 0 backend: store state files on disk.
    In later phases, the state file will be parquet; here we just treat it as a file blob.
    """

    def __init__(self, settings: LocalPathSettings):
        self.settings = settings

    def load_state(self) -> tuple[Path | None, dict[str, Any]]:
        meta: dict[str, Any] = {}
        if self.settings.meta_path.exists():
            meta = json.loads(self.settings.meta_path.read_text(encoding="utf-8"))

        state_path = self.settings.state_path
        return (state_path if state_path.exists() else None), meta

    def save_state(self, *, state_file: Path, meta: dict[str, Any]) -> None:
        self.settings.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.settings.meta_path.parent.mkdir(parents=True, exist_ok=True)

        # Copy/replace state file
        self.settings.state_path.write_bytes(Path(state_file).read_bytes())
        self.settings.meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8")
