"""GitHub release-backed state backend stub for Phase 0."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

StateRows = list[dict[str, Any]]


@dataclass(frozen=True)
class GitHubReleaseSettings:
    """Configuration for the GitHub release backend."""

    repository: str
    tag: str = "state-latest"
    state_asset_name: str = "state.parquet"
    meta_asset_name: str = "meta.json"


class GitHubReleaseBackend:
    """Phase 0 backend shell; real network behavior is Phase 3 scope."""

    def __init__(self, settings: GitHubReleaseSettings):
        self.settings = settings

    def load_state(self) -> tuple[StateRows, dict[str, Any]]:
        raise NotImplementedError("GitHubReleaseBackend network operations are Phase 3 scope.")

    def save_state(self, state_df: StateRows, meta: dict[str, Any]) -> None:
        raise NotImplementedError("GitHubReleaseBackend network operations are Phase 3 scope.")

    def validate(self) -> None:
        if not self.settings.repository.strip():
            raise ValueError("GitHub release backend requires a non-empty repository setting.")

    def local_cache_paths(self, cache_dir: str | Path) -> tuple[Path, Path]:
        cache_path = Path(cache_dir)
        return (
            cache_path / self.settings.state_asset_name,
            cache_path / self.settings.meta_asset_name,
        )
