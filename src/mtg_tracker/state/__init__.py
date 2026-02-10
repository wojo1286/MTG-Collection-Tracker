"""State backend factory and exports for Phase 0."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from mtg_tracker.config import Config
from mtg_tracker.state.github_release import GitHubReleaseBackend, GitHubReleaseSettings
from mtg_tracker.state.local_path import LocalPathBackend, LocalPathSettings


class StateBackend(Protocol):
    def load_state(self) -> tuple[list[dict[str, object]], dict[str, object]]: ...

    def save_state(self, state_df: list[dict[str, object]], meta: dict[str, object]) -> None: ...


def build_state_backend(config: Config) -> StateBackend:
    """Build the configured backend from the loaded config object."""

    state_config = config.raw.get("state", {})
    backend_name = state_config.get("backend", "local_path")

    if backend_name == "local_path":
        local_cfg = state_config.get("local_path", {})
        return LocalPathBackend(
            LocalPathSettings(
                state_path=Path(local_cfg.get("state_path", "state/state.parquet")),
                meta_path=Path(local_cfg.get("meta_path", "state/meta.json")),
            )
        )

    if backend_name == "github_release":
        gh_cfg = state_config.get("github_release", {})
        backend = GitHubReleaseBackend(
            GitHubReleaseSettings(
                repository=gh_cfg.get("repository", ""),
                tag=gh_cfg.get("tag", "state-latest"),
                state_asset_name=gh_cfg.get("state_asset_name", "state.parquet"),
                meta_asset_name=gh_cfg.get("meta_asset_name", "meta.json"),
            )
        )
        backend.validate()
        return backend

    raise ValueError(f"Unknown state backend: {backend_name}")


__all__ = [
    "build_state_backend",
    "GitHubReleaseBackend",
    "GitHubReleaseSettings",
    "LocalPathBackend",
    "LocalPathSettings",
]
