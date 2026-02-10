from __future__ import annotations

from pathlib import Path

import pytest

from mtg_tracker.state.github_release import GitHubReleaseBackend, GitHubReleaseSettings


def test_github_backend_requires_repository() -> None:
    backend = GitHubReleaseBackend(GitHubReleaseSettings(repository=""))

    with pytest.raises(ValueError):
        backend.validate()


def test_github_backend_cache_paths() -> None:
    backend = GitHubReleaseBackend(
        GitHubReleaseSettings(
            repository="owner/repo",
            state_asset_name="state.parquet",
            meta_asset_name="meta.json",
        )
    )

    state_path, meta_path = backend.local_cache_paths(Path("cache"))

    assert state_path.as_posix() == "cache/state.parquet"
    assert meta_path.as_posix() == "cache/meta.json"
