from __future__ import annotations

from .github_release import GitHubReleaseBackend, GitHubReleaseSettings
from .local_path import LocalPathBackend, LocalPathSettings


def build_state_backend(*, backend: str, settings: dict):
    """
    Build a state backend instance.

    Phase 0: Keep this simple. Phase 3 will flesh out GitHubReleaseBackend.
    """
    if backend == "local_path":
        return LocalPathBackend(LocalPathSettings.from_dict(settings))
    if backend == "github_release":
        return GitHubReleaseBackend(GitHubReleaseSettings.from_dict(settings))

    raise ValueError(f"Unknown state backend: {backend!r}")
