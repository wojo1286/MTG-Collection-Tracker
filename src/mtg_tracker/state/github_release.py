from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class GitHubReleaseSettings:
    repo: str = ""
    tag: str = "state-latest"
    state_asset_name: str = "state.parquet"
    meta_asset_name: str = "meta.json"
    token_env: str = "GITHUB_TOKEN"

    @staticmethod
    def from_dict(d: dict[str, Any]) -> "GitHubReleaseSettings":
        return GitHubReleaseSettings(
            repo=str(d.get("repo", "")),
            tag=str(d.get("tag", "state-latest")),
            state_asset_name=str(d.get("state_asset_name", "state.parquet")),
            meta_asset_name=str(d.get("meta_asset_name", "meta.json")),
            token_env=str(d.get("token_env", "GITHUB_TOKEN")),
        )


class GitHubReleaseBackend:
    """
    Phase 0: stub. Phase 3 will implement real download/upload of release assets.
    """

    def __init__(self, settings: GitHubReleaseSettings):
        self.settings = settings

    def load_state(self):
        raise NotImplementedError("GitHubReleaseBackend is implemented in Phase 3")

    def save_state(self, *args, **kwargs):
        raise NotImplementedError("GitHubReleaseBackend is implemented in Phase 3")
