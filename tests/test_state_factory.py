from __future__ import annotations

from mtg_tracker.config import Config
from mtg_tracker.state import build_state_backend
from mtg_tracker.state.local_path import LocalPathBackend


def test_build_state_backend_from_config_object_returns_local_backend() -> None:
    config = Config(raw={"state": {"backend": "local_path", "local_path": {}}})

    backend = build_state_backend(config)

    assert isinstance(backend, LocalPathBackend)
