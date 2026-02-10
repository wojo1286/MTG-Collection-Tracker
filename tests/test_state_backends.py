from pathlib import Path

from mtg_tracker.state.backends import GitHubReleaseBackend, LocalPathBackend


def test_local_path_backend_round_trip(tmp_path: Path) -> None:
    backend = LocalPathBackend(tmp_path)
    rows = [{"key": "abc", "price": 1.23}]
    meta = {"run_date": "2026-01-01"}

    backend.save_state(rows, meta)
    loaded_rows, loaded_meta = backend.load_state()

    assert loaded_rows == rows
    assert loaded_meta == meta


def test_github_release_backend_stub_metadata() -> None:
    backend = GitHubReleaseBackend(repo="owner/repo")

    state_rows, meta = backend.load_state()

    assert state_rows == []
    assert meta["status"] == "stub"
    assert meta["repo"] == "owner/repo"
