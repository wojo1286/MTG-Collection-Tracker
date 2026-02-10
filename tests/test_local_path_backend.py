from __future__ import annotations

from pathlib import Path

from mtg_tracker.state.local_path import LocalPathBackend


def test_local_backend_missing_files_returns_empty(tmp_path: Path) -> None:
    backend = LocalPathBackend(tmp_path / "state.parquet", tmp_path / "meta.json")

    state_rows, meta = backend.load_state()

    assert state_rows == []
    assert meta == {}


def test_local_backend_round_trip(tmp_path: Path) -> None:
    backend = LocalPathBackend(tmp_path / "state.parquet", tmp_path / "meta.json")
    expected_rows = [{"scryfall_id": "abc", "finish": "normal", "price": 1.25}]
    expected_meta = {"date": "2026-01-01", "rows": 1}

    backend.save_state(expected_rows, expected_meta)
    state_rows, meta = backend.load_state()

    assert state_rows == expected_rows
    assert meta == expected_meta
