from pathlib import Path

import pandas as pd
import pytest

pytest.importorskip("streamlit")
from viewer import app


def _write_state_parquet(path: Path) -> None:
    df = pd.DataFrame(
        {
            "date": ["2026-01-01"],
            "scryfall_id": ["abc"],
            "finish": ["nonfoil"],
            "price": [5.5],
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def test_load_price_history_prefers_state_path(tmp_path, monkeypatch):
    state_path = tmp_path / "data" / "state" / "state.parquet"
    seed_state_path = tmp_path / "data" / "seed" / "state.parquet"
    _write_state_parquet(state_path)
    _write_state_parquet(seed_state_path)

    monkeypatch.setattr(app, "STATE_PATH", state_path)
    monkeypatch.setattr(app, "SEED_STATE_PATH", seed_state_path)
    app.load_price_history.clear()

    df, source = app.load_price_history()

    assert source == state_path
    assert len(df) == 1


def test_load_price_history_falls_back_to_seed(tmp_path, monkeypatch):
    seed_state_path = tmp_path / "data" / "seed" / "state.parquet"
    _write_state_parquet(seed_state_path)

    monkeypatch.setattr(app, "STATE_PATH", tmp_path / "missing.parquet")
    monkeypatch.setattr(app, "SEED_STATE_PATH", seed_state_path)
    app.load_price_history.clear()

    df, source = app.load_price_history()

    assert source == seed_state_path
    assert len(df) == 1


def test_parse_report_date_handles_missing_and_valid_path():
    assert app._parse_report_date(None) == "n/a"

    report = Path("data/reports/spikes_2026-02-01_summary.csv")
    assert app._parse_report_date(report) == "2026-02-01"


def test_latest_matching_file_uses_newest_name(tmp_path, monkeypatch):
    report_dir = tmp_path / "data" / "reports"
    report_dir.mkdir(parents=True)
    (report_dir / "spikes_2026-01-01_summary.csv").write_text("", encoding="utf-8")
    (report_dir / "spikes_2026-01-02_summary.csv").write_text("", encoding="utf-8")

    monkeypatch.setattr(app, "REPORT_DIR", report_dir)

    latest = app._latest_matching_file("spikes_*_summary.csv")

    assert latest == report_dir / "spikes_2026-01-02_summary.csv"
