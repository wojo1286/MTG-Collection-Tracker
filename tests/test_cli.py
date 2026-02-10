from __future__ import annotations

from pathlib import Path

from mtg_tracker.cli import main


def test_report_generates_dummy_artifacts(tmp_path: Path) -> None:
    rc = main(["--config", str(tmp_path / "missing.yaml"), "report", "--output-dir", str(tmp_path)])

    assert rc == 0
    assert (tmp_path / "dummy_report.md").exists()
    assert (tmp_path / "dummy_report.json").exists()
