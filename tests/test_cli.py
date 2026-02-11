from __future__ import annotations

from pathlib import Path

import pandas as pd

from mtg_tracker.cli import main


def test_report_generates_dummy_artifacts(tmp_path: Path) -> None:
    rc = main(["--config", str(tmp_path / "missing.yaml"), "report", "--output-dir", str(tmp_path)])

    assert rc == 0
    assert (tmp_path / "dummy_report.md").exists()
    assert (tmp_path / "dummy_report.json").exists()


def test_ingest_command_generates_collection_outputs(tmp_path: Path) -> None:
    output_path = tmp_path / "collection.parquet"

    rc = main(
        [
            "--config",
            str(tmp_path / "missing.yaml"),
            "ingest",
            "--input",
            "tests/fixtures/manabox_sample.tsv",
            "--out",
            str(output_path),
            "--debug-csv",
        ]
    )

    assert rc == 0
    assert output_path.exists()
    assert (tmp_path / "collection.csv").exists()
    out_df = pd.read_parquet(output_path)
    assert set(out_df.columns) == {
        "scryfall_id",
        "finish",
        "qty",
        "set_code",
        "collector_number",
    }


def test_seed_help_includes_phase_2_arguments(capsys) -> None:
    try:
        main(["seed", "--help"])
    except SystemExit as exc:
        assert exc.code == 0

    captured = capsys.readouterr()
    assert "--collection" in captured.out
    assert "--allprices" in captured.out
    assert "--identifiers" in captured.out
    assert "--out-dir" in captured.out


def test_seed_command_with_tiny_fixtures(tmp_path: Path, monkeypatch) -> None:
    collection_path = tmp_path / "collection.parquet"
    pd.DataFrame(
        [
            {
                "scryfall_id": "sid-1",
                "finish": "normal",
                "qty": 2,
                "set_code": "set1",
                "collector_number": "1",
            },
            {
                "scryfall_id": "sid-2",
                "finish": "foil",
                "qty": 1,
                "set_code": "set2",
                "collector_number": "2",
            },
        ]
    ).to_parquet(collection_path, index=False)

    class _FakeDate:
        @classmethod
        def today(cls):
            return pd.Timestamp("2024-12-05").date()

    monkeypatch.setattr("mtg_tracker.seed.date", _FakeDate)

    out_dir = tmp_path / "seed"
    rc = main(
        [
            "seed",
            "--collection",
            str(collection_path),
            "--allprices",
            "tests/fixtures/allprices_tiny.json",
            "--identifiers",
            "tests/fixtures/allidentifiers_tiny.json",
            "--out-dir",
            str(out_dir),
            "--state-days",
            "1",
        ]
    )

    assert rc == 0
    assert (out_dir / "seed_90d.parquet").exists()
    assert (out_dir / "state.parquet").exists()
    assert (out_dir / "meta.json").exists()
