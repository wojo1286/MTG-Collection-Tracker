from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from mtg_tracker.ingest import IngestValidationError, ingest_manabox_csv, normalize_finish


def test_normalize_finish_values() -> None:
    assert normalize_finish("normal") == ("normal", False)
    assert normalize_finish("FOIL") == ("foil", False)
    assert normalize_finish(" etched ") == ("etched", False)
    assert normalize_finish("") == ("normal", True)
    assert normalize_finish("mystery") == ("normal", True)
    assert normalize_finish(None) == ("normal", True)


def test_ingest_aggregates_duplicates_and_reports_summary(tmp_path: Path) -> None:
    input_path = Path("tests/fixtures/manabox_sample.csv")
    output_path = tmp_path / "collection.parquet"

    summary = ingest_manabox_csv(input_path=input_path, output_path=output_path, debug_csv=True)

    assert summary.total_input_rows == 7
    assert summary.invalid_rows_skipped == 2
    assert summary.unique_keys == 4
    assert summary.total_quantity == 12

    out_df = pd.read_parquet(output_path).sort_values(["scryfall_id", "finish"])
    records = out_df.to_dict(orient="records")
    assert records == [
        {
            "scryfall_id": "id-1",
            "finish": "foil",
            "qty": 4,
            "set_code": "set1",
            "collector_number": 1,
        },
        {
            "scryfall_id": "id-1",
            "finish": "normal",
            "qty": 2,
            "set_code": "set1",
            "collector_number": 1,
        },
        {
            "scryfall_id": "id-2",
            "finish": "normal",
            "qty": 4,
            "set_code": "set2",
            "collector_number": 10,
        },
        {
            "scryfall_id": "id-3",
            "finish": "normal",
            "qty": 2,
            "set_code": "set3",
            "collector_number": 99,
        },
    ]

    assert (tmp_path / "collection.csv").exists()


def test_ingest_requires_columns(tmp_path: Path) -> None:
    missing_finish_csv = tmp_path / "missing_finish.csv"
    missing_finish_csv.write_text("scryfall_id,qty\nid-1,1\n", encoding="utf-8")

    with pytest.raises(IngestValidationError, match="Missing required columns: finish"):
        ingest_manabox_csv(input_path=missing_finish_csv, output_path=tmp_path / "out.parquet")
