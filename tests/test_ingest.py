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
            "name": "Card A",
            "set_code": "set1",
            "collector_number": 1,
        },
        {
            "scryfall_id": "id-1",
            "finish": "normal",
            "qty": 2,
            "name": "Card A",
            "set_code": "set1",
            "collector_number": 1,
        },
        {
            "scryfall_id": "id-2",
            "finish": "normal",
            "qty": 4,
            "name": "Card B",
            "set_code": "set2",
            "collector_number": 10,
        },
        {
            "scryfall_id": "id-3",
            "finish": "normal",
            "qty": 2,
            "name": "Card C",
            "set_code": "set3",
            "collector_number": 99,
        },
    ]

    assert (tmp_path / "collection.csv").exists()


def test_ingest_supports_tsv_input(tmp_path: Path) -> None:
    output_path = tmp_path / "collection.parquet"

    summary = ingest_manabox_csv(
        input_path=Path("tests/fixtures/manabox_sample.tsv"),
        output_path=output_path,
    )

    assert summary.total_input_rows == 21
    out_df = pd.read_parquet(output_path)
    assert set(out_df.columns) == {
        "scryfall_id",
        "finish",
        "qty",
        "name",
        "set_code",
        "collector_number",
    }


def test_ingest_requires_columns_after_aliasing(tmp_path: Path) -> None:
    missing_foil_csv = tmp_path / "missing_foil.csv"
    missing_foil_csv.write_text(
        "Binder Name,Quantity,Scryfall ID\nMain,1,id-1\n",
        encoding="utf-8",
    )

    with pytest.raises(IngestValidationError, match="Missing required columns: finish"):
        ingest_manabox_csv(input_path=missing_foil_csv, output_path=tmp_path / "out.parquet")
