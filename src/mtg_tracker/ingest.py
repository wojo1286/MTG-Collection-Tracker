"""ManaBox CSV ingest pipeline for Phase 1."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

LOGGER = logging.getLogger(__name__)

REQUIRED_COLUMNS = {"scryfall_id", "qty", "finish"}
OPTIONAL_COLUMNS = ["set_code", "collector_number"]
VALID_FINISHES = {"normal", "foil", "etched"}


class IngestValidationError(ValueError):
    """Raised when input CSV does not match expected shape."""


@dataclass(frozen=True)
class IngestSummary:
    """Operational summary emitted after ingest completes."""

    total_input_rows: int
    invalid_rows_skipped: int
    unique_keys: int
    total_quantity: int


def normalize_finish(raw_finish: object) -> tuple[str, bool]:
    """Normalize raw finish values to one of {normal, foil, etched}.

    Returns a tuple of (normalized_value, used_fallback_default).
    """

    if raw_finish is None or pd.isna(raw_finish):
        return "normal", True

    normalized = str(raw_finish).strip().lower()
    if normalized in VALID_FINISHES:
        return normalized, False

    if normalized == "":
        return "normal", True

    return "normal", True


def ingest_manabox_csv(
    input_path: Path, output_path: Path, debug_csv: bool = False
) -> IngestSummary:
    """Ingest ManaBox CSV and write a normalized collection parquet file."""

    df = pd.read_csv(input_path)
    _validate_required_columns(df)

    total_input_rows = len(df)
    working = df.copy()

    finish_normalized = working["finish"].map(normalize_finish)
    working["finish"] = finish_normalized.map(lambda item: item[0])
    defaulted_finish_count = int(finish_normalized.map(lambda item: item[1]).sum())
    if defaulted_finish_count:
        LOGGER.warning(
            "Defaulted finish to 'normal' for %d rows due to blank/unknown values.",
            defaulted_finish_count,
        )

    working["qty"] = pd.to_numeric(working["qty"], errors="coerce")

    invalid_mask = (
        working["scryfall_id"].isna()
        | (working["scryfall_id"].astype(str).str.strip() == "")
        | working["qty"].isna()
    )
    invalid_rows_skipped = int(invalid_mask.sum())
    cleaned = working.loc[~invalid_mask].copy()

    cleaned["scryfall_id"] = cleaned["scryfall_id"].astype(str).str.strip()
    cleaned["qty"] = cleaned["qty"].astype(int)

    selected_columns = ["scryfall_id", "finish", "qty"]
    for optional in OPTIONAL_COLUMNS:
        if optional in cleaned.columns:
            selected_columns.append(optional)

    grouped = (
        cleaned[selected_columns]
        .groupby(["scryfall_id", "finish"], as_index=False, sort=True)
        .agg(_build_aggregations(selected_columns))
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    grouped.to_parquet(output_path, index=False)

    if debug_csv:
        debug_csv_path = output_path.with_suffix(".csv")
        grouped.to_csv(debug_csv_path, index=False)

    summary = IngestSummary(
        total_input_rows=total_input_rows,
        invalid_rows_skipped=invalid_rows_skipped,
        unique_keys=len(grouped),
        total_quantity=int(grouped["qty"].sum()) if not grouped.empty else 0,
    )

    LOGGER.info("Total input rows: %d", summary.total_input_rows)
    LOGGER.info("Invalid rows skipped: %d", summary.invalid_rows_skipped)
    LOGGER.info("Unique (scryfall_id, finish) keys: %d", summary.unique_keys)
    LOGGER.info("Total quantity: %d", summary.total_quantity)

    return summary


def _build_aggregations(selected_columns: list[str]) -> dict[str, str]:
    aggregations: dict[str, str] = {"qty": "sum"}
    for optional in OPTIONAL_COLUMNS:
        if optional in selected_columns:
            aggregations[optional] = "first"
    return aggregations


def _validate_required_columns(df: pd.DataFrame) -> None:
    missing_columns = REQUIRED_COLUMNS - set(df.columns)
    if missing_columns:
        missing_csv = ", ".join(sorted(missing_columns))
        raise IngestValidationError(f"Missing required columns: {missing_csv}")
