"""Helper logic for the Streamlit collection explorer."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

KEY_COLUMNS = ["scryfall_id", "finish"]


def resolve_state_path(
    preferred_path: Path | str = Path("data/state/state.parquet"),
    fallback_path: Path | str = Path("data/seed/state.parquet"),
) -> Path:
    """Return preferred state path if present, otherwise fallback path."""
    preferred = Path(preferred_path)
    fallback = Path(fallback_path)
    return preferred if preferred.exists() else fallback


def load_collection(collection_path: Path | str) -> pd.DataFrame:
    """Load collection parquet and normalize common dtypes."""
    df = pd.read_parquet(collection_path)
    if "qty" in df.columns:
        df["qty"] = pd.to_numeric(df["qty"], errors="coerce")
    return df


def load_price_history(price_path: Path | str) -> pd.DataFrame:
    """Load a price-history parquet and normalize date/price columns."""
    df = pd.read_parquet(price_path)
    return _normalize_price_history(df)


def _normalize_price_history(df: pd.DataFrame) -> pd.DataFrame:
    required = {"date", "scryfall_id", "finish", "price"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Price history missing required columns: {sorted(missing)}")

    out = df.copy()
    out["date"] = pd.to_datetime(out["date"])
    out["price"] = pd.to_numeric(out["price"], errors="coerce")
    return out.dropna(subset=["date", "price"])


def latest_price_table(history_df: pd.DataFrame) -> pd.DataFrame:
    """Build latest price table keyed by (scryfall_id, finish)."""
    history = _normalize_price_history(history_df)
    latest_idx = history.groupby(KEY_COLUMNS)["date"].idxmax()
    return (
        history.loc[latest_idx, KEY_COLUMNS + ["date", "price"]]
        .rename(columns={"date": "latest_date", "price": "latest_price"})
        .reset_index(drop=True)
    )


def attach_latest_prices(collection_df: pd.DataFrame, latest_df: pd.DataFrame) -> pd.DataFrame:
    """Join latest price metadata onto collection rows and compute total value."""
    merged = collection_df.merge(latest_df, on=KEY_COLUMNS, how="left")
    merged["qty"] = pd.to_numeric(merged.get("qty"), errors="coerce")
    merged["latest_price"] = pd.to_numeric(merged.get("latest_price"), errors="coerce")
    merged["total_value"] = merged["qty"] * merged["latest_price"]
    return merged


def choose_comparison_history(
    recent_state_df: pd.DataFrame,
    seed_90d_df: pd.DataFrame | None,
    window_days: int,
    state_days: int,
) -> pd.DataFrame:
    """Choose the dataset for window comparisons.

    If seed history exists, it is preferred (and required for windows above state_days).
    """
    if seed_90d_df is not None and not seed_90d_df.empty:
        return _normalize_price_history(seed_90d_df)
    if window_days > state_days:
        raise ValueError("Selected window requires 90-day history, but seed history is unavailable")
    return _normalize_price_history(recent_state_df)


def compute_window_changes(history_df: pd.DataFrame, window_days: int) -> pd.DataFrame:
    """Compute latest/past change metrics by key for a lookback window."""
    if window_days <= 0:
        raise ValueError("window_days must be positive")

    history = _normalize_price_history(history_df)
    latest_df = latest_price_table(history)
    latest_df["target_date"] = latest_df["latest_date"] - pd.to_timedelta(window_days, unit="D")

    candidates = history.merge(
        latest_df[KEY_COLUMNS + ["latest_date", "target_date"]],
        on=KEY_COLUMNS,
        how="inner",
    )
    eligible = candidates[candidates["date"] <= candidates["target_date"]].copy()
    if eligible.empty:
        return pd.DataFrame(
            columns=KEY_COLUMNS
            + ["latest_date", "latest_price", "past_date", "past_price", "abs_change", "pct_change"]
        )

    idx = eligible.groupby(KEY_COLUMNS)["date"].idxmax()
    past = (
        eligible.loc[idx, KEY_COLUMNS + ["date", "price"]]
        .rename(columns={"date": "past_date", "price": "past_price"})
        .reset_index(drop=True)
    )

    changes = latest_df.merge(past, on=KEY_COLUMNS, how="inner")
    changes = changes[changes["past_price"] > 0].copy()
    changes["abs_change"] = changes["latest_price"] - changes["past_price"]
    changes["pct_change"] = changes["abs_change"] / changes["past_price"]
    return changes.drop(columns=["target_date"])


def compute_movers_for_collection(
    collection_df: pd.DataFrame,
    history_df: pd.DataFrame,
    window_days: int,
) -> pd.DataFrame:
    """Attach windowed rise/decline metrics to collection rows."""
    changes = compute_window_changes(history_df, window_days=window_days)
    enriched = attach_latest_prices(collection_df, changes)
    return enriched.dropna(subset=["latest_price", "past_price", "pct_change"])


def compute_highest_value_cards(
    collection_df: pd.DataFrame, history_df: pd.DataFrame
) -> pd.DataFrame:
    """Attach latest prices (from history dataset) for value ranking."""
    latest_df = latest_price_table(history_df)
    return attach_latest_prices(collection_df, latest_df)
