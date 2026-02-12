"""Simple Streamlit viewer for local MTG tracker outputs."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

STATE_PATH = Path("data/state/state.parquet")
SEED_STATE_PATH = Path("data/seed/state.parquet")
SEED_90D_PATH = Path("data/seed/seed_90d.parquet")
REPORT_DIR = Path("data/reports")


def _latest_matching_file(pattern: str) -> Path | None:
    files = sorted(REPORT_DIR.glob(pattern))
    return files[-1] if files else None


@st.cache_data(show_spinner=False)
def load_price_history() -> tuple[pd.DataFrame, Path | None]:
    """Load state parquet with seed fallback for viewer charts."""

    for path in (STATE_PATH, SEED_STATE_PATH):
        if path.exists():
            columns = ["date", "scryfall_id", "finish", "price"]
            return pd.read_parquet(path, columns=columns), path
    return pd.DataFrame(columns=["date", "scryfall_id", "finish", "price"]), None


@st.cache_data(show_spinner=False)
def load_seed_history() -> pd.DataFrame:
    """Load optional 90d seed history for longer chart ranges."""

    if not SEED_90D_PATH.exists():
        return pd.DataFrame(columns=["date", "scryfall_id", "finish", "price"])
    columns = ["date", "scryfall_id", "finish", "price"]
    return pd.read_parquet(SEED_90D_PATH, columns=columns)


@st.cache_data(show_spinner=False)
def load_latest_spike_summary() -> tuple[pd.DataFrame, Path | None]:
    """Load most recent summary report if present."""

    summary_path = _latest_matching_file("spikes_*_summary.csv")
    if summary_path is None:
        return pd.DataFrame(), None
    return pd.read_csv(summary_path), summary_path


@st.cache_data(show_spinner=False)
def load_raw_spike_report(path: str) -> pd.DataFrame:
    """Load a raw spikes csv selected in UI."""

    return pd.read_csv(path)


def _coerce_history_types(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["scryfall_id"] = out["scryfall_id"].astype(str)
    out["finish"] = out["finish"].astype(str)
    out["price"] = pd.to_numeric(out["price"], errors="coerce")
    return out.dropna(subset=["date", "scryfall_id", "finish", "price"])


def _parse_report_date(path: Path | None) -> str:
    if path is None:
        return "n/a"

    token = path.stem.replace("_summary", "")
    parts = token.split("_")
    return parts[1] if len(parts) > 1 else "n/a"


def _filter_spikes(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    filtered = df.copy()
    st.sidebar.header("Filters")

    if "finish" in filtered.columns:
        finish_values = sorted(value for value in filtered["finish"].dropna().astype(str).unique())
        selected_finish = st.sidebar.multiselect("finish", finish_values, default=finish_values)
        if selected_finish:
            filtered = filtered[filtered["finish"].astype(str).isin(selected_finish)]

    if "pct_change" in filtered.columns:
        min_pct = float(
            st.sidebar.number_input("minimum pct change", min_value=0.0, value=0.0, step=0.05)
        )
        filtered = filtered[pd.to_numeric(filtered["pct_change"], errors="coerce") >= min_pct]

    if "today_price" in filtered.columns:
        min_price = float(
            st.sidebar.number_input("minimum today price", min_value=0.0, value=0.0, step=0.5)
        )
        filtered = filtered[pd.to_numeric(filtered["today_price"], errors="coerce") >= min_price]

    if "window_days" in filtered.columns:
        window_values = sorted(
            int(value)
            for value in pd.to_numeric(filtered["window_days"], errors="coerce").dropna().unique()
        )
        selected_windows = st.sidebar.multiselect(
            "window_days", window_values, default=window_values
        )
        if selected_windows:
            filtered = filtered[
                pd.to_numeric(filtered["window_days"], errors="coerce")
                .astype("Int64")
                .isin(selected_windows)
            ]

    return filtered.reset_index(drop=True)


def _friendly_missing_data_message() -> None:
    st.info(
        "No local viewer data found yet. Run seed/daily commands first, "
        "then open this viewer again. "
        "Expected paths: data/state/state.parquet (or data/seed/state.parquet) and optional "
        "data/reports/spikes_*_summary.csv."
    )


def main() -> None:
    st.set_page_config(page_title="MTG Tracker Viewer", layout="wide")
    st.title("MTG Collection Tracker Viewer")

    state_df, state_path = load_price_history()
    seed_90d_df = load_seed_history()
    summary_df, summary_path = load_latest_spike_summary()

    state_df = _coerce_history_types(state_df)
    seed_90d_df = _coerce_history_types(seed_90d_df)

    if state_df.empty and seed_90d_df.empty and summary_df.empty:
        _friendly_missing_data_message()
        return

    st.subheader("Run Stats")
    col1, col2, col3 = st.columns(3)
    col1.metric("State rows", f"{len(state_df):,}")
    col2.metric("State source", str(state_path) if state_path else "n/a")
    col3.metric("Last report date", _parse_report_date(summary_path))

    st.subheader("Spikes")
    raw_spike_path = None
    if summary_df.empty:
        st.warning("No spikes summary found at data/reports/spikes_*_summary.csv")
        raw_candidates = sorted(REPORT_DIR.glob("spikes_*.csv"))
        if raw_candidates:
            selected = st.selectbox(
                "Load raw spikes report",
                options=["None", *[path.name for path in raw_candidates]],
                index=0,
            )
            if selected != "None":
                raw_spike_path = REPORT_DIR / selected
                summary_df = load_raw_spike_report(str(raw_spike_path))

    filtered_spikes = _filter_spikes(summary_df)

    if filtered_spikes.empty:
        st.info("No spikes match current filters.")
        return

    display_columns = [
        column
        for column in [
            "name",
            "scryfall_id",
            "finish",
            "today_price",
            "pct_change",
            "abs_change",
            "best_window_days",
            "window_days",
        ]
        if column in filtered_spikes.columns
    ]
    st.dataframe(filtered_spikes[display_columns], hide_index=True, use_container_width=True)

    option_labels = [
        f"{row.get('name', 'unknown')} | {row['scryfall_id']} | {row['finish']}"
        for _, row in filtered_spikes.iterrows()
    ]
    selected_label = st.selectbox("Select spike row", option_labels, index=0)
    selected_index = option_labels.index(selected_label)
    selected_row = filtered_spikes.iloc[selected_index]

    scryfall_id = str(selected_row["scryfall_id"])
    finish = str(selected_row["finish"])

    chart_history = pd.concat([seed_90d_df, state_df], ignore_index=True)
    chart_history = chart_history.drop_duplicates(
        subset=["date", "scryfall_id", "finish", "price"], keep="last"
    )
    chart_history = chart_history[
        (chart_history["scryfall_id"] == scryfall_id) & (chart_history["finish"] == finish)
    ].sort_values("date")

    if chart_history.empty:
        st.info("No price history found for selected spike in state/seed parquet files.")
        return

    st.subheader("Price History")
    st.line_chart(chart_history.set_index("date")["price"], use_container_width=True)

    source_label = str(summary_path) if summary_path else str(raw_spike_path)
    st.caption(f"Report source: {source_label}")


if __name__ == "__main__":
    main()
