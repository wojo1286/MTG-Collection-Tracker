"""Read-only Streamlit viewer for spikes and collection exploration."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from mtg_tracker.config import load_config
from mtg_tracker.viewer_logic import (
    attach_latest_prices,
    choose_comparison_history,
    compute_highest_value_cards,
    compute_movers_for_collection,
    latest_price_table,
    load_collection,
    load_price_history,
)

DEFAULT_COLLECTION_PATH = Path("data/out/collection.parquet")
DEFAULT_RECENT_STATE_PATH = Path("data/state/state.parquet")
DEFAULT_HISTORY_90D_PATH = Path("data/seed/seed_90d.parquet")
DEFAULT_STATE_DAYS = 14


@st.cache_data(show_spinner=False)
def cached_collection(path: str) -> pd.DataFrame:
    return load_collection(path)


@st.cache_data(show_spinner=False)
def cached_history(path: str) -> pd.DataFrame:
    return load_price_history(path)


def _path_from_config(raw: dict[str, Any], keys: list[str], default: Path) -> Path:
    value: Any = raw
    for key in keys:
        if not isinstance(value, dict):
            return default
        value = value.get(key)
    if isinstance(value, str) and value.strip():
        return Path(value)
    return default


def _history_for_key(history_df: pd.DataFrame, scryfall_id: str, finish: str) -> pd.DataFrame:
    history = history_df[
        (history_df["scryfall_id"] == scryfall_id) & (history_df["finish"] == finish)
    ].copy()
    return history.sort_values("date")


def _render_details(
    selection: str,
    option_map: dict[str, tuple[str, str]],
    rows: pd.DataFrame,
    history_df: pd.DataFrame,
    window_days: int | None = None,
) -> None:
    scryfall_id, finish = option_map[selection]
    selected_rows = rows[(rows["scryfall_id"] == scryfall_id) & (rows["finish"] == finish)]
    if selected_rows.empty:
        st.info("No details available for current selection.")
        return

    first = selected_rows.iloc[0]
    history = _history_for_key(history_df, scryfall_id, finish)

    st.subheader(f"{first.get('name', scryfall_id)} ({finish})")
    if not history.empty:
        show_zoom = window_days is not None and st.checkbox(
            f"Zoom chart to last {window_days} days",
            value=False,
            key=f"zoom_{scryfall_id}_{finish}",
        )
        chart_df = history
        if show_zoom and window_days is not None:
            cutoff = history["date"].max() - pd.to_timedelta(window_days, unit="D")
            chart_df = history[history["date"] >= cutoff]
        st.line_chart(chart_df.set_index("date")["price"])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Latest Price", f"{first.get('latest_price', float('nan')):.2f}")
    c2.metric("Qty", f"{first.get('qty', float('nan')):.0f}")
    c3.metric("Total Value", f"{first.get('total_value', float('nan')):.2f}")
    latest_date = first.get("latest_date")
    c4.metric("Latest Date", str(latest_date.date()) if pd.notna(latest_date) else "-")

    if "past_price" in first:
        c5, c6, c7 = st.columns(3)
        c5.metric("Past Price", f"{first.get('past_price', float('nan')):.2f}")
        c6.metric("Abs Change", f"{first.get('abs_change', float('nan')):.2f}")
        c7.metric("Pct Change", f"{first.get('pct_change', float('nan')):.2%}")

    st.markdown(f"[View on Scryfall](https://scryfall.com/card/{scryfall_id})")


def _selection_for_rows(rows: pd.DataFrame, key: str) -> tuple[str, dict[str, tuple[str, str]]]:
    labels = (
        rows["name"].astype(str)
        + " | "
        + rows["set_code"].astype(str)
        + " | "
        + rows["collector_number"].astype(str)
        + " | "
        + rows["finish"].astype(str)
    )
    option_map = dict(
        zip(labels, zip(rows["scryfall_id"], rows["finish"], strict=False), strict=False)
    )
    selected = st.selectbox("Select card", options=list(option_map.keys()), key=key)
    return selected, option_map


def _render_search_tab(rows: pd.DataFrame, chart_history_df: pd.DataFrame) -> None:
    query = st.text_input("Search by name", "")
    finishes = sorted(rows["finish"].dropna().astype(str).unique())
    selected_finishes = st.multiselect("Finish", finishes, default=finishes)
    min_price = st.number_input("Min latest price", min_value=0.0, value=0.0, step=0.25)

    filtered = rows.copy()
    if query:
        filtered = filtered[filtered["name"].str.contains(query, case=False, na=False)]
    if selected_finishes:
        filtered = filtered[filtered["finish"].astype(str).isin(selected_finishes)]
    filtered = filtered[(filtered["latest_price"].isna()) | (filtered["latest_price"] >= min_price)]

    display_cols = [
        "name",
        "finish",
        "qty",
        "set_code",
        "collector_number",
        "latest_price",
        "total_value",
        "scryfall_id",
    ]
    st.dataframe(filtered[display_cols], use_container_width=True)

    selectable = filtered.dropna(subset=["latest_price"])
    if selectable.empty:
        st.info("No priced cards available for selection.")
        return
    selection, option_map = _selection_for_rows(selectable, key="search_select")
    _render_details(selection, option_map, selectable, chart_history_df)


def _render_highest_value_tab(rows: pd.DataFrame, chart_history_df: pd.DataFrame) -> None:
    top_n = st.slider("Top N", min_value=10, max_value=200, value=50, step=5)
    query = st.text_input("Name search", "", key="value_name_search")
    filtered = rows.dropna(subset=["latest_price"]).copy()
    if query:
        filtered = filtered[filtered["name"].str.contains(query, case=False, na=False)]
    ranked = filtered.sort_values("total_value", ascending=False).head(top_n)

    display_cols = [
        "name",
        "finish",
        "qty",
        "set_code",
        "collector_number",
        "latest_date",
        "latest_price",
        "total_value",
        "scryfall_id",
    ]
    st.dataframe(ranked[display_cols], use_container_width=True)
    if ranked.empty:
        st.info("No priced cards found.")
        return
    selection, option_map = _selection_for_rows(ranked, key="top_value_select")
    _render_details(selection, option_map, ranked, chart_history_df)


def _render_movers_tab(
    collection_df: pd.DataFrame,
    recent_state_df: pd.DataFrame,
    history_90d_df: pd.DataFrame,
    state_days: int,
) -> None:
    mode = st.radio("Mode", ["Decliners", "Risers"], horizontal=True)
    window_days = st.selectbox("Window days", [7, 14, 30, 60, 90], index=0)
    top_n = st.slider("Top N movers", min_value=10, max_value=200, value=50, step=5)
    name_query = st.text_input("Name search", "", key="mover_name_search")
    min_latest_price = st.number_input(
        "Min latest price", min_value=0.0, value=0.5, step=0.25, key="mover_min_price"
    )
    min_qty = st.number_input("Min qty", min_value=0, value=1, step=1, key="mover_min_qty")

    comparison_history_df = choose_comparison_history(
        recent_state_df=recent_state_df,
        seed_90d_df=history_90d_df,
        window_days=window_days,
        state_days=state_days,
    )
    movers = compute_movers_for_collection(
        collection_df=collection_df,
        history_df=comparison_history_df,
        window_days=window_days,
    )

    finishes = sorted(movers["finish"].dropna().astype(str).unique()) if not movers.empty else []
    selected_finishes = st.multiselect("Finish", finishes, default=finishes, key="mover_finish")

    filtered = movers.copy()
    if name_query:
        filtered = filtered[filtered["name"].str.contains(name_query, case=False, na=False)]
    filtered = filtered[filtered["latest_price"] >= min_latest_price]
    filtered = filtered[filtered["qty"].fillna(0) >= min_qty]
    if selected_finishes:
        filtered = filtered[filtered["finish"].astype(str).isin(selected_finishes)]

    ascending = mode == "Decliners"
    filtered = filtered.sort_values("pct_change", ascending=ascending).head(top_n)

    display_cols = [
        "name",
        "finish",
        "qty",
        "set_code",
        "collector_number",
        "latest_date",
        "latest_price",
        "past_date",
        "past_price",
        "abs_change",
        "pct_change",
        "total_value",
        "scryfall_id",
    ]
    st.dataframe(filtered[display_cols], use_container_width=True)
    if filtered.empty:
        st.info("No movers match current filters.")
        return
    selection, option_map = _selection_for_rows(filtered, key="mover_select")
    _render_details(selection, option_map, filtered, history_90d_df, window_days=window_days)


def main() -> None:
    st.set_page_config(page_title="MTG Collection Explorer", layout="wide")
    st.title("MTG Collection Viewer")

    config = load_config()
    collection_path = _path_from_config(
        config.raw,
        ["viewer", "collection_path"],
        default=DEFAULT_COLLECTION_PATH,
    )
    recent_state_path = _path_from_config(
        config.raw,
        ["viewer", "state_path"],
        default=DEFAULT_RECENT_STATE_PATH,
    )
    history_90d_path = _path_from_config(
        config.raw,
        ["viewer", "seed_90d_path"],
        default=DEFAULT_HISTORY_90D_PATH,
    )
    state_days = int(config.raw.get("daily", {}).get("state_days", DEFAULT_STATE_DAYS))

    if not collection_path.exists():
        st.error(f"Collection parquet not found: {collection_path}")
        return
    if not recent_state_path.exists():
        st.error(f"Recent state parquet not found: {recent_state_path}")
        return
    if not history_90d_path.exists():
        st.error(f"90-day history parquet not found: {history_90d_path}")
        return

    collection_df = cached_collection(str(collection_path))
    recent_state_df = cached_history(str(recent_state_path))
    history_90d_df = cached_history(str(history_90d_path))

    latest_df = latest_price_table(history_90d_df)
    collection_with_prices = attach_latest_prices(collection_df, latest_df)
    highest_value_df = compute_highest_value_cards(collection_df, history_90d_df)

    spikes_tab, explorer_tab = st.tabs(["Spikes", "Collection Explorer"])

    with spikes_tab:
        st.info(
            "Spikes tab remains unchanged; use existing daily report artifacts for spike review."
        )

    with explorer_tab:
        mode = st.radio(
            "View",
            ["Search", "Highest Value Cards", "Risers / Decliners"],
            horizontal=True,
        )
        if mode == "Search":
            _render_search_tab(collection_with_prices, history_90d_df)
        elif mode == "Highest Value Cards":
            _render_highest_value_tab(highest_value_df, history_90d_df)
        else:
            _render_movers_tab(
                collection_df, recent_state_df, history_90d_df, state_days=state_days
            )


if __name__ == "__main__":
    main()
