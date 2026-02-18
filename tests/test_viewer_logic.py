import pandas as pd

from mtg_tracker.viewer_logic import (
    choose_comparison_history,
    compute_window_changes,
    latest_price_table,
)


def test_latest_price_table_picks_max_date_per_key() -> None:
    state_df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-03", "2024-01-02", "2024-01-04"]),
            "scryfall_id": ["sid-1", "sid-1", "sid-2", "sid-2"],
            "finish": ["nonfoil", "nonfoil", "foil", "foil"],
            "price": [1.0, 2.5, 3.0, 2.8],
        }
    )

    latest = (
        latest_price_table(state_df).sort_values(["scryfall_id", "finish"]).reset_index(drop=True)
    )

    assert latest.loc[0, "scryfall_id"] == "sid-1"
    assert latest.loc[0, "latest_price"] == 2.5
    assert latest.loc[0, "latest_date"] == pd.Timestamp("2024-01-03")
    assert latest.loc[1, "scryfall_id"] == "sid-2"
    assert latest.loc[1, "latest_price"] == 2.8
    assert latest.loc[1, "latest_date"] == pd.Timestamp("2024-01-04")


def test_compute_window_changes_uses_closest_earlier_date() -> None:
    history_df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-04", "2024-01-08"]),
            "scryfall_id": ["sid-1", "sid-1", "sid-1"],
            "finish": ["nonfoil", "nonfoil", "nonfoil"],
            "price": [10.0, 8.0, 6.0],
        }
    )

    changes = compute_window_changes(history_df, window_days=7)

    assert len(changes) == 1
    assert changes.loc[0, "latest_date"] == pd.Timestamp("2024-01-08")
    assert changes.loc[0, "past_date"] == pd.Timestamp("2024-01-01")
    assert changes.loc[0, "past_price"] == 10.0
    assert changes.loc[0, "pct_change"] == -0.4


def test_choose_comparison_history_prefers_seed_for_long_windows() -> None:
    state_df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-10", "2024-01-14"]),
            "scryfall_id": ["sid-1", "sid-1"],
            "finish": ["nonfoil", "nonfoil"],
            "price": [10.0, 12.0],
        }
    )
    seed_df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2023-10-15", "2024-01-14"]),
            "scryfall_id": ["sid-1", "sid-1"],
            "finish": ["nonfoil", "nonfoil"],
            "price": [8.0, 12.0],
        }
    )

    selected_7d = choose_comparison_history(
        recent_state_df=state_df,
        seed_90d_df=seed_df,
        window_days=7,
        state_days=14,
    )
    selected_90d = choose_comparison_history(
        recent_state_df=state_df,
        seed_90d_df=seed_df,
        window_days=90,
        state_days=14,
    )

    # We prefer the seed dataset whenever available.
    assert selected_7d["date"].min() == pd.Timestamp("2023-10-15")
    assert selected_90d["date"].min() == pd.Timestamp("2023-10-15")

    changes_90d = compute_window_changes(selected_90d, window_days=90)
    assert len(changes_90d) == 1
    assert changes_90d.loc[0, "past_price"] == 8.0
