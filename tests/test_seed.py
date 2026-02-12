from __future__ import annotations

import json
import lzma
from pathlib import Path

import pandas as pd

from mtg_tracker.seed import (
    SEED_COLUMNS,
    build_scryfall_to_uuid_map,
    build_state_window,
    extract_seed_prices,
    run_seed,
    validate_uuid_mapping_against_allprices,
)


def _write_identifiers(path: Path) -> None:
    payload = {
        "data": {
            "uuid-1": {"identifiers": {"scryfallId": "sid-1"}},
            "uuid-2": {"identifiers": {"scryfallId": "sid-2"}},
            "uuid-3": {"identifiers": {"scryfallId": "sid-unused"}},
        }
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_allprices(path: Path) -> None:
    payload = {
        "data": {
            "uuid-1": {
                "paper": {
                    "tcgplayer": {
                        "market": {
                            "2024-12-01": 2.0,
                            "2024-12-02": 3.0,
                            "2024-12-04": 0,
                            "2024-12-bad": 10,
                        }
                    }
                }
            },
            "uuid-2": {
                "paper": {
                    "tcgplayer": {
                        "market": {
                            "2024-12-02": 5.0,
                            "2024-12-03": None,
                        }
                    }
                }
            },
            "uuid-ignored": {
                "paper": {
                    "tcgplayer": {
                        "market": {
                            "2024-12-02": 100,
                        }
                    }
                }
            },
        }
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_build_scryfall_to_uuid_map_counts_unmapped(tmp_path: Path) -> None:
    identifiers_path = tmp_path / "AllIdentifiers.json"
    _write_identifiers(identifiers_path)

    mapping = build_scryfall_to_uuid_map(identifiers_path, {"sid-1", "sid-2", "sid-missing"})

    assert mapping == {"sid-1": "uuid-1", "sid-2": "uuid-2"}


def test_build_scryfall_to_uuid_map_uses_top_level_uuid_key(tmp_path: Path) -> None:
    identifiers_path = tmp_path / "AllIdentifiers.json"
    payload = {
        "data": {
            "mtgjson-uuid-1": {
                "uuid": "scryfall-uuid-1",
                "identifiers": {"scryfallId": "scryfall-uuid-1"},
            }
        }
    }
    identifiers_path.write_text(json.dumps(payload), encoding="utf-8")

    mapping = build_scryfall_to_uuid_map(identifiers_path, {"scryfall-uuid-1"})

    assert mapping == {"scryfall-uuid-1": "mtgjson-uuid-1"}


def test_extract_seed_prices_filters_and_no_forward_fill(tmp_path: Path, monkeypatch) -> None:
    allprices_path = tmp_path / "AllPrices.json"
    _write_allprices(allprices_path)

    mapped_keys_df = pd.DataFrame(
        [
            {"scryfall_id": "sid-1", "finish": "normal", "mtgjson_uuid": "uuid-1"},
            {"scryfall_id": "sid-2", "finish": "foil", "mtgjson_uuid": "uuid-2"},
        ]
    )

    class _FakeDate:
        @classmethod
        def today(cls):
            return pd.Timestamp("2024-12-05").date()

    monkeypatch.setattr("mtg_tracker.seed.date", _FakeDate)

    seed_df = extract_seed_prices(
        allprices_path=allprices_path,
        mapped_keys_df=mapped_keys_df,
        provider="tcgplayer",
        price_type="market",
        market="paper",
        days=90,
    )

    assert list(seed_df.columns) == SEED_COLUMNS
    assert set(seed_df["scryfall_id"]) == {"sid-1", "sid-2"}
    assert "uuid-ignored" not in set(seed_df["mtgjson_uuid"])

    sid_1_days = seed_df.loc[seed_df["scryfall_id"] == "sid-1", "date"].tolist()
    assert sid_1_days == ["2024-12-01", "2024-12-02"]

    sid_2_days = seed_df.loc[seed_df["scryfall_id"] == "sid-2", "date"].tolist()
    assert sid_2_days == ["2024-12-02"]


def test_build_state_window_keeps_last_n_dates_per_key() -> None:
    seed_df = pd.DataFrame(
        [
            {
                "date": "2024-01-01",
                "scryfall_id": "sid-1",
                "finish": "normal",
                "mtgjson_uuid": "u1",
                "price": 1.0,
            },
            {
                "date": "2024-01-02",
                "scryfall_id": "sid-1",
                "finish": "normal",
                "mtgjson_uuid": "u1",
                "price": 1.2,
            },
            {
                "date": "2024-01-03",
                "scryfall_id": "sid-1",
                "finish": "normal",
                "mtgjson_uuid": "u1",
                "price": 1.4,
            },
            {
                "date": "2024-01-01",
                "scryfall_id": "sid-2",
                "finish": "foil",
                "mtgjson_uuid": "u2",
                "price": 2.0,
            },
        ]
    )

    state_df = build_state_window(seed_df, state_days=2)

    assert list(state_df.columns) == SEED_COLUMNS
    sid_1_dates = state_df.loc[state_df["scryfall_id"] == "sid-1", "date"].tolist()
    assert sid_1_dates == ["2024-01-02", "2024-01-03"]


def test_run_seed_writes_outputs_and_schema_with_xz_inputs(tmp_path: Path, monkeypatch) -> None:
    collection_path = tmp_path / "collection.parquet"
    collection_df = pd.DataFrame(
        [
            {
                "scryfall_id": "sid-1",
                "finish": "normal",
                "qty": 2,
                "set_code": "set",
                "collector_number": "1",
            },
            {
                "scryfall_id": "sid-2",
                "finish": "foil",
                "qty": 1,
                "set_code": "set",
                "collector_number": "2",
            },
            {
                "scryfall_id": "sid-missing",
                "finish": "etched",
                "qty": 1,
                "set_code": "set",
                "collector_number": "3",
            },
        ]
    )
    collection_df.to_parquet(collection_path, index=False)

    identifiers_json = tmp_path / "AllIdentifiers.json"
    _write_identifiers(identifiers_json)
    identifiers_xz = tmp_path / "AllIdentifiers.json.xz"
    identifiers_xz.write_bytes(lzma.compress(identifiers_json.read_bytes()))

    prices_json = tmp_path / "AllPrices.json"
    _write_allprices(prices_json)
    prices_xz = tmp_path / "AllPrices.json.xz"
    prices_xz.write_bytes(lzma.compress(prices_json.read_bytes()))

    class _FakeDate:
        @classmethod
        def today(cls):
            return pd.Timestamp("2024-12-05").date()

    monkeypatch.setattr("mtg_tracker.seed.date", _FakeDate)

    summary = run_seed(
        collection_path=collection_path,
        allprices_path=prices_xz,
        identifiers_path=identifiers_xz,
        out_dir=tmp_path / "out",
        state_days=1,
    )

    seed_df = pd.read_parquet(tmp_path / "out" / "seed_90d.parquet")
    state_df = pd.read_parquet(tmp_path / "out" / "state.parquet")
    meta = json.loads((tmp_path / "out" / "meta.json").read_text(encoding="utf-8"))

    assert list(seed_df.columns) == SEED_COLUMNS
    assert list(state_df.columns) == SEED_COLUMNS
    assert len(state_df) == 2
    assert summary.missing_mapping_count == 1
    assert meta["missing_mapping_count"] == 1
    assert meta["num_collection_keys"] == 3
    assert meta["num_mapped_keys"] == 2
    assert meta["num_priced_keys"] == 2


def test_validate_uuid_mapping_against_allprices_raises_when_no_key_match(tmp_path: Path) -> None:
    allprices_path = tmp_path / "AllPrices.json"
    _write_allprices(allprices_path)

    try:
        validate_uuid_mapping_against_allprices(allprices_path, ["not-a-price-uuid"])
    except ValueError as exc:
        assert "Mapped MTGJSON UUIDs not found in AllPrices keyspace" in str(exc)
    else:
        raise AssertionError("expected validation to fail when no UUIDs exist in AllPrices")
