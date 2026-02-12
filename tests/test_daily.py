from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pandas as pd

from mtg_tracker.cli import main
from mtg_tracker.daily import (
    DailyConfig,
    detect_spikes,
    extract_today_prices,
    run_daily,
    truncate_state_dates,
)


def _today_utc() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _write_allprices_today(path: Path, today: str) -> None:
    payload = {
        "data": {
            "uuid-1": {
                "scryfallId": "sid-1",
                "paper": {
                    "tcgplayer": {
                        "retail": {
                            "normal": {
                                today: 12.0,
                            }
                        }
                    }
                },
            },
            "uuid-2": {
                "scryfallId": "sid-2",
                "paper": {
                    "tcgplayer": {
                        "retail": {
                            "normal": {
                                today: Decimal("9.50"),
                            }
                        }
                    }
                },
            },
            "uuid-3": {
                "scryfallId": "sid-3",
                "paper": {
                    "tcgplayer": {
                        "retail": {
                            "normal": {
                                today: 8.2,
                            }
                        }
                    }
                },
            },
        }
    }
    path.write_text(json.dumps(payload, default=str), encoding="utf-8")


def test_daily_smoke_updates_state_and_reports(tmp_path: Path) -> None:
    today = _today_utc()
    d1 = (pd.Timestamp(today) - pd.Timedelta(days=1)).date().isoformat()
    d2 = (pd.Timestamp(today) - pd.Timedelta(days=2)).date().isoformat()
    d3 = (pd.Timestamp(today) - pd.Timedelta(days=3)).date().isoformat()

    collection = pd.DataFrame(
        [
            {
                "scryfall_id": "sid-1",
                "finish": "normal",
                "qty": 2,
                "set_code": "set",
                "collector_number": "1",
                "mtgjson_uuid": "uuid-1",
            },
            {
                "scryfall_id": "sid-2",
                "finish": "normal",
                "qty": 1,
                "set_code": "set",
                "collector_number": "2",
                "mtgjson_uuid": "uuid-2",
            },
            {
                "scryfall_id": "sid-3",
                "finish": "normal",
                "qty": 3,
                "set_code": "set",
                "collector_number": "3",
                "mtgjson_uuid": "uuid-3",
            },
        ]
    )
    collection_path = tmp_path / "collection.parquet"
    collection.to_parquet(collection_path, index=False)

    seed_state = pd.DataFrame(
        [
            {
                "date": d3,
                "scryfall_id": "sid-1",
                "finish": "normal",
                "mtgjson_uuid": "uuid-1",
                "price": 8.0,
            },
            {
                "date": d2,
                "scryfall_id": "sid-1",
                "finish": "normal",
                "mtgjson_uuid": "uuid-1",
                "price": 9.0,
            },
            {
                "date": d1,
                "scryfall_id": "sid-1",
                "finish": "normal",
                "mtgjson_uuid": "uuid-1",
                "price": 10.0,
            },
            {
                "date": d3,
                "scryfall_id": "sid-2",
                "finish": "normal",
                "mtgjson_uuid": "uuid-2",
                "price": 8.9,
            },
            {
                "date": d2,
                "scryfall_id": "sid-2",
                "finish": "normal",
                "mtgjson_uuid": "uuid-2",
                "price": 9.0,
            },
            {
                "date": d1,
                "scryfall_id": "sid-2",
                "finish": "normal",
                "mtgjson_uuid": "uuid-2",
                "price": 9.2,
            },
            {
                "date": d3,
                "scryfall_id": "sid-3",
                "finish": "normal",
                "mtgjson_uuid": "uuid-3",
                "price": 7.5,
            },
            {
                "date": d2,
                "scryfall_id": "sid-3",
                "finish": "normal",
                "mtgjson_uuid": "uuid-3",
                "price": 7.8,
            },
            {
                "date": d1,
                "scryfall_id": "sid-3",
                "finish": "normal",
                "mtgjson_uuid": "uuid-3",
                "price": 8.0,
            },
        ]
    )
    seed_state_path = tmp_path / "seed_state.parquet"
    seed_state.to_parquet(seed_state_path, index=False)

    allprices_today_path = tmp_path / "AllPricesToday.json"
    _write_allprices_today(allprices_today_path, today)

    state_out_path = tmp_path / "state" / "state.parquet"
    report_dir = tmp_path / "reports"

    result = run_daily(
        DailyConfig(
            collection_path=collection_path,
            allprices_today_path=allprices_today_path,
            state_in_path=tmp_path / "missing_state.parquet",
            seed_state_path=seed_state_path,
            state_out_path=state_out_path,
            report_dir=report_dir,
            state_days=4,
            windows=(1, 3, 7),
            price_floor=5.0,
            pct_threshold=0.20,
            abs_min=1.0,
            pct_override=0.50,
        )
    )

    assert result.state_rows > 0
    assert state_out_path.exists()
    assert result.spikes_csv_path.exists()
    assert result.spikes_md_path.exists()

    state_out = pd.read_parquet(state_out_path)
    assert today in set(state_out["date"])
    assert set(state_out["date"]) == {d3, d2, d1, today}

    spikes = pd.read_csv(result.spikes_csv_path)
    assert not spikes.empty
    assert set(spikes["scryfall_id"]) == {"sid-1"}
    assert set(spikes["window_days"]) == {1, 3}


def test_extract_today_prices_coerces_decimal_strings(tmp_path: Path) -> None:
    today = _today_utc()
    path = tmp_path / "today.json"
    payload = {
        "data": {
            "uuid-a": {
                "scryfallId": "sid-a",
                "paper": {
                    "tcgplayer": {
                        "retail": {"normal": {today: "7.25"}},
                    }
                },
            },
            "uuid-b": {
                "scryfallId": "sid-b",
                "paper": {
                    "tcgplayer": {
                        "retail": {"normal": {today: "bad"}},
                    }
                },
            },
        }
    }
    path.write_text(json.dumps(payload), encoding="utf-8")

    keys = pd.DataFrame(
        [
            {"scryfall_id": "sid-a", "finish": "normal"},
            {"scryfall_id": "sid-b", "finish": "normal"},
        ]
    )

    today_df = extract_today_prices(path, keys, today, "paper", "tcgplayer", "retail")

    assert len(today_df) == 1
    assert today_df.iloc[0]["price"] == 7.25


def test_guardrail_allows_high_pct_even_when_abs_below_min() -> None:
    today = "2026-01-08"
    state_df = pd.DataFrame(
        [
            {
                "date": "2026-01-07",
                "scryfall_id": "sid-1",
                "finish": "normal",
                "mtgjson_uuid": "u1",
                "price": 5.2,
            },
            {
                "date": today,
                "scryfall_id": "sid-1",
                "finish": "normal",
                "mtgjson_uuid": "u1",
                "price": 5.8,
            },
        ]
    )
    qty_df = pd.DataFrame([{"scryfall_id": "sid-1", "finish": "normal", "qty": 2}])

    spikes = detect_spikes(
        state_df=state_df,
        qty_df=qty_df,
        today_date=today,
        windows=(1,),
        price_floor=5.0,
        pct_threshold=0.20,
        abs_min=1.0,
        pct_override=0.50,
    )
    assert spikes.empty

    spikes_relaxed = detect_spikes(
        state_df=state_df,
        qty_df=qty_df,
        today_date=today,
        windows=(1,),
        price_floor=5.0,
        pct_threshold=0.10,
        abs_min=1.0,
        pct_override=0.10,
    )
    assert len(spikes_relaxed) == 1


def test_truncate_state_keeps_last_unique_dates() -> None:
    state_df = pd.DataFrame(
        [
            {
                "date": "2026-01-01",
                "scryfall_id": "sid-1",
                "finish": "normal",
                "mtgjson_uuid": "u1",
                "price": 1.0,
            },
            {
                "date": "2026-01-02",
                "scryfall_id": "sid-1",
                "finish": "normal",
                "mtgjson_uuid": "u1",
                "price": 2.0,
            },
            {
                "date": "2026-01-03",
                "scryfall_id": "sid-1",
                "finish": "normal",
                "mtgjson_uuid": "u1",
                "price": 3.0,
            },
            {
                "date": "2026-01-03",
                "scryfall_id": "sid-2",
                "finish": "foil",
                "mtgjson_uuid": "u2",
                "price": 9.0,
            },
        ]
    )

    out = truncate_state_dates(state_df, days=2)
    assert set(out["date"]) == {"2026-01-02", "2026-01-03"}


def test_daily_command_smoke(tmp_path: Path) -> None:
    today = _today_utc()

    collection = pd.DataFrame(
        [
            {
                "scryfall_id": "sid-1",
                "finish": "normal",
                "qty": 2,
                "set_code": "set",
                "collector_number": "1",
            }
        ]
    )
    collection_path = tmp_path / "collection.parquet"
    collection.to_parquet(collection_path, index=False)

    seed_state = pd.DataFrame(
        [
            {
                "date": (pd.Timestamp(today) - pd.Timedelta(days=1)).date().isoformat(),
                "scryfall_id": "sid-1",
                "finish": "normal",
                "mtgjson_uuid": "uuid-1",
                "price": 6.0,
            }
        ]
    )
    seed_state_path = tmp_path / "seed.parquet"
    seed_state.to_parquet(seed_state_path, index=False)

    allprices_today_path = tmp_path / "allprices_today.json"
    _write_allprices_today(allprices_today_path, today)

    rc = main(
        [
            "daily",
            "--collection",
            str(collection_path),
            "--allprices-today",
            str(allprices_today_path),
            "--state-in",
            str(tmp_path / "missing.parquet"),
            "--seed-state",
            str(seed_state_path),
            "--state-out",
            str(tmp_path / "state.parquet"),
            "--report-dir",
            str(tmp_path / "reports"),
        ]
    )

    assert rc == 0
    assert (tmp_path / "state.parquet").exists()
