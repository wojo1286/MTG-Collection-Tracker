"""Phase 2 seed builder from collection + MTGJSON dumps."""

from __future__ import annotations

import json
import logging
import lzma
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, BinaryIO

import pandas as pd

LOGGER = logging.getLogger(__name__)

SEED_COLUMNS = ["date", "scryfall_id", "finish", "mtgjson_uuid", "price"]


@dataclass(frozen=True)
class SeedSummary:
    """Operational summary for the seed run."""

    run_date_utc: str
    provider: str
    price_type: str
    market: str
    state_days: int
    num_collection_keys: int
    num_mapped_keys: int
    num_priced_keys: int
    seed_rows: int
    state_rows: int
    missing_price_keys_count: int
    missing_mapping_count: int
    missing_mapping_examples: list[str]
    missing_price_examples: list[str]

    def as_meta(self) -> dict[str, Any]:
        return {
            "run_date_utc": self.run_date_utc,
            "provider": self.provider,
            "price_type": self.price_type,
            "market": self.market,
            "state_days": self.state_days,
            "num_collection_keys": self.num_collection_keys,
            "num_mapped_keys": self.num_mapped_keys,
            "num_priced_keys": self.num_priced_keys,
            "seed_rows": self.seed_rows,
            "state_rows": self.state_rows,
            "missing_price_keys_count": self.missing_price_keys_count,
            "missing_mapping_count": self.missing_mapping_count,
            "missing_key_examples": {
                "missing_mapping": self.missing_mapping_examples,
                "missing_price": self.missing_price_examples,
            },
        }


def run_seed(
    collection_path: Path,
    allprices_path: Path,
    identifiers_path: Path,
    out_dir: Path,
    state_days: int = 14,
    provider: str = "tcgplayer",
    price_type: str = "market",
    market: str = "paper",
) -> SeedSummary:
    """Build seed_90d.parquet, state.parquet, and meta.json for Phase 2."""

    key_df = load_collection_keys(collection_path)
    unique_scryfall_ids = set(key_df["scryfall_id"].tolist())

    sid_to_uuid = build_scryfall_to_uuid_map(identifiers_path, unique_scryfall_ids)
    key_df["mtgjson_uuid"] = key_df["scryfall_id"].map(sid_to_uuid)

    mapped_df = key_df.dropna(subset=["mtgjson_uuid"]).copy()
    mapped_df["mtgjson_uuid"] = mapped_df["mtgjson_uuid"].astype(str)

    validate_mapped_uuids_in_allprices(
        allprices_path=allprices_path,
        mapped_uuids=mapped_df["mtgjson_uuid"].tolist(),
    )

    missing_mapping = sorted(unique_scryfall_ids - set(sid_to_uuid.keys()))

    prices_df = extract_seed_prices(
        allprices_path=allprices_path,
        mapped_keys_df=mapped_df,
        provider=provider,
        price_type=price_type,
        market=market,
        days=90,
    )

    state_df = build_state_window(prices_df, state_days)

    out_dir.mkdir(parents=True, exist_ok=True)
    prices_df.to_parquet(out_dir / "seed_90d.parquet", index=False)
    state_df.to_parquet(out_dir / "state.parquet", index=False)

    priced_keys = set(zip(prices_df["scryfall_id"], prices_df["finish"], strict=False))
    mapped_keys = set(zip(mapped_df["scryfall_id"], mapped_df["finish"], strict=False))
    missing_price_keys = sorted(mapped_keys - priced_keys)

    summary = SeedSummary(
        run_date_utc=datetime.now(timezone.utc).isoformat(),
        provider=provider,
        price_type=price_type,
        market=market,
        state_days=state_days,
        num_collection_keys=len(key_df),
        num_mapped_keys=len(mapped_df),
        num_priced_keys=len(priced_keys),
        seed_rows=len(prices_df),
        state_rows=len(state_df),
        missing_price_keys_count=len(missing_price_keys),
        missing_mapping_count=len(missing_mapping),
        missing_mapping_examples=_format_key_examples(missing_mapping),
        missing_price_examples=_format_key_examples(missing_price_keys),
    )

    (out_dir / "meta.json").write_text(json.dumps(summary.as_meta(), indent=2), encoding="utf-8")

    LOGGER.info("Seed rows: %d", summary.seed_rows)
    LOGGER.info("State rows: %d", summary.state_rows)
    LOGGER.info("Mapped keys: %d/%d", summary.num_mapped_keys, summary.num_collection_keys)

    return summary


def load_collection_keys(collection_path: Path) -> pd.DataFrame:
    """Load collection parquet and return unique (scryfall_id, finish) keys."""

    collection_df = pd.read_parquet(collection_path)
    required_cols = {"scryfall_id", "finish", "qty", "set_code", "collector_number"}
    missing = sorted(required_cols - set(collection_df.columns))
    if missing:
        missing_csv = ", ".join(missing)
        raise ValueError(f"collection parquet missing required columns: {missing_csv}")

    keys_df = (
        collection_df[["scryfall_id", "finish"]]
        .dropna(subset=["scryfall_id", "finish"])
        .drop_duplicates()
        .sort_values(["scryfall_id", "finish"])
        .reset_index(drop=True)
    )

    keys_df["scryfall_id"] = keys_df["scryfall_id"].astype(str)
    keys_df["finish"] = keys_df["finish"].astype(str)
    return keys_df


def build_scryfall_to_uuid_map(
    identifiers_path: Path, collection_scryfall_ids: set[str]
) -> dict[str, str]:
    """Stream AllIdentifiers and map collection scryfall ids to MTGJSON UUIDs."""

    sid_to_uuid: dict[str, str] = {}

    for uuid, payload in iter_data_kv_items(identifiers_path):
        if not isinstance(payload, dict):
            continue
        for sid in _iter_scryfall_ids(payload):
            if sid in collection_scryfall_ids:
                sid_to_uuid[sid] = str(uuid)

        if len(sid_to_uuid) == len(collection_scryfall_ids):
            break

    return sid_to_uuid


def extract_seed_prices(
    allprices_path: Path,
    mapped_keys_df: pd.DataFrame,
    provider: str,
    price_type: str,
    market: str,
    days: int,
) -> pd.DataFrame:
    """Stream AllPrices and emit long-form rows only for mapped keys and available dates."""

    if mapped_keys_df.empty:
        return pd.DataFrame(columns=SEED_COLUMNS)

    uuid_to_keys: dict[str, list[tuple[str, str]]] = {}
    for row in mapped_keys_df.itertuples(index=False):
        uuid_to_keys.setdefault(str(row.mtgjson_uuid), []).append(
            (str(row.scryfall_id), str(row.finish))
        )

    target_uuids = set(uuid_to_keys.keys())
    min_date = date.today() - timedelta(days=days - 1)

    output_rows: list[dict[str, Any]] = []
    found_uuids: set[str] = set()

    for uuid, payload in iter_data_kv_items(allprices_path):
        uuid_str = str(uuid)
        if uuid_str not in target_uuids:
            continue

        found_uuids.add(uuid_str)
        series = _extract_price_series(
            payload, market=market, provider=provider, price_type=price_type
        )
        if not isinstance(series, dict):
            continue

        keys_for_uuid = uuid_to_keys[uuid_str]
        for day_str, raw_price in series.items():
            parsed_day = _parse_price_date(day_str)
            if not parsed_day or parsed_day < min_date:
                continue

            price = _to_positive_float(raw_price)
            if price is None:
                continue

            for scryfall_id, finish in keys_for_uuid:
                output_rows.append(
                    {
                        "date": day_str,
                        "scryfall_id": scryfall_id,
                        "finish": finish,
                        "mtgjson_uuid": uuid_str,
                        "price": price,
                    }
                )

        if len(found_uuids) == len(target_uuids):
            break

    if not output_rows:
        return pd.DataFrame(columns=SEED_COLUMNS)

    seed_df = pd.DataFrame(output_rows, columns=SEED_COLUMNS)
    return seed_df.sort_values(["scryfall_id", "finish", "date"]).reset_index(drop=True)


def validate_mapped_uuids_in_allprices(
    allprices_path: Path, mapped_uuids: list[str], sample_size: int = 20
) -> None:
    """Guard against wrong identifier mapping by validating sampled UUIDs in AllPrices."""

    sampled = list(dict.fromkeys(str(uuid) for uuid in mapped_uuids if uuid))[:sample_size]
    if not sampled:
        return

    sampled_set = set(sampled)
    for uuid, _ in iter_data_kv_items(allprices_path):
        if uuid in sampled_set:
            return

    raise ValueError(
        "Sanity check failed: none of the sampled mapped MTGJSON UUIDs were found in "
        "AllPrices data keys. Mapping likely returned non-MTGJSON IDs (for example "
        "Scryfall IDs) or AllIdentifiers and AllPrices are from mismatched versions."
    )


def build_state_window(seed_df: pd.DataFrame, state_days: int) -> pd.DataFrame:
    """Trim seed rows to each key's most recent state_days dates."""

    if seed_df.empty:
        return pd.DataFrame(columns=SEED_COLUMNS)

    state_df = (
        seed_df.sort_values(["scryfall_id", "finish", "date"])
        .groupby(["scryfall_id", "finish"], group_keys=False)
        .tail(state_days)
        .reset_index(drop=True)
    )
    return state_df[SEED_COLUMNS]


def iter_data_kv_items(path: Path) -> Iterator[tuple[str, Any]]:
    """Yield (key, value) items from top-level JSON object's `data` map."""

    try:
        import ijson  # type: ignore
    except ModuleNotFoundError:
        LOGGER.warning("ijson is not installed; using non-streaming JSON fallback for %s", path)
        with open_json_stream(path) as fp:
            payload = json.load(fp)
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        if isinstance(data, dict):
            for key, value in data.items():
                yield str(key), value
        return

    with open_json_stream(path) as fp:
        for key, value in ijson.kvitems(fp, "data"):
            yield str(key), value


def open_json_stream(path: Path) -> BinaryIO:
    """Open JSON or JSON.xz path as a binary stream for parsers."""

    if path.suffix.lower() == ".xz":
        return lzma.open(path, mode="rb")
    return path.open("rb")


def _extract_price_series(
    payload: Any, market: str, provider: str, price_type: str
) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None

    market_node = payload.get(market)
    if not isinstance(market_node, dict):
        return None

    provider_node = market_node.get(provider)
    if not isinstance(provider_node, dict):
        return None

    series = provider_node.get(price_type)
    if isinstance(series, dict):
        return series

    return None


def _iter_scryfall_ids(payload: Any) -> Iterator[str]:
    """Yield candidate scryfall IDs from identifier payload variants."""

    if not isinstance(payload, dict):
        return

    identifiers = payload.get("identifiers")
    if isinstance(identifiers, dict):
        scryfall_id = identifiers.get("scryfallId")
        if scryfall_id:
            yield str(scryfall_id)

    for key in ("scryfallId", "scryfall_id"):
        value = payload.get(key)
        if value:
            yield str(value)


def _to_positive_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None

    if parsed <= 0:
        return None

    return parsed


def _parse_price_date(value: Any) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _format_key_examples(
    keys: Iterable[str] | Iterable[tuple[str, str]], max_examples: int = 10
) -> list[str]:
    out: list[str] = []
    for item in keys:
        if isinstance(item, tuple):
            out.append(f"{item[0]}|{item[1]}")
        else:
            out.append(str(item))
        if len(out) >= max_examples:
            break
    return out
