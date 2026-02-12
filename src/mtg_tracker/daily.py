"""Phase 3 local daily update + spike detection + reporting."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from mtg_tracker.seed import (
    _coerce_price,
    iter_data_kv_items,
    load_collection_keys,
    open_json_stream,
)

LOGGER = logging.getLogger(__name__)

STATE_COLUMNS = ["date", "scryfall_id", "finish", "mtgjson_uuid", "price"]
SPIKE_COLUMNS = [
    "scryfall_id",
    "finish",
    "mtgjson_uuid",
    "qty",
    "today_date",
    "today_price",
    "window_days",
    "past_date",
    "past_price",
    "abs_change",
    "pct_change",
]


@dataclass(frozen=True)
class DailyConfig:
    """Runtime configuration for a daily update run."""

    collection_path: Path
    allprices_today_path: Path
    state_in_path: Path
    seed_state_path: Path
    state_out_path: Path
    report_dir: Path
    market: str = "paper"
    provider: str = "tcgplayer"
    price_type: str = "retail"
    state_days: int = 14
    windows: tuple[int, ...] = (1, 3, 7)
    price_floor: float = 5.0
    pct_threshold: float = 0.20
    abs_min: float = 1.0
    pct_override: float = 0.50


@dataclass(frozen=True)
class DailyResult:
    """Useful outputs from a daily run for tests/CLI logging."""

    today_date: str
    state_rows: int
    spike_rows: int
    state_out_path: Path
    spikes_csv_path: Path
    spikes_md_path: Path


def run_daily(config: DailyConfig) -> DailyResult:
    """Run local-state daily update and generate spike reports."""

    today_date = datetime.now(timezone.utc).date().isoformat()
    collection_df = pd.read_parquet(config.collection_path)
    qty_df = _build_qty_frame(collection_df)

    prior_state = _load_prior_state(config.state_in_path, config.seed_state_path)
    today_prices = extract_today_prices(
        allprices_today_path=config.allprices_today_path,
        collection_keys=load_collection_keys(config.collection_path),
        date_str=today_date,
        market=config.market,
        provider=config.provider,
        price_type=config.price_type,
    )

    updated_state = merge_state(prior_state, today_prices)
    truncated_state = truncate_state_dates(updated_state, days=config.state_days)

    spikes_df = detect_spikes(
        state_df=truncated_state,
        qty_df=qty_df,
        today_date=today_date,
        windows=config.windows,
        price_floor=config.price_floor,
        pct_threshold=config.pct_threshold,
        abs_min=config.abs_min,
        pct_override=config.pct_override,
    )

    config.state_out_path.parent.mkdir(parents=True, exist_ok=True)
    truncated_state.to_parquet(config.state_out_path, index=False)

    config.report_dir.mkdir(parents=True, exist_ok=True)
    spikes_csv_path = config.report_dir / f"spikes_{today_date}.csv"
    spikes_md_path = config.report_dir / f"spikes_{today_date}.md"
    spikes_df.to_csv(spikes_csv_path, index=False)
    spikes_md_path.write_text(
        render_spikes_markdown(
            spikes_df=spikes_df,
            today_date=today_date,
            windows=config.windows,
            price_floor=config.price_floor,
            pct_threshold=config.pct_threshold,
            abs_min=config.abs_min,
            pct_override=config.pct_override,
        ),
        encoding="utf-8",
    )

    LOGGER.info("Wrote updated state: %s", config.state_out_path)
    LOGGER.info("Wrote spikes report: %s", spikes_csv_path)

    return DailyResult(
        today_date=today_date,
        state_rows=len(truncated_state),
        spike_rows=len(spikes_df),
        state_out_path=config.state_out_path,
        spikes_csv_path=spikes_csv_path,
        spikes_md_path=spikes_md_path,
    )


def _build_qty_frame(collection_df: pd.DataFrame) -> pd.DataFrame:
    if "qty" not in collection_df.columns:
        return pd.DataFrame(columns=["scryfall_id", "finish", "qty"])

    qty_df = (
        collection_df[["scryfall_id", "finish", "qty"]]
        .dropna(subset=["scryfall_id", "finish", "qty"])
        .assign(
            scryfall_id=lambda df: df["scryfall_id"].astype(str),
            finish=lambda df: df["finish"].astype(str),
            qty=lambda df: pd.to_numeric(df["qty"], errors="coerce"),
        )
        .dropna(subset=["qty"])
        .groupby(["scryfall_id", "finish"], as_index=False)["qty"]
        .sum()
    )
    return qty_df


def _load_prior_state(state_in_path: Path, seed_state_path: Path) -> pd.DataFrame:
    if state_in_path.exists():
        state_df = pd.read_parquet(state_in_path)
    elif seed_state_path.exists():
        state_df = pd.read_parquet(seed_state_path)
    else:
        raise FileNotFoundError(
            f"No prior state found. state-in={state_in_path} seed-state={seed_state_path}"
        )

    state_df = _normalize_state_columns(state_df)
    return state_df


def _normalize_state_columns(state_df: pd.DataFrame) -> pd.DataFrame:
    out = state_df.copy()
    for column in ["date", "scryfall_id", "finish", "price"]:
        if column not in out.columns:
            raise ValueError(f"state parquet missing required column: {column}")

    if "mtgjson_uuid" not in out.columns:
        out["mtgjson_uuid"] = pd.NA

    out["date"] = out["date"].astype(str)
    out["scryfall_id"] = out["scryfall_id"].astype(str)
    out["finish"] = out["finish"].astype(str)
    out["mtgjson_uuid"] = out["mtgjson_uuid"].astype("string")
    out["price"] = pd.to_numeric(out["price"], errors="coerce")
    out = out.dropna(subset=["price"])
    out["price"] = out["price"].astype(float)
    return out[STATE_COLUMNS]


def extract_today_prices(
    allprices_today_path: Path,
    collection_keys: pd.DataFrame,
    date_str: str,
    market: str,
    provider: str,
    price_type: str,
) -> pd.DataFrame:
    """Extract today's prices for collection keys keyed by (scryfall_id, finish)."""

    if collection_keys.empty:
        return pd.DataFrame(columns=STATE_COLUMNS)

    sid_to_finishes = (
        collection_keys.groupby("scryfall_id")["finish"].apply(list).to_dict()  # type: ignore[arg-type]
    )

    rows: list[dict[str, Any]] = []
    for uuid, payload in iter_data_kv_items(allprices_today_path):
        if not isinstance(payload, dict):
            continue

        market_node = payload.get(market)
        if not isinstance(market_node, dict):
            continue

        provider_node = market_node.get(provider)
        if not isinstance(provider_node, dict):
            continue

        price_node = provider_node.get(price_type)
        if not isinstance(price_node, dict):
            continue

        scryfall_id = _extract_scryfall_id(payload)
        if not scryfall_id or scryfall_id not in sid_to_finishes:
            continue

        for finish in sid_to_finishes[scryfall_id]:
            finish_series = _resolve_finish_series(price_node, finish)
            if not isinstance(finish_series, dict):
                continue

            raw_price = finish_series.get(date_str)
            price = _coerce_price(raw_price)
            if price is None or price <= 0:
                continue

            rows.append(
                {
                    "date": date_str,
                    "scryfall_id": scryfall_id,
                    "finish": finish,
                    "mtgjson_uuid": uuid,
                    "price": float(price),
                }
            )

    if not rows:
        return pd.DataFrame(columns=STATE_COLUMNS)

    return pd.DataFrame(rows, columns=STATE_COLUMNS).drop_duplicates(
        subset=["date", "scryfall_id", "finish"], keep="last"
    )


def _extract_scryfall_id(payload: dict[str, Any]) -> str | None:
    identifiers = payload.get("identifiers")
    if isinstance(identifiers, dict):
        for key in ("scryfallId", "scryfall_id", "scryfallID"):
            candidate = identifiers.get(key)
            if candidate:
                return str(candidate)

    for key in ("scryfallId", "scryfall_id", "scryfallID"):
        candidate = payload.get(key)
        if candidate:
            return str(candidate)

    return None


def _resolve_finish_series(price_node: dict[str, Any], finish: str) -> dict[str, Any] | None:
    direct = price_node.get(str(finish))
    if isinstance(direct, dict):
        return direct

    if finish == "normal" and any(_looks_like_date(key) for key in price_node):
        return price_node

    return None


def _looks_like_date(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return False
    return True


def merge_state(prior_state: pd.DataFrame, today_prices: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat([prior_state, today_prices], ignore_index=True)
    combined = combined.sort_values(["scryfall_id", "finish", "date", "mtgjson_uuid"])
    combined = combined.drop_duplicates(subset=["date", "scryfall_id", "finish"], keep="last")
    return combined.reset_index(drop=True)[STATE_COLUMNS]


def truncate_state_dates(state_df: pd.DataFrame, days: int) -> pd.DataFrame:
    if state_df.empty:
        return state_df[STATE_COLUMNS]

    unique_dates = sorted(state_df["date"].astype(str).unique())
    keep_dates = set(unique_dates[-days:])
    out = state_df[state_df["date"].isin(keep_dates)].copy()
    return out.sort_values(["scryfall_id", "finish", "date"]).reset_index(drop=True)[STATE_COLUMNS]


def detect_spikes(
    state_df: pd.DataFrame,
    qty_df: pd.DataFrame,
    today_date: str,
    windows: tuple[int, ...],
    price_floor: float,
    pct_threshold: float,
    abs_min: float,
    pct_override: float,
) -> pd.DataFrame:
    if state_df.empty:
        return pd.DataFrame(columns=SPIKE_COLUMNS)

    pivot = (
        state_df.pivot_table(
            index=["scryfall_id", "finish", "mtgjson_uuid"],
            columns="date",
            values="price",
            aggfunc="last",
        )
        .sort_index(axis=1)
        .reset_index()
    )

    if today_date not in pivot.columns:
        return pd.DataFrame(columns=SPIKE_COLUMNS)

    candidates: list[pd.DataFrame] = []
    for window in sorted(set(windows)):
        if window <= 0:
            continue

        past_date = _date_minus_days(today_date, window)
        if past_date not in pivot.columns:
            continue

        subset = pivot[["scryfall_id", "finish", "mtgjson_uuid", today_date, past_date]].copy()
        subset = subset.rename(columns={today_date: "today_price", past_date: "past_price"})
        subset = subset.dropna(subset=["today_price", "past_price"])
        subset = subset[subset["past_price"] > 0]
        subset = subset[subset["today_price"] >= price_floor]
        if subset.empty:
            continue

        subset["abs_change"] = subset["today_price"] - subset["past_price"]
        subset["pct_change"] = subset["abs_change"] / subset["past_price"]
        subset = subset[subset["pct_change"] >= pct_threshold]
        subset = subset[(subset["abs_change"] >= abs_min) | (subset["pct_change"] >= pct_override)]
        if subset.empty:
            continue

        subset["today_date"] = today_date
        subset["past_date"] = past_date
        subset["window_days"] = window
        candidates.append(subset)

    if not candidates:
        return pd.DataFrame(columns=SPIKE_COLUMNS)

    spikes_df = pd.concat(candidates, ignore_index=True)
    spikes_df = spikes_df.merge(qty_df, on=["scryfall_id", "finish"], how="left")
    if "qty" not in spikes_df.columns:
        spikes_df["qty"] = pd.NA

    spikes_df = spikes_df[SPIKE_COLUMNS]
    spikes_df = spikes_df.sort_values(["pct_change", "abs_change"], ascending=False).reset_index(
        drop=True
    )
    return spikes_df


def _date_minus_days(date_str: str, days: int) -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d").date()
    return (dt - pd.Timedelta(days=days)).isoformat()


def render_spikes_markdown(
    spikes_df: pd.DataFrame,
    today_date: str,
    windows: tuple[int, ...],
    price_floor: float,
    pct_threshold: float,
    abs_min: float,
    pct_override: float,
) -> str:
    header = [
        f"# Daily Spikes Report ({today_date})",
        "",
        f"- Windows: {', '.join(str(w) for w in windows)}",
        f"- Price floor: {price_floor:.2f}",
        f"- pct_threshold: {pct_threshold:.2f}",
        f"- Guardrail: abs_change >= {abs_min:.2f} OR pct_change >= {pct_override:.2f}",
        "",
        f"Total spikes: {len(spikes_df)}",
        "",
    ]

    if spikes_df.empty:
        header.append("No spikes met thresholds today.")
        return "\n".join(header) + "\n"

    top = spikes_df.head(20)
    lines = [
        "| scryfall_id | finish | qty | window_days | today_price | past_price "
        "| abs_change | pct_change |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in top.itertuples(index=False):
        row_line = (
            "| {scryfall_id} | {finish} | {qty} | {window_days} | "
            "{today_price:.2f} | {past_price:.2f} | {abs_change:.2f} | {pct_change:.2%} |"
        ).format(
            scryfall_id=row.scryfall_id,
            finish=row.finish,
            qty="" if pd.isna(row.qty) else int(row.qty),
            window_days=int(row.window_days),
            today_price=float(row.today_price),
            past_price=float(row.past_price),
            abs_change=float(row.abs_change),
            pct_change=float(row.pct_change),
        )
        lines.append(row_line)

    return "\n".join(header + ["Top movers by pct_change:", ""] + lines) + "\n"


def load_allprices_today(path: Path) -> dict[str, Any]:
    """Helper for testing/debugging allprices-today fixture loading."""

    with open_json_stream(path) as fp:
        payload = json.load(fp)
    if not isinstance(payload, dict):
        return {}
    return payload
