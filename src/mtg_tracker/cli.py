"""CLI entrypoint for Phase 0 command scaffolding."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from mtg_tracker.config import load_config
from mtg_tracker.daily import DailyConfig, run_daily
from mtg_tracker.ingest import ingest_manabox_csv
from mtg_tracker.logging_utils import setup_logging
from mtg_tracker.seed import run_seed

LOGGER = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    config = load_config(args.config)
    setup_logging(config.logging_level)

    if args.command == "report":
        run_report(args)
        return 0

    if args.command == "ingest":
        run_ingest(args)
        return 0

    if args.command == "seed":
        run_seed_command(args)
        return 0

    if args.command == "daily":
        run_daily_command(args, config.raw)
        return 0

    parser.print_help()
    return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mtg-tracker")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")

    subparsers = parser.add_subparsers(dest="command")

    ingest_parser = subparsers.add_parser(
        "ingest", help="Ingest ManaBox CSV into normalized collection"
    )
    ingest_parser.add_argument("--input", required=True, help="Path to ManaBox CSV export")
    ingest_parser.add_argument("--out", required=True, help="Output collection parquet path")
    ingest_parser.add_argument(
        "--debug-csv",
        action="store_true",
        help="Also write a collection CSV next to --out for debugging",
    )

    seed_parser = subparsers.add_parser(
        "seed",
        help="Create initial 90-day seed and rolling state for collection keys",
    )
    seed_parser.add_argument("--collection", required=True, help="Path to collection parquet")
    seed_parser.add_argument(
        "--allprices",
        required=True,
        help="Path to AllPrices.json or AllPrices.json.xz",
    )
    seed_parser.add_argument(
        "--identifiers",
        required=True,
        help="Path to AllIdentifiers.json or AllIdentifiers.json.xz",
    )
    seed_parser.add_argument("--out-dir", required=True, help="Output directory for seed artifacts")
    seed_parser.add_argument(
        "--state-days",
        type=int,
        default=14,
        help="Number of recent days retained in state.parquet (default: 14)",
    )
    seed_parser.add_argument("--provider", default="tcgplayer", help="Pricing provider")
    seed_parser.add_argument("--price-type", default="market", help="Price type")
    seed_parser.add_argument("--market", default="paper", help="Market scope")

    daily_parser = subparsers.add_parser(
        "daily", help="Update local rolling state, detect spikes, and write daily reports"
    )
    daily_parser.add_argument("--collection", required=True, help="Path to collection parquet")
    daily_parser.add_argument(
        "--allprices-today",
        required=True,
        help="Path to AllPricesToday.json or AllPricesToday.json.xz",
    )
    daily_parser.add_argument(
        "--state-in",
        default="data/state/state.parquet",
        help="Input rolling state parquet path",
    )
    daily_parser.add_argument(
        "--seed-state",
        default="data/seed/state.parquet",
        help="Seed state parquet path used when --state-in is missing",
    )
    daily_parser.add_argument(
        "--state-out",
        default="data/state/state.parquet",
        help="Output rolling state parquet path",
    )
    daily_parser.add_argument(
        "--report-dir", default="data/reports", help="Output directory for daily reports"
    )
    daily_parser.add_argument("--market", default="paper", help="Market scope")
    daily_parser.add_argument("--provider", default="tcgplayer", help="Pricing provider")
    daily_parser.add_argument("--price-type", default="retail", help="Price type")
    daily_parser.add_argument(
        "--state-days",
        type=int,
        default=14,
        help="Number of unique dates retained in rolling state",
    )
    daily_parser.add_argument(
        "--windows",
        nargs="+",
        type=int,
        default=[1, 3, 7],
        help="Window sizes in days for spike checks",
    )
    daily_parser.add_argument(
        "--price-floor",
        type=float,
        default=5.0,
        help="Minimum today price required for spike checks",
    )
    daily_parser.add_argument(
        "--pct-threshold",
        type=float,
        default=0.20,
        help="Base percent-change threshold",
    )
    daily_parser.add_argument(
        "--abs-min",
        type=float,
        default=1.0,
        help="Guardrail minimum absolute change",
    )
    daily_parser.add_argument(
        "--pct-override",
        type=float,
        default=0.50,
        help="Guardrail percent override",
    )

    report_parser = subparsers.add_parser("report", help="Generate a no-op report artifact")
    report_parser.add_argument(
        "--output-dir", default="artifacts", help="Output directory for report"
    )

    return parser


def run_report(args: argparse.Namespace) -> None:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).isoformat()

    report_md = output_dir / "dummy_report.md"
    report_json = output_dir / "dummy_report.json"

    report_md.write_text(
        "# MTG Tracker Dummy Report\n\n"
        "This is a Phase 0 no-op artifact to validate workflow wiring.\n"
        f"Generated at: {timestamp}\n",
        encoding="utf-8",
    )
    report_json.write_text(
        json.dumps({"status": "ok", "generated_at": timestamp}, indent=2),
        encoding="utf-8",
    )
    LOGGER.info("Wrote dummy report artifacts to %s", output_dir)


def run_ingest(args: argparse.Namespace) -> None:
    ingest_manabox_csv(
        input_path=Path(args.input),
        output_path=Path(args.out),
        debug_csv=bool(args.debug_csv),
    )


def run_seed_command(args: argparse.Namespace) -> None:
    run_seed(
        collection_path=Path(args.collection),
        allprices_path=Path(args.allprices),
        identifiers_path=Path(args.identifiers),
        out_dir=Path(args.out_dir),
        state_days=int(args.state_days),
        provider=str(args.provider),
        price_type=str(args.price_type),
        market=str(args.market),
    )


def run_daily_command(args: argparse.Namespace, raw_config: dict[str, object]) -> None:
    daily_defaults = raw_config.get("daily", {})
    if not isinstance(daily_defaults, dict):
        daily_defaults = {}

    result = run_daily(
        DailyConfig(
            collection_path=Path(args.collection),
            allprices_today_path=Path(args.allprices_today),
            state_in_path=Path(args.state_in),
            seed_state_path=Path(args.seed_state),
            state_out_path=Path(args.state_out),
            report_dir=Path(args.report_dir),
            market=str(args.market),
            provider=str(args.provider),
            price_type=str(args.price_type),
            state_days=int(daily_defaults.get("state_days", args.state_days)),
            windows=tuple(int(w) for w in daily_defaults.get("windows", args.windows)),
            price_floor=float(daily_defaults.get("price_floor", args.price_floor)),
            pct_threshold=float(daily_defaults.get("pct_threshold", args.pct_threshold)),
            abs_min=float(daily_defaults.get("abs_min", args.abs_min)),
            pct_override=float(daily_defaults.get("pct_override", args.pct_override)),
        )
    )
    LOGGER.info(
        "Daily run complete: date=%s state_rows=%d spikes=%d",
        result.today_date,
        result.state_rows,
        result.spike_rows,
    )
