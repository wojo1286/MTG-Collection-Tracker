"""CLI entrypoint for Phase 0 command scaffolding."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from mtg_tracker.config import load_config
from mtg_tracker.ingest import ingest_manabox_csv
from mtg_tracker.logging_utils import setup_logging
from mtg_tracker.seed import run_seed
from mtg_tracker.state import build_state_backend

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
        backend = build_state_backend(config)
        LOGGER.info("Running 'daily' stub with backend=%s", type(backend).__name__)
        LOGGER.info("Phase 0 only: 'daily' is intentionally not implemented.")
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

    subparsers.add_parser("daily", help="Phase 3 stub: update daily prices and detect spikes")

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
