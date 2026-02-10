"""CLI entrypoint for mtg-tracker."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence

from mtg_tracker.config import load_config
from mtg_tracker.logging_config import configure_logging

LOGGER = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mtg-tracker", description="MTG Collection Tracker CLI")
    parser.add_argument("--config", default="config.yaml", help="Path to config YAML")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for name, help_text in {
        "ingest": "Phase 1 command stub for collection ingest",
        "seed": "Phase 2 command stub for initial seed",
        "daily": "Phase 3 command stub for daily update",
        "report": "Phase 4 command stub for report generation",
    }.items():
        subparsers.add_parser(name, help=help_text)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    configure_logging()
    config = load_config(args.config)

    LOGGER.info("Loaded config using backend=%s", config.backend_kind)
    LOGGER.info("Command '%s' is a Phase 0 stub and currently performs no action.", args.command)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
