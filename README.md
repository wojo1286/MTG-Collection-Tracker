# MTG Collection Tracker

Phase 0 scaffold for a CLI-based MTG collection price tracker.

## Setup

```bash
python -m pip install -U pip
python -m pip install -r requirements.txt -r requirements-dev.txt
```

## CLI (stub commands)

```bash
mtg-tracker --help
mtg-tracker ingest
mtg-tracker seed
mtg-tracker daily
mtg-tracker report
```

Each subcommand is currently a Phase 0 stub and logs a no-op message.

## Tests

```bash
pytest -q
```
