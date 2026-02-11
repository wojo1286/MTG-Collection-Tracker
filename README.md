# MTG-Collection-Tracker

Phase 0 scaffolding for a collection tracker with pluggable state backends.

## Bootstrap (Codespaces / new dev env)

```bash
bash scripts/bootstrap.sh
```

Then run:

```bash
ruff check .
pytest -q
mtg-tracker --help
```

## Install

```bash
python -m pip install -U pip
python -m pip install -r requirements.txt -r requirements-dev.txt
python -m pip install -e .
```

## CLI

```bash
mtg-tracker --help
# or
python -m mtg_tracker --help
```

Subcommands currently stubbed for phase wiring:
- `ingest`
- `seed`
- `daily`
- `report` (creates a dummy report artifact)

## Test

```bash
pytest -q
```

## Local data locations

- `tests/fixtures/` contains fake sample data only.
- `data/` is for real/private files and is gitignored.
- `tmp/` is for scratch outputs and is gitignored.

## Quick verification

```bash
python -m mtg_tracker --help
mtg-tracker --help
pytest -q
```
