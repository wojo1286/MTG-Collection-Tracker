# MTG-Collection-Tracker

Phase 0 scaffolding for a collection tracker with pluggable state backends.

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
