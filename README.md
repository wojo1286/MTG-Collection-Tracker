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

Subcommands:
- `ingest`
- `seed` (Phase 2: build 90-day seed + rolling state)
- `daily` (Phase 3 stub)
- `report` (creates a dummy report artifact)


## Phase 2 seed usage

Store private inputs in gitignored paths:

- Collection parquet: `data/out/collection.parquet`
- MTGJSON dumps: `downloads/AllPrices.json.xz`, `downloads/AllIdentifiers.json.xz`

Example:

```bash
mtg-tracker seed \
  --collection data/out/collection.parquet \
  --allprices downloads/AllPrices.json.xz \
  --identifiers downloads/AllIdentifiers.json.xz \
  --out-dir data/seed
```

Outputs in `--out-dir`:
- `seed_90d.parquet`
- `state.parquet` (rolling N-day state, default 14)
- `meta.json`

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
