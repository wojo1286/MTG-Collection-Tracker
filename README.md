# MTG-Collection-Tracker

A GitHub-first MTG collection price tracker that:
- Ingests a ManaBox export into a normalized collection dataset (Phase 1)
- Builds a filtered 90-day seed price history + rolling state from MTGJSON (Phase 2)
- (Coming next) Runs daily updates + spike detection without committing a growing DB to git (Phase 3)

---

## Bootstrap (Codespaces / new dev env)

```bash
bash scripts/bootstrap.sh
```

Then run:

```bash
ruff check .
ruff format --check .
pytest -q
mtg-tracker --help
Install (manual)
python -m pip install -U pip
python -m pip install -r requirements.txt -r requirements-dev.txt
python -m pip install -e .
```
## CLI
```bash
mtg-tracker --help
```

Subcommands:

- 'ingest' (Phase 1: ManaBox → normalized collection parquet)
- 'seed' (Phase 2: build 90-day seed + rolling state from MTGJSON)
- 'daily' (Phase 3 local state: update + spike detection + reports)
- 'report' (creates a dummy report artifact)

Note: Most workflows assume the editable install from scripts/bootstrap.sh so mtg-tracker is available.

Local data locations (gitignored)
These directories are intended for real/private files and scratch outputs and are not committed to git:

- downloads/ — MTGJSON dumps (e.g., AllPrices.json.xz, AllIdentifiers.json.xz)
- data/ — real/private collection inputs + outputs (e.g., data/out/collection.parquet)
- tmp/ — scratch outputs / temporary run artifacts
- tests/fixtures/ contains fake sample data only.

Create the folders if they don't exist in a fresh clone:

- mkdir -p downloads data/out data/seed tmp

 ## Phase 2: Seed usage (real/private files)

Store private inputs in gitignored paths:

- Collection parquet: data/out/collection.parquet
- MTGJSON dumps: downloads/AllPrices.json.xz, downloads/AllIdentifiers.json.xz

Example:

```bash
mtg-tracker seed \
  --collection data/out/collection.parquet \
  --allprices downloads/AllPrices.json.xz \
  --identifiers downloads/AllIdentifiers.json.xz \
  --out-dir data/seed
```
Outputs in --out-dir:

- seed_90d.parquet
- state.parquet (rolling N-day state, default 14)
- meta.json

## Fixture-based seed smoke test (tiny, deterministic)

For a tiny local seed run with non-zero mapped and seed rows, first build a collection from the sample TSV,
then run 'seed' with the bundled tiny MTGJSON fixtures:

mkdir -p tmp

```bash
mtg-tracker ingest \
  --input tests/fixtures/manabox_sample.tsv \
  --out tmp/collection.parquet

rm -rf tmp/seed_test

mtg-tracker seed \
  --collection tmp/collection.parquet \
  --allprices tests/fixtures/allprices_tiny.json \
  --identifiers tests/fixtures/allidentifiers_tiny.json \
  --out-dir tmp/seed_test
```

## Quick output sanity check:
```bash
python - <<'PY'
import json
from pathlib import Path
import pyarrow.parquet as pq

out = Path("tmp/seed_test")
meta = json.loads((out / "meta.json").read_text())
print("mapped_keys:", meta.get("mapped_keys"))
print("seed rows:", pq.ParquetFile(out / "seed_90d.parquet").metadata.num_rows)
print("state rows:", pq.ParquetFile(out / "state.parquet").metadata.num_rows)
PY

## Test
```bash
pytest -q
```

## Quick verification
```bash
mtg-tracker --help
pytest -q
```
```makefile
::contentReference[oaicite:0]{index=0}
```


## Phase 3: Daily local update usage

Run daily update using local state files (gitignored paths):

```bash
mtg-tracker daily \
  --collection data/out/collection.parquet \
  --allprices-today downloads/AllPricesToday.json.xz \
  --state-in data/state/state.parquet \
  --seed-state data/seed/state.parquet \
  --state-out data/state/state.parquet \
  --report-dir data/reports \
  --market paper --provider tcgplayer --price-type retail \
  --state-days 14 \
  --windows 1 3 7 \
  --price-floor 5.00 \
  --pct-threshold 0.20 \
  --abs-min 1.00 \
  --pct-override 0.50
```

Outputs:
- `data/state/state.parquet` with rolling local state for the last N unique dates.
- `data/reports/spikes_YYYY-MM-DD.csv` with spike candidates.
- `data/reports/spikes_YYYY-MM-DD.md` markdown summary (UTC date).

Behavior notes:
- If `--state-in` does not exist, `--seed-state` is used to initialize state.
- Missing prices are not forward-filled; spike windows are skipped if today/past prices are missing.
- Guardrail: spike promotion requires `abs_change >= abs_min` OR `pct_change >= pct_override`.
