# Codex / Agent Instructions

You are working in this repo as an implementation agent. Follow these rules so we can ship incrementally and keep history clean.

## Ground rules
- **One phase per PR.** Do not mix Phase 1 work with Phase 2+.
- Keep PRs **small and reviewable** (ideally < 500 lines net).
- Add/modify **tests with every behavior change**.
- Don’t introduce new tooling unless it’s clearly justified.
- Prefer refactors only when required to land the phase cleanly.
- Update `docs/spec.md` only if the implementation changes the spec.

## What not to do
- Do **NOT** commit any ever-growing database or state into git history.
- Do **NOT** add MTGJSON dumps to the repo.
- Do **NOT** rely on manual steps for the daily workflow (beyond initial setup).

## Definition of Done (per PR)
- CI passes (tests + lint)
- New functionality is covered by unit tests
- Commands in this file run cleanly
- Any new config options are documented in code comments and/or README

## Commands to run
Assume Python 3.11+.

### Install
- `python -m pip install -U pip`
- `python -m pip install -r requirements.txt -r requirements-dev.txt`

### Lint / format
- `ruff check .`
- `ruff format --check .`

### Tests
- `pytest -q`

(If mypy is added later, include `mypy .` here, but don’t add it without a clear reason.)

## Repo structure conventions
- `src/mtg_tracker/` — library code
- `src/mtg_tracker/cli.py` — CLI entrypoint
- `tests/` — unit tests only (fast, deterministic)
- `docs/` — spec and operational docs
- `scripts/` — optional helper scripts (thin wrappers only)
- `state/` — local dev state (gitignored)

## State / DB strategy (GitHub-first, server-later)
We persist a **small rolling state file** outside git, so daily runs can compute deltas without a committed DB.

### Current target approach
- Rolling state lives in a **GitHub Release asset** (preferred for persistence)
  - e.g., release tag: `state-latest`
  - assets:
    - `state.parquet` (rolling 14d prices)
    - `meta.json` (run metadata, config hash)
- Each daily workflow run:
  1) downloads the latest release assets (if they exist)
  2) updates state using `AllPricesToday.json`
  3) uploads the new state back to the same release (replace assets)

### Local/server approach (later)
- Same files, stored on disk (e.g., `/var/lib/mtg-tracker/state.parquet`)
- Backend selected by config/env (no code changes required)

## Phase guidance (what to implement when)

### Phase 0 (infrastructure)
Implement:
- Project skeleton + CI
- Config loader
- State backend interface:
  - `load_state() -> (state_df, meta)`
  - `save_state(state_df, meta) -> None`
- `LocalPathBackend` fully working
- `GitHubReleaseBackend` can be stubbed, but must have the interface and tests for the non-network parts

Do not implement:
- Full MTGJSON parsing
- Spike logic (that is Phase 3)

### Phase 3 (daily updates)
Implement:
- Full `GitHubReleaseBackend` behavior:
  - fetch latest release asset(s)
  - handle “no existing state” gracefully
  - upload/replace assets atomically enough to avoid partial corruption
- Rolling window maintenance
- Spike detection + guardrails + reporting
- Workflow wiring (cron + permissions + artifacts)

## CI / workflow notes
- Workflows must have least-privilege permissions.
- If the workflow uploads release assets, it needs `contents: write`.
- Artifacts (daily reports) are fine to store as workflow artifacts; state must be persisted via Release assets (or another external store), not via git.

## Testing expectations
- Unit test finish normalization and keying
- Unit test spike rules with synthetic price series
- Unit test rolling-window truncation behavior
- Avoid tests that require downloading MTGJSON (mock IO boundaries)
