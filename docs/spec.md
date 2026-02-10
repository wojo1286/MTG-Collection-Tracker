diff --git a/docs/spec.md b/docs/spec.md
index 0000000..0000000 100644
--- a/docs/spec.md
+++ b/docs/spec.md
@@ -1,76 +1,91 @@
 # MTG Collection Price Tracker — Spec
 
 ## Summary
 Track daily price movement for a 15k–20k card collection and detect “spikes” with guardrails to reduce noise.
 
 **Primary inputs**
 - ManaBox CSV export: `scryfall_id`, `set_code`, `collector_number`, `finish` (normal/foil/etched), `qty`
 - MTGJSON:
   - `AllPrices.json` (initial 90-day seed)
   - `AllPricesToday.json` (daily updates)
 
 **Primary outputs**
 - Daily spike report (CSV + Markdown summary)
 - Persisted rolling state (small file; NOT committed to git)
 
 ## Non-goals
 - Full historical database in git history
 - Real-time pricing / intraday updates
 - Marketplace arbitrage, buylist tracking, or sales automation (future work)
 
 ## Canonical identifiers
 A unique “printing+finish” in this tracker is:
 - `key = (scryfall_id, finish)`
 - `finish ∈ {normal, foil, etched}`
 
 `set_code` and `collector_number` are kept for reference/debugging, but Scryfall ID is the join key.
+
+## MTGJSON join key note
+MTGJSON `AllPrices*` files are keyed by **MTGJSON card UUID** (not Scryfall ID). This tracker therefore maintains
+an internal mapping between `(scryfall_id, finish)` and `mtgjson_uuid`:
+- Phase 1 produces `mtgjson_uuid` for each collection row (collection output includes a `mtgjson_uuid` column).
+- Phase 2/3 filter MTGJSON using `mtgjson_uuid`, but persist rolling state and run spike detection keyed by
+  `(scryfall_id, finish)` (optionally also storing `mtgjson_uuid` in state for debugging).
 
 ## Pricing model
 MTGJSON contains multiple providers/types. We choose one canonical price series:
 - **Default provider**: `tcgplayer`
 - **Default price type**: `market`
 - **Market**: `paper`
 
 All of the above must be configurable.
 
 ### Missing price handling
 - If a key is missing in MTGJSON for a day: carry **no value** (don’t forward-fill silently).
 - Spike detection requires both endpoints of a window (today and lookback day) to exist.
 
 ## Spike detection rules
 Only evaluate keys that meet the price floor criteria (configurable):
 - Default: `today_price >= 5.00` OR `max(lookback_prices) >= 5.00` (captures cards crossing the threshold)
 
 ### Windows
 Evaluate percent change over:
 - 1 day, 3 day, 7 day windows
 
 ### Baseline spike rule
 A key is a spike candidate if:
 - `pct_change(window) >= 20%` for **any** window in {1d, 3d, 7d}
 
 ### Noise guardrail
 A candidate is only promoted to a spike if:
 - `abs_change(window) >= 1.00` **OR** `pct_change(window) >= 50%`
 
 `abs_change_min` and `pct_change_override` must be configurable.
 
 ### Definitions
 For a window `w` with lookback `t-w`:
 - `abs_change = today_price - past_price`
 - `pct_change = abs_change / past_price` (skip if `past_price <= 0`)
 
 ## Data artifacts (recommended)
 - `collection.parquet` — normalized ManaBox collection (deduped, aggregated qty)
 - `state.parquet` — rolling price window (e.g., last 14 days) for all tracked keys
 - `meta.json` — run metadata (date, source versions, counts, config hash)
 - Daily reports:
   - `spikes_YYYY-MM-DD.csv`
   - `spikes_YYYY-MM-DD.md`
 
 Parquet is recommended for compactness and fast IO.
 
 ## Configuration
 A single config file (e.g., `config.yaml`) plus env var overrides.
 Must include:
 - Price floor (default 5.00)
 - Spike baseline: pct threshold (default 0.20)
 - Guardrails: abs min (default 1.00), pct override (default 0.50)
 - Windows: [1, 3, 7]
 - Rolling state window length (default 14 days)
 - Provider + price type selection
 - State backend selection: `github_release` | `local_path` (server later)
 
 ---
 
 # Phases
 
 ## Phase 0 — Repo scaffolding + state interface
 **Goal:** Establish a safe, testable foundation with a pluggable state backend (GitHub-first).
 
@@ -105,22 +120,26 @@
 - No state/DB files are committed to git
 
 ---
 
 ## Phase 1 — Collection ingest (ManaBox → normalized collection)
 **Goal:** Ingest ManaBox export and produce a normalized collection dataset.
 
 ### Scope
 - CSV parser for ManaBox export
 - Normalize finish values to `{normal, foil, etched}`
 - Aggregate duplicates:
   - same `(scryfall_id, finish)` → sum `qty`
+- Derive and include MTGJSON UUID mapping:
+  - add `mtgjson_uuid` column for each `(scryfall_id, finish)` key (mapping method defined in implementation)
 - Validate required columns and types
 - Output `collection.parquet` (and optional `collection.csv` for debugging)
 
 ### Acceptance criteria
 - Running `mtg-tracker ingest --input manabox.csv --out collection.parquet` succeeds
 - Aggregation is correct (unit tests for duplicates/finish mapping)
 - Summary stats printed/logged:
   - total rows, unique keys, total quantity, invalid rows count
 - Handles 20k rows in a reasonable time on GitHub runner (< 1 minute)
 
 ---
 
 ## Phase 2 — Initial 90-day seed (AllPrices → seed history + initial state)
 **Goal:** Build a 90-day price history for only the collection keys, without storing a giant DB in git.
 
 ### Scope
 - Download (or accept path to) `AllPrices.json`
-- Filter to collection keys and selected provider/type
+- Filter MTGJSON by `mtgjson_uuid` (from `collection.parquet`) and selected provider/type
 - Extract last 90 days of daily prices (where available)
 - Write:
   - `seed_90d.parquet` (one-time artifact; not committed)
-  - Initialize rolling `state.parquet` with the most recent N days (default 14)
+  - Initialize rolling `state.parquet` with the most recent N days (default 14), stored keyed by `(scryfall_id, finish)`
+    (optionally include `mtgjson_uuid` for debugging)
   - `meta.json` with counts and missing coverage
 
 ### Acceptance criteria
 - `mtg-tracker seed --collection collection.parquet --allprices AllPrices.json` completes on a GitHub runner
 - Output artifacts exist and are readable
 - Missing coverage is reported (e.g., “X% keys have no price data”)
 - State file size remains practical (target: tens of MB, not GB, for 20k keys and 14 days)
 
 ---
 
 ## Phase 3 — Daily update + spike detection (AllPricesToday → updated state + reports)
 **Goal:** Daily scheduled run that updates rolling state and emits spike reports.
 
 ### Scope
 - Scheduled workflow (cron) + manual dispatch
 - Download `AllPricesToday.json`
 - Load previous `state.parquet` from GitHub-first backend
-- Append today’s prices, drop older than rolling window (default 14 days)
+- Filter MTGJSON by `mtgjson_uuid` and append today’s prices to state (persisted keyed by `(scryfall_id, finish)`),
+  then drop older than rolling window (default 14 days)
 - Compute spike candidates and apply guardrails
 - Emit daily reports (CSV + Markdown) and upload as workflow artifacts
 - Persist updated `state.parquet` back to the GitHub-first backend
 
 ### Acceptance criteria
 - A scheduled workflow run:
   - restores previous state (or initializes cleanly)
   - updates state
   - produces `spikes_YYYY-MM-DD.csv` and `.md`
   - saves updated state without committing it to git
 - Spike logic is unit-tested with synthetic price series:
   - triggers on 20%+ change
   - guardrails behave as specified
   - respects price floor behavior
