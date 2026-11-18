# Architecture Decision & Technical Design: Repository Skeleton and Data Ingestion (`trump-quant` — Part 1)

**Date:** 2026-11-18  
**Scope:** Flat modular monorepo skeleton + working Part 1 (Truth Social and Binance data ingestion to Bronze Parquet) + scaffolding for future ETL and AI modules.

---

## 1. Intent and Why Now

The immediate problem is **losing ongoing data day by day**: as long as ingestion is not running regularly, new Truth Social posts (along with evolving engagement metrics) and live cryptocurrency market data are not being captured on our side.

At the same time, the project needs to start in a way that:
1. Avoids heavyweight upfront infrastructure (Azure VM setup, Dagster deployment, DWH databases) so we can **immediately run reliable data collection locally on the author's machine**.
2. Establishes a **clean, modular single repository** from day one (without Git submodules or Cruft) that stays easy to navigate as the codebase grows to include the ETL layer, a third data source, and 1–2 AI modules.
3. Remains trivially portable (Docker / Compose + externalized storage configuration) so moving the workload onto an Azure VM and/or switching local storage to cloud object storage requires configuration changes rather than rewriting ingestion code.

---

## 2. Goals, Non-Goals, and Anti-Goals

### Goals
- **Working collection immediately:** one unified `ingestion` module pulling Donald Trump's Truth Social posts and Binance candlestick data, converting incoming JSON **on the fly into Parquet files** in the `bronze` layer.
- **Zero lost increments or state:** repeatable, idempotent-safe runs that write new batches without overwriting historical data and never persist still-open (unclosed) market candles.
- **Externalized configuration for key variables:**
  - Binance candle interval (default `1m`, easily switched to `5m` via configuration),
  - target trading pairs (default basket from project decisions: `BTCUSDT`, `ETHUSDT`, `SOLUSDT`, `XRPUSDT`, `TRUMPUSDT`),
  - storage destination (local directory now, cloud URI/bucket later through a storage abstraction).
- **Ready for expansion:**
  - clean internal connector contract in `ingestion` so a **third data source** drops in without restructuring,
  - repository skeleton with clear homes for the **ETL layer** and **1–2 AI modules**,
  - lightweight `Dockerfile` + `docker-compose.yml` to run ingestion consistently locally or on Azure.

### Non-Goals (Out of Scope for Part 1)
- **Full historical backfill from 2022** (`data.binance.vision` `.zip` archives covering 2022–2026) — deferred as optional; immediate priority is capturing forward data from now on.
- **ETL / Data Warehouse implementation** (Silver/Gold layers, `dbt`, `DuckDB` star schema, `Great Expectations`, `Dagster`) — only skeleton placeholders are created in Part 1.
- **AI / ML model implementation** (Laya/ModernBERT NLP, vision/multimodal models) — skeleton placeholders only.
- **Production Azure deployment** — portability is built in, while provisioning and deploying the Azure VM happens separately.

### Anti-Goals
- **Splitting into multiple repositories, Git submodules, or Cruft templates** — one developer will write most of the code, and splitting creates unnecessary Docker wiring and cross-repo schema synchronization overhead.
- **Cleaning or filtering records in Bronze** — Bronze must act as a faithful raw landing zone preserving source payloads so downstream Silver/Gold models can always reprocess from source truth.
- **Persisting unclosed (in-progress) candles** as finalized Binance records.

---

## 3. Constraints and Live-Verified Source Facts (2026-11-18)

### Truth Social (`https://ix.cnn.io/data/truth-social/truth_archive.json`)
- Endpoint returns HTTP 200, gzip-compressed (`~5.1 MB` compressed, `~21.6 MB` JSON), with `ETag` and `Last-Modified` headers (`Cache-Control: max-age=0, no-store`).
- Currently contains `38,078` unique records in a JSON array (`2022-02-14` through `2026-11-18`).
- Keys per record:
  - `id` (`str`), `created_at` (`str` ISO-8601 UTC), `content` (`str`), `url` (`str`),
  - `media` (`list[str]` — list of media URLs, empty or populated),
  - `replies_count`, `reblogs_count`, `favourites_count` (`int | None` — **9 records had `null`**, so the Parquet schema must allow nullable integers for engagement counts).
- Fresh posts may initially appear in the archive with zero/low engagement counts that update in later archive refreshes.

### Binance Public Market Data (`/api/v3/klines`)
- Both `https://api.binance.com` and `https://data-api.binance.vision` return HTTP 200 with status `TRADING` for all 5 pairs (`BTCUSDT`, `ETHUSDT`, `SOLUSDT`, `XRPUSDT`, `TRUMPUSDT`).
- `https://data-api.binance.vision` is Binance's official public market-data endpoint and avoids regional IP blocks (HTTP 451) that affect `api.binance.com` on some cloud VMs (e.g., US regions).
- `/api/v3/klines` returns an array of 12-element positional arrays:
  `[open_time_ms, open, high, low, close, volume, close_time_ms, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore]`.
- Request weight is `2` (public IP limit: `6000` weight/min), returning up to `1000` candles per call. Without an `endTime` bound, the trailing candle can still be open (`close_time_ms >= now_ms`).

---

## 4. Repository Architecture (`Projekty/trump-quant`)

We use **one flat, modular repository** managed with **`uv`** (no Git submodules, no Cruft, and no extra `packages/` or `src/` nesting layers):

```text
trump-quant/
├── pyproject.toml              # Project metadata + dependency extras (ingestion, etl, ai)
├── docker-compose.yml          # One-command container execution (starting with ingestion)
├── .env.example                # Environment variable template (storage URI, interval, symbols, etc.)
├── README.md                   # Local and Docker usage instructions
├── data/                       # Default local data directory (gitignored)
│   └── bronze/
├── ingestion/                  # [PART 1 — IMPLEMENTED NOW] Data ingestion into Bronze
│   ├── __init__.py
│   ├── Dockerfile              # Lightweight image installing only the `ingestion` dependency extra
│   ├── config.py               # Settings loader (pydantic-settings / env)
│   ├── storage.py              # Parquet + state storage abstraction (local now, cloud URI/fsspec later)
│   ├── state.py                # Incremental watermark & ETag state management
│   ├── sources/
│   │   ├── __init__.py
│   │   ├── base.py             # Shared source connector contract (ready for 3rd data source)
│   │   ├── truth_social.py
│   │   └── binance.py
│   └── cli.py                  # CLI entrypoint (`run` once / continuous `loop`)
├── etl/                        # [SKELETON] ETL / DuckDB / dbt / Great Expectations / orchestration
│   └── README.md
├── ai/                         # [SKELETON] AI layer (room for 1–2 independent modules/models, e.g., NLP & vision)
│   └── README.md
├── tests/                      # Automated tests (starting with tests/ingestion/)
│   └── ingestion/
└── docs/
    └── superpowers/specs/      # Design and architecture decision documents
```

**Why this structure wins long-term:**
- Flat top-level folders (`ingestion/`, `etl/`, `ai/`) keep navigation effortless for both the primary developer and teammates, and allow future orchestration in `etl/` to invoke `ingestion` directly without multi-package editable install friction.
- Dependencies live in one `pyproject.toml` split by optional extras (`ingestion`, `etl`, `ai`), so the `ingestion` Docker image remains lightweight without pulling `dbt` or `torch`.
- Adding the upcoming 3rd data source only requires adding a connector module under `ingestion/sources/` implementing `base.py`.
- If `ai/` later introduces heavy or conflicting GPU/CUDA dependencies (`torch`, `transformers`) relative to `etl/`, `ai/` alone can be split into its own standalone `pyproject.toml` or marked via `[tool.uv] conflicts` without disrupting `ingestion/` or `etl/`.

---

## 5. Ingestion Module Design (`ingestion/` — Part 1)

### 5.1. Configuration and Storage Abstraction (`config.py`, `storage.py`)
- Configuration loaded from environment variables / `.env` with sensible defaults:
  - `STORAGE_URI` (default local path `./data/bronze`, designed for `fsspec`-compatible cloud URIs such as `az://...` or `s3://...` later),
  - `BINANCE_BASE_URL` (default `https://data-api.binance.vision`),
  - `BINANCE_SYMBOLS` (default `BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,TRUMPUSDT`),
  - `BINANCE_INTERVAL` (default `1m`; supports Binance intervals such as `1m`, `5m`),
  - `BINANCE_INITIAL_LOOKBACK_HOURS` (default `24` — initial catch-up window on clean start instead of a multi-year backfill),
  - `TRUTH_SOCIAL_URL` (default `https://ix.cnn.io/data/truth-social/truth_archive.json`),
  - `POLL_INTERVAL_SECONDS` (interval for continuous `loop` mode, e.g. `300`).
- **On-the-fly JSON → Parquet:** parsed JSON payloads are converted directly in memory to `pyarrow` tables and written as compressed Parquet (`zstd` or `snappy`) via `storage.py` using safe/atomic writes.

### 5.2. Truth Social Connector (`truth_social.py`)
- Queries the CNN archive with conditional HTTP headers (`If-None-Match` using stored `ETag`, `If-Modified-Since` using stored `Last-Modified`) so frequent runs avoid re-downloading and duplicating `~21 MB` when unchanged (`HTTP 304 Not Modified`).
- When changed (`HTTP 200`):
  - converts records to Parquet in memory and adds ingestion metadata (`ingested_at` UTC, `source_etag`),
  - preserves all raw source fields faithfully, keeping `replies_count`, `reblogs_count`, `favourites_count` as nullable integers and `media` as `list<string>`,
  - writes an **immutable Parquet snapshot** partitioned by ingestion date (e.g., `truth_social/snapshot_date=YYYY-MM-DD/snapshot_<timestamp>.parquet`).
  - *Rationale:* Immutable snapshots capture new posts, engagement metric growth over time, and any later edits or deletions without losing history; downstream Silver models can deduplicate or extract deltas by `(id, ingested_at)`.

### 5.3. Binance Connector (`binance.py`)
- For each pair in `BINANCE_SYMBOLS` and configured `BINANCE_INTERVAL`:
  - Reads the latest persisted closed-candle `watermark` from `state.py` (stored under `STORAGE_URI` alongside the data so state travels with storage if moved to Azure).
  - If no watermark exists (first run): starts from `now - BINANCE_INITIAL_LOOKBACK_HOURS`, aligned to the interval boundary.
  - Bounds the fetch window to the **latest fully closed candle** (`close_time_ms < now_ms`), paginating up to `1000` candles per request via `startTime` until caught up.
  - Writes raw candle columns on the fly to Parquet (preserving source precision without lossy rounding, plus `symbol`, `interval`, `ingested_at`), partitioned cleanly (e.g., `binance/interval=<interval>/symbol=<symbol>/date=YYYY-MM-DD/<batch>.parquet`).
  - Advances the `watermark` only after Parquet write succeeds.
  - Handles transient HTTP errors and rate limits gracefully (backoff on HTTP 429/5xx).

### 5.4. Extensible Connector Contract (`sources/base.py`)
- Simple connector protocol/interface (e.g., `name`, `run(storage, state, config) -> IngestionResult`) registered in a central source registry in `cli.py`, making the future 3rd data source a drop-in addition under `ingestion/sources/`.

### 5.5. Local Execution and Docker Portability
- **Local via `uv`:**
  - single-shot execution for all or one source (`uv run ingest run [--source all|truth_social|binance]`),
  - continuous polling mode (`uv run ingest loop`) running on a configurable interval with clean logging and graceful shutdown (`SIGINT`/`SIGTERM`), ready to run directly on macOS or via cron/`launchd` without requiring Dagster yet.
- **Via Docker / `docker-compose.yml`:**
  - Lightweight `ingestion` service running `ingest loop` with a persistent volume mounted for `./data` and `.env` configuration, enabling one-command deployment (`docker compose up -d`) both locally and later on an Azure VM.

---

## 6. Design Calls Made on Your Behalf ("Our Calls")

The following defaults were chosen to fill details not explicitly specified in discussion:
1. **Flat repository structure (`ingestion/`, `etl/`, `ai/` at root) + single `pyproject.toml` using `uv` dependency extras** — avoids deep nesting upfront while allowing `ai/` to split out later if GPU dependencies conflict.
2. **No Dagster in Part 1** — `ingestion` exposes a lightweight CLI (`run` / `loop`), which `etl/` can later wrap as Dagster assets/jobs.
3. **Default Binance host set to `https://data-api.binance.vision`** (overridable via `.env`) to avoid potential regional cloud IP blocks (HTTP 451).
4. **Incremental state (`state.py`) stored inside `STORAGE_URI`** alongside Bronze Parquet files so state moves seamlessly when switching to Azure/cloud storage.
5. **Immutable Truth Social snapshots gated by `ETag`/`Last-Modified`** — writes full compressed Parquet snapshots (~a few MB) on change rather than computing Bronze-level diffs, preserving engagement evolution cleanly.
6. **Initial Binance lookback window of `24` hours (`BINANCE_INITIAL_LOOKBACK_HOURS=24`)** on clean first start while historical backfill remains deferred.
7. **In-memory JSON → Parquet conversion via `pyarrow` + `fsspec`** with no intermediate `.json` files persisted to disk.

---

## 7. Implementation Details Left to the Builder

The implementation plan will resolve the remaining concrete engineering details:
- exact Arrow/Parquet column types (including Bronze representation of Binance decimal strings: `string` vs `decimal128` vs `float64`),
- exact JSON schema and paths for state files under `STORAGE_URI/_state/`,
- exact starter contents of `etl/` and `ai/` skeleton directories (`README.md`, minimal `__init__.py` files),
- automated unit tests (mocked HTTP payloads for Truth Social including the 9 `null` engagement records, mocked Binance responses verifying watermark progression and exclusion of unclosed candles, on-the-fly Parquet validation),
- concrete contents of `Dockerfile`, `docker-compose.yml`, `.gitignore`, and `README.md`.
