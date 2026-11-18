# Repository Skeleton & Part 1 Data Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Initialize the flat `PDF` (`President Driven Finances`) monorepo skeleton (`ingestion/`, `etl/`, `ai/`), implement Part 1 incremental JSON-to-Bronze-Parquet ingestion for Truth Social and Binance, and package it with `uv` and Docker for local and Azure portability.

**Architecture:** One flat Python repository managed via a single root `pyproject.toml` with optional dependency extras (`ingestion`, `etl`, `ai`). Part 1 implements `ingestion/` with `pydantic-settings` config, an `fsspec` + `pyarrow` storage/state layer, a connector protocol in `sources/base.py` hosting `truth_social` and `binance`, and a CLI (`ingest run` / `ingest loop`) wrapped in a lightweight `ingestion/Dockerfile` + `docker-compose.yml`.

**Tech Stack:** Python `>=3.12` (verified `3.13.2` locally), `uv`, `httpx`, `pyarrow`, `fsspec`, `pydantic-settings`, `pytest`, `ruff`, Docker / Docker Compose.

**Spec:** [`docs/superpowers/specs/2026-11-18-ingestion-and-repo-design.md`](../specs/2026-11-18-ingestion-and-repo-design.md)

## Global Constraints

- Flat repo layout at repo root (`PDF/`): `ingestion/`, `etl/`, `ai/`, `tests/`, `docs/` directly at root (no `packages/`, no `src/`, no Git submodules, no Cruft).
- Single root `pyproject.toml` using `[project.optional-dependencies]` (`ingestion`, `etl`, `ai`) and `[dependency-groups]` (`dev`), so `ingestion/Dockerfile` installs only `--extra ingestion` (`--no-dev`).
- Bronze Parquet preserves source payloads without cleaning or lossy numeric rounding; Truth Social `replies_count`, `reblogs_count`, `favourites_count` MUST be nullable integers (`pa.int64()`), `media` MUST be `pa.list_(pa.string())`.
- Binance default host: `https://data-api.binance.vision`; default symbols: `BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,TRUMPUSDT`; default interval: `1m` (configurable to `5m` etc.); default initial lookback: `24` hours; never persist still-open candles (`close_time_ms >= now_ms`).
- State files (`ETag`/`Last-Modified` and per-symbol Binance watermarks) live inside `STORAGE_URI/_state/`, not outside storage.
- `etl/` and `ai/` are skeleton placeholders only (`__init__.py` + `README.md`), no ETL/ML logic implemented in Part 1.
- Minimal clear logging via standard Python `logging` on logger `"ingestion"` (`%(asctime)s %(levelname)s [%(name)s] %(message)s`), persisted to a rotating log file (`LOG_FILE`, default `./data/logs/ingestion.log`, `RotatingFileHandler` `5 MB × 3` backups) plus stderr stream (for `docker compose logs`): every fetch failure or retry MUST log source, target symbol/URL, HTTP status + short response body snippet (when available) or exception type/message so failures are immediately diagnosable.

## Review Focus

1. **Corrupt/empty Truth Social responses or missing `ETag`/`Last-Modified` headers:** Missing conditional headers must still write snapshots without crashing, and HTTP errors or malformed/empty-non-list JSON must never overwrite saved state (`Task 3`).
2. **Comma-separated `BINANCE_SYMBOLS` env strings and invalid `BINANCE_INTERVAL`:** `pydantic-settings` must parse `"BTCUSDT,ETHUSDT"` env values cleanly without JSON-decode failure, and invalid intervals like `"2m"` or `"bad"` must fail fast at config load (`Task 2`).
3. **Binance UTC midnight boundary, multi-page >1000 kline pagination, and zero-trade flat candles:** Closed candles spanning UTC midnight must split into separate `date=YYYY-MM-DD` Parquet partitions, `volume="0.00000000"` candles must be retained, and trailing open candles (`close_time_ms >= now_ms`) must be dropped (`Task 4`).
4. **Partial symbol failure isolation:** If one Binance pair fails after retries, already-succeeded pairs keep their Parquet files and watermarks, the failed pair does not advance its watermark, `ingest run` exits non-zero, and `ingest loop` logs and survives to the next poll cycle (`Task 4`, `Task 5`).
5. **Atomic Parquet writes and `file://` URI support:** Interrupted writes must clean up staging/temp files and never leave half-written `.parquet` artifacts at the target path, supporting both local paths and `file://` URIs via `fsspec` (`Task 2`).

---

## Planned File Structure

| File | Responsibility |
| :--- | :--- |
| `.gitignore` | Exclude `data/`, `.env`, `.venv/`, caches, build artifacts |
| `.env.example` | Template for `STORAGE_URI`, Binance settings, Truth Social URL, poll interval |
| `pyproject.toml` | `uv` project config, `ingest` script entrypoint, `ingestion`/`etl`/`ai` extras, `dev` group, `pytest`/`ruff` config |
| `ingestion/__init__.py` | Ingestion package marker |
| `etl/__init__.py`, `etl/README.md` | Skeleton placeholder for future DuckDB/dbt/Great Expectations/Dagster layer |
| `ai/__init__.py`, `ai/README.md` | Skeleton placeholder for 1–2 future AI/ML modules |
| `ingestion/config.py` | `IngestionSettings`, env parsing, `interval_to_ms()` validator |
| `ingestion/storage.py` | `BronzeStorage` over `fsspec` + `pyarrow` with atomic Parquet writes & byte state I/O |
| `ingestion/state.py` | `StateStore` JSON persistence under `STORAGE_URI/_state/` |
| `ingestion/sources/__init__.py` | Sources subpackage |
| `ingestion/sources/base.py` | `IngestionResult` dataclass and `SourceConnector` protocol |
| `ingestion/sources/truth_social.py` | `TruthSocialConnector` conditional GET, Arrow schema, immutable Bronze snapshot writer |
| `ingestion/sources/binance.py` | `BinanceConnector` paginated `/api/v3/klines` fetch, closed-candle filter, retry/backoff, date partitioning |
| `ingestion/cli.py` | Source registry, `run_once()`, `run_loop()`, `SIGINT`/`SIGTERM` handling, `ingest` CLI |
| `ingestion/Dockerfile` | Slim Python 3.13 + `uv` image installing `--extra ingestion` only |
| `docker-compose.yml` | Portable `ingestion` service mounting `./data` and reading `.env` |
| `README.md` | Local `uv` and Docker usage instructions |
| `tests/ingestion/...` | Unit & integration tests with `httpx.MockTransport` and `tmp_path` storage |

---

### Task 1: Git Initialization, Flat Repo Skeleton, and `uv` Baseline

**Files:**
- Create: `.gitignore`, `.env.example`, `pyproject.toml`, `ingestion/__init__.py`, `etl/__init__.py`, `etl/README.md`, `ai/__init__.py`, `ai/README.md`, `tests/__init__.py`, `tests/ingestion/test_skeleton.py`

**Interfaces:**
- Produces: top-level Python packages `ingestion`, `etl`, `ai` importable via `uv run` and `pytest`; committed English spec at `docs/superpowers/specs/2026-11-18-ingestion-and-repo-design.md`.

- [ ] **Step 1: Initialize Git repository in `PDF` and commit the approved design docs**

Run in repo root (`PDF/`):
```bash
git init
```
Create `.gitignore` ignoring `data/`, `.env`, `.venv/`, `__pycache__/`, `.pytest_cache/`, `.ruff_cache/`, `*.egg-info/`, `.DS_Store`.

- [ ] **Step 2: Write failing skeleton test `tests/ingestion/test_skeleton.py`**

```python
import importlib
from pathlib import Path


def test_top_level_packages_importable_and_skeleton_files_exist():
    for pkg in ("ingestion", "etl", "ai"):
        mod = importlib.import_module(pkg)
        assert mod is not None
    root = Path(__file__).resolve().parents[2]
    assert (root / "etl" / "README.md").is_file()
    assert (root / "ai" / "README.md").is_file()
    assert (root / ".env.example").is_file()
```

- [ ] **Step 3: Run pytest to verify failure before creating packages**

Run: `uv run pytest tests/ingestion/test_skeleton.py -v` (or `python3 -m pytest` if `pyproject.toml` missing)
Expected: FAIL (no `pyproject.toml` / packages missing).

- [ ] **Step 4: Create `pyproject.toml`, `.env.example`, package markers, and skeleton READMEs**

Configure `pyproject.toml`:
- `requires-python = ">=3.12"`
- `[project.optional-dependencies]`:
  - `ingestion = ["httpx>=0.27", "pyarrow>=18.0", "pydantic-settings>=2.6", "fsspec>=2024.10"]`
  - `etl = []`
  - `ai = []`
- `[dependency-groups] dev = ["pytest>=8.3", "ruff>=0.8"]`
- `[project.scripts] ingest = "ingestion.cli:main"`
- `[build-system]` using `hatchling` with `[tool.hatch.build.targets.wheel] packages = ["ingestion", "etl", "ai"]`
- `[tool.pytest.ini_options] pythonpath = ["."], testpaths = ["tests"]` and `[tool.ruff] line-length = 100`.

Create `.env.example` documenting `STORAGE_URI=./data/bronze`, `LOG_FILE=./data/logs/ingestion.log`, `BINANCE_BASE_URL=https://data-api.binance.vision`, `BINANCE_SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,TRUMPUSDT`, `BINANCE_INTERVAL=1m`, `BINANCE_INITIAL_LOOKBACK_HOURS=24`, `TRUTH_SOCIAL_URL=https://ix.cnn.io/data/truth-social/truth_archive.json`, `POLL_INTERVAL_SECONDS=300`.

Create `ingestion/__init__.py`, `etl/__init__.py`, `etl/README.md` (documenting future DuckDB/dbt/Great Expectations/Dagster role), `ai/__init__.py`, `ai/README.md` (documenting space for 1–2 future NLP/multimodal/ML modules), `tests/__init__.py`.

- [ ] **Step 5: Sync environment and run skeleton test**

Run: `uv sync --extra ingestion && uv run pytest tests/ingestion/test_skeleton.py -v`
Expected: PASS (`1 passed`) and `uv.lock` created.

- [ ] **Step 6: Commit**

```bash
git add .gitignore .env.example pyproject.toml uv.lock docs/ ingestion/__init__.py etl/ ai/ tests/
git commit -m "chore: initialize flat PDF repo skeleton and ingestion env"
```

---

### Task 2: Configuration (`config.py`), Storage Abstraction (`storage.py`), and State Store (`state.py`)

**Files:**
- Create: `ingestion/config.py`, `ingestion/storage.py`, `ingestion/state.py`, `tests/ingestion/test_config_storage_state.py`

**Interfaces:**
- Consumes: `pydantic-settings`, `fsspec`, `pyarrow`, `pyarrow.parquet`.
- Produces:
  - `interval_to_ms(interval: str) -> int`
  - `class IngestionSettings(BaseSettings)` with fields: `storage_uri: str`, `log_file: str | None`, `binance_base_url: str`, `binance_symbols: list[str]`, `binance_interval: str`, `binance_initial_lookback_hours: int`, `truth_social_url: str`, `poll_interval_seconds: int`, `http_timeout_seconds: float`
  - `class BronzeStorage(storage_uri: str)` with methods: `write_parquet(relative_path: str, table: pa.Table) -> str`, `read_bytes(relative_path: str) -> bytes | None`, `write_bytes(relative_path: str, data: bytes) -> None`
  - `class StateStore(storage: BronzeStorage)` with methods: `get_json(key: str) -> dict[str, Any] | None`, `put_json(key: str, payload: dict[str, Any]) -> None`

- [ ] **Step 1: Write failing tests in `tests/ingestion/test_config_storage_state.py`**

```python
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pydantic import ValidationError

from ingestion.config import IngestionSettings, interval_to_ms
from ingestion.state import StateStore
from ingestion.storage import BronzeStorage


def test_settings_parses_comma_separated_symbols_and_rejects_invalid_interval(monkeypatch):
    monkeypatch.setenv("BINANCE_SYMBOLS", "BTCUSDT, ETHUSDT ,SOLUSDT")
    monkeypatch.setenv("BINANCE_INTERVAL", "5m")
    settings = IngestionSettings(_env_file=None)
    assert settings.binance_symbols == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    assert settings.binance_interval == "5m"
    assert interval_to_ms("1m") == 60_000
    assert interval_to_ms("5m") == 300_000

    monkeypatch.setenv("BINANCE_INTERVAL", "2m")
    with pytest.raises(ValidationError):
        IngestionSettings(_env_file=None)


def test_bronze_storage_atomic_write_and_file_uri(tmp_path, monkeypatch):
    table = pa.table({"id": pa.array(["1", "2"], type=pa.string())})
    for uri in (str(tmp_path / "bronze"), f"file://{tmp_path / 'bronze_uri'}"):
        storage = BronzeStorage(uri)
        written = storage.write_parquet("sample/part-0.parquet", table)
        assert written.endswith("sample/part-0.parquet")
        read_back = pq.read_table(written.removeprefix("file://"))
        assert read_back.to_pydict() == {"id": ["1", "2"]}

    # Simulate failure inside pq.write_table and ensure no corrupt target or leftover tmp file remains
    storage = BronzeStorage(str(tmp_path / "bronze_fail"))
    monkeypatch.setattr(pq, "write_table", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("disk full")))
    with pytest.raises(RuntimeError):
        storage.write_parquet("sample/bad.parquet", table)
    target_dir = tmp_path / "bronze_fail" / "sample"
    assert not any(target_dir.glob("*")) if target_dir.exists() else True


def test_state_store_roundtrip_inside_storage_uri(tmp_path):
    storage = BronzeStorage(str(tmp_path / "bronze"))
    store = StateStore(storage)
    assert store.get_json("binance_BTCUSDT_1m") is None
    store.put_json("binance_BTCUSDT_1m", {"last_closed_open_time_ms": 1700000000000})
    assert store.get_json("binance_BTCUSDT_1m") == {"last_closed_open_time_ms": 1700000000000}
    assert (tmp_path / "bronze" / "_state" / "binance_BTCUSDT_1m.json").is_file()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/ingestion/test_config_storage_state.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ingestion.config'`.

- [ ] **Step 3: Implement `ingestion/config.py`**

Implement `SUPPORTED_BINANCE_INTERVALS` mapping (`1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d` to milliseconds), `interval_to_ms(interval: str) -> int`, and `IngestionSettings(BaseSettings)` with `model_config = SettingsConfigDict(env_file=".env", extra="ignore")` (no env prefix, so `STORAGE_URI`, `BINANCE_SYMBOLS`, `BINANCE_INTERVAL`, etc. map directly). Use `Annotated[list[str], NoDecode]` + `@field_validator("binance_symbols", mode="before")` so both `"BTCUSDT,ETHUSDT"` and JSON arrays parse cleanly, and validate `binance_interval` via `interval_to_ms`.

- [ ] **Step 4: Implement `ingestion/storage.py` and `ingestion/state.py`**

In `BronzeStorage(storage_uri: str)`:
- Resolve `(fs, base_path)` via `fsspec.core.url_to_fs(storage_uri.rstrip("/"))`.
- Ensure parent dirs exist (`fs.makedirs(parent, exist_ok=True)`).
- Write Parquet via a temporary sibling file `<target>.<uuid>.tmp` written with `pq.write_table(..., compression="zstd")` and moved/renamed (`fs.mv(tmp_path, target_path)`) in a `try/except` block that removes the temp file on failure. Same atomic temp+move pattern for `write_bytes()`.
- `read_bytes()` returns `None` when `not fs.exists(target_path)`.

In `StateStore(storage: BronzeStorage)`:
- Read/write UTF-8 JSON under `f"_state/{key}.json"`.

- [ ] **Step 5: Run tests to verify pass**

Run: `uv run pytest tests/ingestion/test_config_storage_state.py -v`
Expected: PASS (`3 passed`).

- [ ] **Step 6: Commit**

```bash
git add ingestion/config.py ingestion/storage.py ingestion/state.py tests/ingestion/test_config_storage_state.py
git commit -m "feat(ingestion): add config, atomic fsspec parquet storage, and state store"
```

---

### Task 3: Connector Protocol (`sources/base.py`) and Truth Social Connector (`sources/truth_social.py`)

**Files:**
- Create: `ingestion/sources/__init__.py`, `ingestion/sources/base.py`, `ingestion/sources/truth_social.py`, `tests/ingestion/test_truth_social.py`

**Interfaces:**
- Consumes: `IngestionSettings`, `BronzeStorage`, `StateStore`.
- Produces:
  - `IngestionResult` dataclass (`source: str`, `status: Literal["success", "skipped", "partial_failure", "failed"]`, `records_written: int`, `files_written: list[str]`, `details: dict[str, Any]`)
  - `SourceConnector` `Protocol` (`name: str`, `run(storage: BronzeStorage, state: StateStore, settings: IngestionSettings, client: httpx.Client, now_fn: Callable[[], datetime] | None = None) -> IngestionResult`)
  - `TRUTH_SOCIAL_ARROW_SCHEMA: pa.Schema`
  - `TruthSocialConnector` (`name = "truth_social"`)

- [ ] **Step 1: Write failing tests in `tests/ingestion/test_truth_social.py`**

```python
import json
from datetime import UTC, datetime
import httpx
import pyarrow.parquet as pq
import pytest

from ingestion.config import IngestionSettings
from ingestion.sources.truth_social import TruthSocialConnector
from ingestion.state import StateStore
from ingestion.storage import BronzeStorage


SAMPLE_POSTS = [
    {
        "id": "117634741427255539",
        "created_at": "2026-11-18T01:01:45.859Z",
        "content": "Hello",
        "url": "https://truthsocial.com/@realDonaldTrump/117634741427255539",
        "media": ["https://example.com/a.jpg"],
        "replies_count": 586,
        "reblogs_count": 1622,
        "favourites_count": 6329,
    },
    {
        "id": "107797156496908384",
        "created_at": "2022-02-14T15:54:32.528Z",
        "content": "",
        "url": "https://truthsocial.com/@realDonaldTrump/107797156496908384",
        "media": [],
        "replies_count": None,
        "reblogs_count": None,
        "favourites_count": None,
    },
]


def test_truth_social_writes_parquet_with_nulls_and_skips_304(tmp_path):
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(dict(request.headers))
        if request.headers.get("if-none-match") == '"etag-1"':
            return httpx.Response(304)
        return httpx.Response(
            200,
            json=SAMPLE_POSTS,
            headers={"ETag": '"etag-1"', "Last-Modified": "Wed, 18 Nov 2026 05:01:49 GMT"},
        )

    storage = BronzeStorage(str(tmp_path / "bronze"))
    state = StateStore(storage)
    settings = IngestionSettings(_env_file=None, storage_uri=str(tmp_path / "bronze"))
    connector = TruthSocialConnector()
    fixed_now = lambda: datetime(2026, 11, 18, 7, 0, 0, tzinfo=UTC)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        res1 = connector.run(storage, state, settings, client, now_fn=fixed_now)
        res2 = connector.run(storage, state, settings, client, now_fn=fixed_now)

    assert res1.status == "success"
    assert res1.records_written == 2
    assert len(res1.files_written) == 1
    assert "truth_social/snapshot_date=2026-11-18/snapshot_20261118T070000Z.parquet" in res1.files_written[0]

    table = pq.read_table(res1.files_written[0])
    assert table.column("replies_count").to_pylist() == [586, None]
    assert table.column("media").to_pylist() == [["https://example.com/a.jpg"], []]
    assert table.column("source_etag").to_pylist() == ['"etag-1"', '"etag-1"']

    assert res2.status == "skipped"
    assert res2.records_written == 0
    assert calls[1].get("if-none-match") == '"etag-1"'
    assert calls[1].get("if-modified-since") == "Wed, 18 Nov 2026 05:01:49 GMT"


def test_truth_social_missing_etag_and_corrupt_response_preserves_state(tmp_path, caplog):
    storage = BronzeStorage(str(tmp_path / "bronze"))
    state = StateStore(storage)
    state.put_json("truth_social", {"etag": '"prev"', "last_modified": "Tue, 17 Nov 2026 00:00:00 GMT"})
    settings = IngestionSettings(_env_file=None, storage_uri=str(tmp_path / "bronze"))
    connector = TruthSocialConnector()
    fixed_now = lambda: datetime(2026, 11, 18, 8, 0, 0, tzinfo=UTC)

    # HTTP 500 must log failure context and preserve saved state
    with caplog.at_level("ERROR"):
        with httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(502, text="bad gateway"))) as c:
            with pytest.raises(httpx.HTTPStatusError):
                connector.run(storage, state, settings, c, now_fn=fixed_now)
    assert "502" in caplog.text and "truth_social" in caplog.text
    assert state.get_json("truth_social")["etag"] == '"prev"'

    # Corrupt non-list JSON must fail without overwriting saved state
    with httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"error": "bad"}))) as c:
        with pytest.raises(ValueError):
            connector.run(storage, state, settings, c, now_fn=fixed_now)
    assert state.get_json("truth_social")["etag"] == '"prev"'

    # Valid 200 without ETag/Last-Modified must still write snapshot cleanly
    with httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(200, json=SAMPLE_POSTS))) as c:
        res = connector.run(storage, state, settings, c, now_fn=fixed_now)
    assert res.status == "success"
    assert res.records_written == 2
    assert pq.read_table(res.files_written[0]).column("source_etag").to_pylist() == [None, None]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/ingestion/test_truth_social.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ingestion.sources'`.

- [ ] **Step 3: Implement `ingestion/sources/__init__.py` and `ingestion/sources/base.py`**

Define `IngestionResult` dataclass and `SourceConnector` `Protocol`.

- [ ] **Step 4: Implement `ingestion/sources/truth_social.py`**

Define explicit `TRUTH_SOCIAL_ARROW_SCHEMA` (`id`, `created_at`, `content`, `url`, `media`, `replies_count`, `reblogs_count`, `favourites_count`, `ingested_at`, `source_etag`). Send `If-None-Match` and `If-Modified-Since` if present in `"truth_social"` state. Log HTTP/transport/JSON validation errors via `logger.error("truth_social fetch failed ...", ...)` including status code, URL, and short body snippet before re-raising. Handle `304` -> `skipped`; validate `isinstance(payload, list)`; build Arrow table via column arrays using the explicit schema; write to `truth_social/snapshot_date={YYYY-MM-DD}/snapshot_{YYYYMMDDTHHMMSSZ}.parquet`; persist updated `"truth_social"` state only after successful Parquet write.

- [ ] **Step 5: Run tests to verify pass**

Run: `uv run pytest tests/ingestion/test_truth_social.py -v`
Expected: PASS (`2 passed`).

- [ ] **Step 6: Commit**

```bash
git add ingestion/sources/__init__.py ingestion/sources/base.py ingestion/sources/truth_social.py tests/ingestion/test_truth_social.py
git commit -m "feat(ingestion): add connector protocol and Truth Social snapshot connector"
```

---

### Task 4: Binance Connector (`sources/binance.py`)

**Files:**
- Create: `ingestion/sources/binance.py`, `tests/ingestion/test_binance.py`

**Interfaces:**
- Consumes: `IngestionSettings`, `BronzeStorage`, `StateStore`, `interval_to_ms`, `IngestionResult`, `SourceConnector`.
- Produces:
  - `BINANCE_KLINE_ARROW_SCHEMA: pa.Schema`
  - `BinanceConnector` (`name = "binance"`)

- [ ] **Step 1: Write failing tests in `tests/ingestion/test_binance.py`**

```python
from datetime import UTC, datetime
import httpx
import pyarrow.parquet as pq

from ingestion.config import IngestionSettings
from ingestion.sources.binance import BinanceConnector
from ingestion.state import StateStore
from ingestion.storage import BronzeStorage


def _kline(open_ms: int, interval_ms: int = 60_000, vol: str = "10.50000000", trades: int = 12):
    return [
        open_ms,
        "100.00000000",
        "101.00000000",
        "99.50000000",
        "100.25000000",
        vol,
        open_ms + interval_ms - 1,
        "1050.00000000",
        trades,
        "5.00000000",
        "500.00000000",
        "0",
    ]


def test_binance_pagination_midnight_split_excludes_unclosed_and_keeps_zero_volume(tmp_path):
    # now = 2026-11-18T00:02:30Z -> 00:00 and 00:01 are closed; 00:02 is still open
    now_dt = datetime(2026, 11, 18, 0, 2, 30, tzinfo=UTC)
    t_prev_day = int(datetime(2026, 11, 17, 23, 59, 0, tzinfo=UTC).timestamp() * 1000)
    t_mid_0 = t_prev_day + 60_000
    t_mid_1 = t_mid_0 + 60_000
    t_open = t_mid_1 + 60_000  # close_time = 00:02:59.999 >= now_ms

    storage = BronzeStorage(str(tmp_path / "bronze"))
    state = StateStore(storage)
    state.put_json("binance_BTCUSDT_1m", {"last_closed_open_time_ms": t_prev_day - 60_000})
    settings = IngestionSettings(
        _env_file=None,
        storage_uri=str(tmp_path / "bronze"),
        binance_symbols=["BTCUSDT"],
        binance_interval="1m",
    )

    page1 = [_kline(t_prev_day), _kline(t_mid_0)]
    page2 = [_kline(t_mid_1, vol="0.00000000", trades=0), _kline(t_open)]
    req_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal req_count
        req_count += 1
        start_time = int(request.url.params["startTime"])
        if start_time == t_prev_day:
            return httpx.Response(200, json=page1)
        return httpx.Response(200, json=page2)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        res = BinanceConnector(max_limit=2).run(storage, state, settings, client, now_fn=lambda: now_dt)

    assert req_count == 2
    assert res.status == "success"
    assert res.records_written == 3
    assert len(res.files_written) == 2
    assert any("date=2026-11-17" in f for f in res.files_written)
    assert any("date=2026-11-18" in f for f in res.files_written)

    day2_file = [f for f in res.files_written if "date=2026-11-18" in f][0]
    day2_table = pq.read_table(day2_file)
    assert day2_table.column("open_time_ms").to_pylist() == [t_mid_0, t_mid_1]
    assert day2_table.column("volume").to_pylist() == ["10.50000000", "0.00000000"]
    assert day2_table.column("number_of_trades").to_pylist() == [12, 0]
    assert state.get_json("binance_BTCUSDT_1m")["last_closed_open_time_ms"] == t_mid_1


def test_binance_isolated_symbol_failure_preserves_watermarks_and_retries_429(tmp_path, caplog):
    now_dt = datetime(2026, 11, 18, 1, 5, 10, tzinfo=UTC)
    interval_ms = 300_000  # 5m
    t0 = int(datetime(2026, 11, 18, 0, 55, 0, tzinfo=UTC).timestamp() * 1000)
    t1 = t0 + interval_ms  # 01:00:00 (closed before 01:05:10)

    storage = BronzeStorage(str(tmp_path / "bronze"))
    state = StateStore(storage)
    state.put_json("binance_BTCUSDT_5m", {"last_closed_open_time_ms": t0})
    state.put_json("binance_TRUMPUSDT_5m", {"last_closed_open_time_ms": t0})
    settings = IngestionSettings(
        _env_file=None,
        storage_uri=str(tmp_path / "bronze"),
        binance_symbols=["BTCUSDT", "TRUMPUSDT"],
        binance_interval="5m",
    )

    btc_attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal btc_attempts
        sym = request.url.params["symbol"]
        if sym == "BTCUSDT":
            btc_attempts += 1
            if btc_attempts == 1:
                return httpx.Response(429, headers={"Retry-After": "0"})
            return httpx.Response(200, json=[_kline(t1, interval_ms=interval_ms)])
        return httpx.Response(500, json={"msg": "upstream error"})

    with caplog.at_level("WARNING"):
        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            res = BinanceConnector(max_retries=2, backoff_seconds=0.0).run(
                storage, state, settings, client, now_fn=lambda: now_dt
            )

    assert btc_attempts == 2
    assert res.status == "partial_failure"
    assert res.records_written == 1
    assert state.get_json("binance_BTCUSDT_5m")["last_closed_open_time_ms"] == t1
    assert state.get_json("binance_TRUMPUSDT_5m")["last_closed_open_time_ms"] == t0
    assert "BTCUSDT" in caplog.text and "429" in caplog.text
    assert "TRUMPUSDT" in caplog.text and "500" in caplog.text and "upstream error" in caplog.text
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/ingestion/test_binance.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ingestion.sources.binance'`.

- [ ] **Step 3: Implement `ingestion/sources/binance.py`**

Define `BINANCE_KLINE_ARROW_SCHEMA` (preserving decimal strings `open`, `high`, `low`, `close`, `volume`, `quote_asset_volume`, `taker_buy_base_asset_volume`, `taker_buy_quote_asset_volume`, `ignore` as `pa.string()`, timestamps and `number_of_trades` as `pa.int64()`, plus `symbol`, `interval`, `ingested_at`).
Implement `BinanceConnector(max_limit: int = 1000, max_retries: int = 3, backoff_seconds: float = 1.0)`:
- Align `now_ms = int(now_dt.timestamp() * 1000)` down to `interval_ms`; `max_closed_open_ms = aligned_now_ms - interval_ms`, `end_time_ms = aligned_now_ms - 1`.
- For each symbol independently (catching per-symbol exceptions after retries, logging `logger.warning` on transient retry with symbol/attempt/HTTP status and `logger.error` with symbol/status/response snippet or exception when exhausted):
  - Determine `start_ms` from state `last_closed_open_time_ms + interval_ms`, or `align_down(now_ms - lookback_hours * 3_600_000, interval_ms)`.
  - Loop paginating `startTime` up to `end_time_ms` with `limit=self.max_limit`; advance `cursor = int(batch[-1][0]) + interval_ms`, and stop when `not batch or len(batch) < self.max_limit or cursor > max_closed_open_ms`.
  - Filter out any candle with `int(row[6]) >= now_ms`.
  - Group closed rows by UTC date (`datetime.fromtimestamp(open_ms / 1000, tz=UTC).strftime("%Y-%m-%d")`), write Parquet under `binance/interval={interval}/symbol={symbol}/date={date_str}/klines_{first_open}_{last_open}.parquet`, then update symbol state `last_closed_open_time_ms = max(open_ms)`.
- Return `status="success"` if all succeeded, `"partial_failure"` if some symbols succeeded and some failed, or `"failed"` if all failed (recording failed symbols in `details["errors"]`).

- [ ] **Step 4: Run tests to verify pass**

Run: `uv run pytest tests/ingestion/test_binance.py -v`
Expected: PASS (`2 passed`).

- [ ] **Step 5: Commit**

```bash
git add ingestion/sources/binance.py tests/ingestion/test_binance.py
git commit -m "feat(ingestion): add incremental Binance klines connector with closed-candle guard"
```

---

### Task 5: CLI, Continuous Loop, Docker Packaging, and Documentation

**Files:**
- Create: `ingestion/cli.py`, `ingestion/Dockerfile`, `docker-compose.yml`, `README.md`, `tests/ingestion/test_cli.py`

**Interfaces:**
- Consumes: `IngestionSettings`, `BronzeStorage`, `StateStore`, `TruthSocialConnector`, `BinanceConnector`, `SourceConnector`, `IngestionResult`.
- Produces: `build_registry() -> dict[str, SourceConnector]`, `run_once(source: str, settings: IngestionSettings, ...) -> int`, `run_loop(...) -> int`, `main(argv: list[str] | None = None) -> int`; CLI command `uv run ingest run|loop`; Docker service `ingestion`.

- [ ] **Step 1: Write failing tests in `tests/ingestion/test_cli.py`**

```python
import threading
import pytest

from ingestion.cli import main, run_loop, run_once
from ingestion.config import IngestionSettings
from ingestion.sources.base import IngestionResult


class _FakeConnector:
    def __init__(self, name: str, statuses: list[str]):
        self.name = name
        self._statuses = list(statuses)
        self.calls = 0

    def run(self, storage, state, settings, client, now_fn=None) -> IngestionResult:
        self.calls += 1
        status = self._statuses.pop(0) if self._statuses else "success"
        if status == "raise":
            raise RuntimeError("transient network error")
        return IngestionResult(source=self.name, status=status, records_written=1, files_written=[], details={})


def test_cli_run_returns_nonzero_on_failure_and_help_exits_zero(tmp_path):
    settings = IngestionSettings(_env_file=None, storage_uri=str(tmp_path / "bronze"))
    ok = _FakeConnector("truth_social", ["success"])
    bad = _FakeConnector("binance", ["partial_failure"])
    registry = {"truth_social": ok, "binance": bad}

    assert run_once("truth_social", settings, registry=registry) == 0
    assert run_once("all", settings, registry=registry) == 1
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0


def test_cli_loop_survives_transient_error_and_stops_cleanly(tmp_path, caplog):
    settings = IngestionSettings(_env_file=None, storage_uri=str(tmp_path / "bronze"), poll_interval_seconds=1)
    flaky = _FakeConnector("truth_social", ["raise", "success"])
    stop_event = threading.Event()

    def sleep_or_stop(seconds: float) -> bool:
        if flaky.calls >= 2:
            stop_event.set()
            return True
        return stop_event.wait(0)

    with caplog.at_level("ERROR"):
        exit_code = run_loop("truth_social", settings, registry={"truth_social": flaky}, stop_event=stop_event, sleep_fn=sleep_or_stop)
    assert exit_code == 0
    assert flaky.calls == 2
    assert "truth_social" in caplog.text and "transient network error" in caplog.text


def test_cli_configure_logging_writes_failures_to_log_file(tmp_path):
    from ingestion.cli import configure_logging

    log_path = tmp_path / "logs" / "ingestion.log"
    configure_logging(str(log_path))
    settings = IngestionSettings(_env_file=None, storage_uri=str(tmp_path / "bronze"), log_file=str(log_path))
    bad = _FakeConnector("binance", ["raise"])
    assert run_once("binance", settings, registry={"binance": bad}) == 1
    assert log_path.is_file()
    contents = log_path.read_text(encoding="utf-8")
    assert "binance" in contents and "transient network error" in contents
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/ingestion/test_cli.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ingestion.cli'`.

- [ ] **Step 3: Implement `ingestion/cli.py`**

Implement:
- `configure_logging(log_file: str | None) -> logging.Logger` — configures `logging.getLogger("ingestion")` idempotently (clears prior handlers, sets `INFO`, attaches a `StreamHandler` and, when `log_file` is non-empty, creates parent directories and attaches `RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=3, encoding="utf-8")` with format `%(asctime)s %(levelname)s [%(name)s] %(message)s`).
- `build_registry() -> dict[str, SourceConnector]` returning `{"truth_social": TruthSocialConnector(), "binance": BinanceConnector()}`
- `run_once(source: str, settings: IngestionSettings, registry: dict[str, SourceConnector] | None = None, client: httpx.Client | None = None) -> int` — runs selected connector(s), logs concise summary (`INFO` on success/skipped with counts, `ERROR` with details/exception traceback on `partial_failure`/`failed`/exception), returns `0` if all `success`/`skipped`, `1` otherwise.
- `run_loop(source: str, settings: IngestionSettings, registry: dict[str, SourceConnector] | None = None, stop_event: threading.Event | None = None, sleep_fn: Callable[[float], bool] | None = None) -> int` — registers `SIGINT`/`SIGTERM` handler when running on main thread to set `stop_event`, loops running `run_once` (never crashing on transient exceptions), sleeps via `stop_event.wait(settings.poll_interval_seconds)` so shutdown is immediate between runs and safe during in-flight writes.
- `main(argv: list[str] | None = None) -> int` loads `IngestionSettings()`, calls `configure_logging(settings.log_file)`, and runs `argparse` subcommands `run [--source all|truth_social|binance]` and `loop [--source all|truth_social|binance]`.

- [ ] **Step 4: Create `ingestion/Dockerfile`, `docker-compose.yml`, and `README.md`**

- `ingestion/Dockerfile`: based on `python:3.13-slim` + `ghcr.io/astral-sh/uv:latest` binary copy; first copies `pyproject.toml` and `uv.lock` and runs `uv sync --frozen --no-dev --extra ingestion --no-install-project` for dependency caching, then copies `ingestion/`, `etl/`, `ai/` (required by `hatchling` wheel packages) and runs `uv sync --frozen --no-dev --extra ingestion`; entrypoint `uv run --no-sync ingest loop`.
- `docker-compose.yml`: service `ingestion` building from `.` with `dockerfile: ingestion/Dockerfile`, optional `env_file: [{ path: .env, required: false }]` so `docker compose config` passes on a clean checkout without `.env`, `environment` defaulting `STORAGE_URI=/app/data/bronze` and `LOG_FILE=/app/data/logs/ingestion.log`, volume `./data:/app/data`, `restart: unless-stopped`.
- `README.md`: concise setup & run commands (`uv sync --extra ingestion`, `uv run ingest run`, `uv run ingest loop`, `docker compose up -d --build`, switching `BINANCE_INTERVAL=5m`, adding a 3rd source under `ingestion/sources/`).

- [ ] **Step 5: Run full test suite, linter, and Docker Compose validation**

Run:
```bash
uv run pytest -v
uv run ruff check ingestion tests
docker compose config
```
Expected: All tests PASS, `ruff check` exits `0`, `docker compose config` exits `0`.

- [ ] **Step 6: Commit**

```bash
git add ingestion/cli.py ingestion/Dockerfile docker-compose.yml README.md tests/ingestion/test_cli.py
git commit -m "feat(ingestion): add CLI, loop runner, docker setup, and README"
```
