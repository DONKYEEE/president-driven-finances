"""Binance public REST API (/api/v3/klines) connector for Bronze Parquet."""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx
import pyarrow as pa

from ingestion.config import IngestionSettings, interval_to_ms
from ingestion.sources.base import IngestionResult
from ingestion.state import StateStore
from ingestion.storage import BronzeStorage

logger = logging.getLogger(__name__)

BINANCE_KLINE_ARROW_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("interval", pa.string(), nullable=False),
        pa.field("open_time_ms", pa.int64(), nullable=False),
        pa.field("open", pa.string(), nullable=False),
        pa.field("high", pa.string(), nullable=False),
        pa.field("low", pa.string(), nullable=False),
        pa.field("close", pa.string(), nullable=False),
        pa.field("volume", pa.string(), nullable=False),
        pa.field("close_time_ms", pa.int64(), nullable=False),
        pa.field("quote_asset_volume", pa.string(), nullable=False),
        pa.field("number_of_trades", pa.int64(), nullable=False),
        pa.field("taker_buy_base_asset_volume", pa.string(), nullable=False),
        pa.field("taker_buy_quote_asset_volume", pa.string(), nullable=False),
        pa.field("ignore", pa.string(), nullable=False),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


def align_down(ts_ms: int, interval_ms: int) -> int:
    """Align a millisecond timestamp down to the nearest interval boundary."""
    return (ts_ms // interval_ms) * interval_ms


def _klines_to_arrow(
    rows: list[list[Any]],
    symbol: str,
    interval: str,
    ingested_at: datetime,
) -> pa.Table:
    n = len(rows)
    return pa.table(
        {
            "symbol": pa.array([symbol] * n, type=pa.string()),
            "interval": pa.array([interval] * n, type=pa.string()),
            "open_time_ms": pa.array([int(r[0]) for r in rows], type=pa.int64()),
            "open": pa.array([str(r[1]) for r in rows], type=pa.string()),
            "high": pa.array([str(r[2]) for r in rows], type=pa.string()),
            "low": pa.array([str(r[3]) for r in rows], type=pa.string()),
            "close": pa.array([str(r[4]) for r in rows], type=pa.string()),
            "volume": pa.array([str(r[5]) for r in rows], type=pa.string()),
            "close_time_ms": pa.array([int(r[6]) for r in rows], type=pa.int64()),
            "quote_asset_volume": pa.array([str(r[7]) for r in rows], type=pa.string()),
            "number_of_trades": pa.array([int(r[8]) for r in rows], type=pa.int64()),
            "taker_buy_base_asset_volume": pa.array([str(r[9]) for r in rows], type=pa.string()),
            "taker_buy_quote_asset_volume": pa.array([str(r[10]) for r in rows], type=pa.string()),
            "ignore": pa.array([str(r[11]) for r in rows], type=pa.string()),
            "ingested_at": pa.array([ingested_at] * n, type=pa.timestamp("us", tz="UTC")),
        },
        schema=BINANCE_KLINE_ARROW_SCHEMA,
    )


class BinanceConnector:
    """Incrementally fetches closed klines per symbol into date-partitioned Parquet."""

    name = "binance"

    def __init__(
        self,
        max_limit: int = 1000,
        max_retries: int = 3,
        backoff_seconds: float = 1.0,
    ) -> None:
        self.max_limit = max_limit
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds

    def _fetch_page(
        self,
        client: httpx.Client,
        base_url: str,
        symbol: str,
        interval: str,
        start_time_ms: int,
        end_time_ms: int,
    ) -> list[list[Any]]:
        url = f"{base_url.rstrip('/')}/api/v3/klines"
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": str(start_time_ms),
            "endTime": str(end_time_ms),
            "limit": str(self.max_limit),
        }
        max_attempts = max(1, self.max_retries + 1)

        for attempt in range(1, max_attempts + 1):
            try:
                response = client.get(url, params=params)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, list):
                    raise TypeError(
                        f"Expected list payload from Binance for {symbol}, got {type(payload).__name__}"
                    )
                return payload
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                body_snippet = exc.response.text[:300]
                retryable = status == 429 or status >= 500
                if retryable and attempt < max_attempts:
                    retry_after = float(exc.response.headers.get("Retry-After", "0") or 0)
                    delay = max(retry_after, self.backoff_seconds * (2 ** (attempt - 1)))
                    logger.warning(
                        "binance transient HTTP error, retrying: symbol=%s interval=%s "
                        "attempt=%d/%d status=%s delay=%.2fs body=%r",
                        symbol,
                        interval,
                        attempt,
                        max_attempts,
                        status,
                        delay,
                        body_snippet,
                    )
                    if delay > 0:
                        time.sleep(delay)
                    continue
                logger.error(
                    "binance fetch failed: symbol=%s interval=%s url=%s status=%s body=%r",
                    symbol,
                    interval,
                    url,
                    status,
                    body_snippet,
                )
                raise
            except (httpx.RequestError, ValueError) as exc:
                if attempt < max_attempts:
                    delay = self.backoff_seconds * (2 ** (attempt - 1))
                    logger.warning(
                        "binance request error, retrying: symbol=%s interval=%s "
                        "attempt=%d/%d error=%s",
                        symbol,
                        interval,
                        attempt,
                        max_attempts,
                        exc,
                    )
                    if delay > 0:
                        time.sleep(delay)
                    continue
                logger.error(
                    "binance fetch failed: symbol=%s interval=%s url=%s error=%s",
                    symbol,
                    interval,
                    url,
                    exc,
                )
                raise

        raise RuntimeError(f"Unreachable retry loop termination for {symbol}")

    def _ingest_symbol(
        self,
        storage: BronzeStorage,
        state: StateStore,
        settings: IngestionSettings,
        client: httpx.Client,
        symbol: str,
        now: datetime,
    ) -> tuple[int, list[str]]:
        interval = settings.binance_interval
        interval_ms = interval_to_ms(interval)
        now_ms = int(now.timestamp() * 1000)
        aligned_now_ms = align_down(now_ms, interval_ms)
        max_closed_open_ms = aligned_now_ms - interval_ms
        end_time_ms = aligned_now_ms - 1

        state_key = f"binance_{symbol}_{interval}"
        prev_state = state.get_json(state_key) or {}
        last_open_ms = prev_state.get("last_closed_open_time_ms")
        if last_open_ms is not None:
            start_ms = int(last_open_ms) + interval_ms
        else:
            start_ms = align_down(
                now_ms - settings.binance_initial_lookback_hours * 3_600_000,
                interval_ms,
            )

        if start_ms > max_closed_open_ms:
            return 0, []

        closed_rows: list[list[Any]] = []
        cursor = start_ms
        while cursor <= max_closed_open_ms:
            batch = self._fetch_page(
                client=client,
                base_url=settings.binance_base_url,
                symbol=symbol,
                interval=interval,
                start_time_ms=cursor,
                end_time_ms=end_time_ms,
            )
            if not batch:
                break
            for row in batch:
                if int(row[6]) < now_ms:
                    closed_rows.append(row)
            cursor = int(batch[-1][0]) + interval_ms
            if len(batch) < self.max_limit or cursor > max_closed_open_ms:
                break

        if not closed_rows:
            return 0, []

        grouped: dict[str, list[list[Any]]] = defaultdict(list)
        for row in closed_rows:
            open_ms = int(row[0])
            date_str = datetime.fromtimestamp(open_ms / 1000, tz=UTC).strftime("%Y-%m-%d")
            grouped[date_str].append(row)

        written_files: list[str] = []
        for date_str, day_rows in sorted(grouped.items()):
            first_open = int(day_rows[0][0])
            last_open = int(day_rows[-1][0])
            rel_path = (
                f"binance/interval={interval}/symbol={symbol}/date={date_str}/"
                f"klines_{first_open}_{last_open}.parquet"
            )
            table = _klines_to_arrow(day_rows, symbol=symbol, interval=interval, ingested_at=now)
            written_files.append(storage.write_parquet(rel_path, table))

        max_written_open_ms = max(int(r[0]) for r in closed_rows)
        state.put_json(
            state_key,
            {
                "symbol": symbol,
                "interval": interval,
                "last_closed_open_time_ms": max_written_open_ms,
                "updated_at": now.isoformat(),
            },
        )
        return len(closed_rows), written_files

    def run(
        self,
        storage: BronzeStorage,
        state: StateStore,
        settings: IngestionSettings,
        client: httpx.Client,
        now_fn: Callable[[], datetime] | None = None,
    ) -> IngestionResult:
        now = (now_fn or (lambda: datetime.now(UTC)))().astimezone(UTC)
        total_records = 0
        files_written: list[str] = []
        errors: dict[str, str] = {}
        ok_count = 0

        for symbol in settings.binance_symbols:
            try:
                count, paths = self._ingest_symbol(
                    storage=storage,
                    state=state,
                    settings=settings,
                    client=client,
                    symbol=symbol,
                    now=now,
                )
                ok_count += 1
                total_records += count
                files_written.extend(paths)
            except Exception as exc:
                logger.exception("binance symbol %s failed during ingestion", symbol)
                errors[symbol] = str(exc)

        if errors and ok_count == 0:
            status = "failed"
        elif errors:
            status = "partial_failure"
        elif total_records == 0:
            status = "skipped"
        else:
            status = "success"

        logger.info(
            "binance finished: status=%s interval=%s records=%d files=%d errors=%d",
            status,
            settings.binance_interval,
            total_records,
            len(files_written),
            len(errors),
        )
        return IngestionResult(
            source=self.name,
            status=status,
            records_written=total_records,
            files_written=files_written,
            details={"errors": errors, "interval": settings.binance_interval},
        )
