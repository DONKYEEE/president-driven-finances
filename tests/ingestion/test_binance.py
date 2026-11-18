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
        res = BinanceConnector(max_limit=2).run(
            storage, state, settings, client, now_fn=lambda: now_dt
        )

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


def test_binance_initial_lookback_without_state_and_already_caught_up(tmp_path):
    now_dt = datetime(2026, 11, 18, 12, 0, 30, tzinfo=UTC)
    expected_start_ms = int(datetime(2026, 11, 18, 11, 0, 0, tzinfo=UTC).timestamp() * 1000)
    t_closed = int(datetime(2026, 11, 18, 11, 59, 0, tzinfo=UTC).timestamp() * 1000)

    storage = BronzeStorage(str(tmp_path / "bronze"))
    state = StateStore(storage)
    settings = IngestionSettings(
        _env_file=None,
        storage_uri=str(tmp_path / "bronze"),
        binance_symbols=["ETHUSDT"],
        binance_interval="1m",
        binance_initial_lookback_hours=1,
    )

    seen_starts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_starts.append(int(request.url.params["startTime"]))
        return httpx.Response(200, json=[_kline(t_closed)])

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        res1 = BinanceConnector().run(storage, state, settings, client, now_fn=lambda: now_dt)
        res2 = BinanceConnector().run(storage, state, settings, client, now_fn=lambda: now_dt)

    assert seen_starts == [expected_start_ms]
    assert res1.status == "success"
    assert res1.records_written == 1
    assert res2.status == "skipped"
    assert res2.records_written == 0
