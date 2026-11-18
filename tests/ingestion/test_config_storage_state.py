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
    monkeypatch.setattr(
        pq, "write_table", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("disk full"))
    )
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
