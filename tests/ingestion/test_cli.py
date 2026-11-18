import os
import signal
import threading

import pytest

import ingestion.cli as cli_module
from ingestion.cli import configure_logging, main, run_loop, run_once
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
        return IngestionResult(
            source=self.name,
            status=status,
            records_written=1,
            files_written=[],
            details={},
        )


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
    settings = IngestionSettings(
        _env_file=None,
        storage_uri=str(tmp_path / "bronze"),
        poll_interval_seconds=1,
    )
    flaky = _FakeConnector("truth_social", ["raise", "success"])
    stop_event = threading.Event()

    def sleep_or_stop(seconds: float) -> bool:
        if flaky.calls >= 2:
            stop_event.set()
            return True
        return stop_event.wait(0)

    with caplog.at_level("ERROR"):
        exit_code = run_loop(
            "truth_social",
            settings,
            registry={"truth_social": flaky},
            stop_event=stop_event,
            sleep_fn=sleep_or_stop,
        )
    assert exit_code == 0
    assert flaky.calls == 2
    assert "truth_social" in caplog.text and "transient network error" in caplog.text


def test_cli_configure_logging_writes_failures_to_log_file(tmp_path):
    log_path = tmp_path / "logs" / "ingestion.log"
    configure_logging(str(log_path))
    try:
        settings = IngestionSettings(
            _env_file=None,
            storage_uri=str(tmp_path / "bronze"),
            log_file=str(log_path),
        )
        bad = _FakeConnector("binance", ["raise"])
        assert run_once("binance", settings, registry={"binance": bad}) == 1
    finally:
        configure_logging(None)

    assert log_path.is_file()
    contents = log_path.read_text(encoding="utf-8")
    assert "binance" in contents and "transient network error" in contents


def test_cli_loop_sigint_sets_stop_event_without_logging_in_signal_handler(
    tmp_path, monkeypatch, caplog
):
    settings = IngestionSettings(
        _env_file=None,
        storage_uri=str(tmp_path / "bronze"),
        poll_interval_seconds=1,
    )
    ok = _FakeConnector("truth_social", ["success"])
    stop_event = threading.Event()
    orig_info = cli_module.logger.info

    def _boom_info(*args, **kwargs):
        raise RuntimeError("reentrant call inside buffered stream writer")

    def sleep_and_signal(seconds: float) -> bool:
        # Simulate signal arriving mid-I/O on the main thread
        monkeypatch.setattr(cli_module.logger, "info", _boom_info)
        try:
            os.kill(os.getpid(), signal.SIGINT)
        finally:
            monkeypatch.setattr(cli_module.logger, "info", orig_info)
        return stop_event.is_set()

    with caplog.at_level("INFO"):
        exit_code = run_loop(
            "truth_social",
            settings,
            registry={"truth_social": ok},
            stop_event=stop_event,
            sleep_fn=sleep_and_signal,
        )

    assert exit_code == 0
    assert stop_event.is_set()
    assert ok.calls == 1
    assert "Received signal" in caplog.text
