"""CLI entrypoint and polling loop for Bronze ingestion."""

from __future__ import annotations

import argparse
import logging
import signal
import threading
from collections.abc import Callable
from contextlib import nullcontext
from logging.handlers import RotatingFileHandler
from pathlib import Path

import httpx

from ingestion.config import IngestionSettings
from ingestion.sources.base import SourceConnector
from ingestion.sources.binance import BinanceConnector
from ingestion.sources.truth_social import TruthSocialConnector
from ingestion.state import StateStore
from ingestion.storage import BronzeStorage

logger = logging.getLogger(__name__)

_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


def configure_logging(log_file: str | None) -> logging.Logger:
    """Configure the ingestion package logger with stderr and optional rotating file output."""
    pkg_logger = logging.getLogger("ingestion")
    for handler in list(pkg_logger.handlers):
        pkg_logger.removeHandler(handler)
        handler.close()

    pkg_logger.setLevel(logging.INFO)
    pkg_logger.propagate = True
    formatter = logging.Formatter(_LOG_FORMAT)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    pkg_logger.addHandler(stream_handler)

    if log_file:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            path,
            maxBytes=5_000_000,
            backupCount=3,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        pkg_logger.addHandler(file_handler)

    return pkg_logger


def build_registry() -> dict[str, SourceConnector]:
    """Build the default registry of configured source connectors."""
    connectors: tuple[SourceConnector, ...] = (
        TruthSocialConnector(),
        BinanceConnector(),
    )
    return {connector.name: connector for connector in connectors}


def _select_connectors(
    source: str,
    registry: dict[str, SourceConnector],
) -> list[SourceConnector]:
    if source == "all":
        return list(registry.values())
    if source not in registry:
        known = ", ".join(["all", *registry.keys()])
        raise ValueError(f"Unknown source {source!r}; expected one of: {known}")
    return [registry[source]]


def run_once(
    source: str,
    settings: IngestionSettings,
    registry: dict[str, SourceConnector] | None = None,
    client: httpx.Client | None = None,
) -> int:
    """Execute one ingestion pass for the selected source(s). Returns 0 on success, 1 on errors."""
    reg = registry if registry is not None else build_registry()
    connectors = _select_connectors(source, reg)
    storage = BronzeStorage(settings.storage_uri)
    state = StateStore(storage)
    had_failure = False

    client_ctx = (
        nullcontext(client)
        if client is not None
        else httpx.Client(
            timeout=settings.http_timeout_seconds,
            headers={"User-Agent": "pdf-ingestion/0.1"},
        )
    )

    with client_ctx as active_client:
        for connector in connectors:
            try:
                result = connector.run(storage, state, settings, active_client)
            except Exception:
                logger.exception("Connector %s crashed during fetch", connector.name)
                had_failure = True
                continue

            if result.status in ("partial_failure", "failed"):
                logger.error(
                    "Connector %s finished with status=%s records=%d details=%s",
                    result.source,
                    result.status,
                    result.records_written,
                    result.details,
                )
                had_failure = True
            else:
                logger.info(
                    "Connector %s finished with status=%s records=%d files=%d",
                    result.source,
                    result.status,
                    result.records_written,
                    len(result.files_written),
                )

    return 1 if had_failure else 0


def run_loop(
    source: str,
    settings: IngestionSettings,
    registry: dict[str, SourceConnector] | None = None,
    stop_event: threading.Event | None = None,
    sleep_fn: Callable[[float], bool] | None = None,
) -> int:
    """Continuously run ingestion on poll_interval_seconds until interrupted."""
    event = stop_event or threading.Event()
    waiter = sleep_fn or event.wait
    received_signals: list[int] = []

    def _handle_signal(signum: int, _frame: object) -> None:
        received_signals.append(signum)
        event.set()

    on_main_thread = threading.current_thread() is threading.main_thread()
    prev_sigint = signal.getsignal(signal.SIGINT) if on_main_thread else None
    prev_sigterm = signal.getsignal(signal.SIGTERM) if on_main_thread else None
    if on_main_thread:
        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

    try:
        while not event.is_set():
            try:
                run_once(source, settings, registry=registry)
            except Exception:
                logger.exception("Unexpected error in ingestion loop for source=%s", source)
            if event.is_set():
                break
            if waiter(float(settings.poll_interval_seconds)):
                break
        if received_signals:
            logger.info("Received signal %d, stopping loop", received_signals[0])
    finally:
        if on_main_thread:
            signal.signal(signal.SIGINT, prev_sigint)
            signal.signal(signal.SIGTERM, prev_sigterm)

    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ingest",
        description="Pull Truth Social and Binance data into Bronze Parquet.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for cmd_name, help_text in (
        ("run", "Run one ingestion pass and exit"),
        ("loop", "Continuously run ingestion on POLL_INTERVAL_SECONDS"),
    ):
        sub = subparsers.add_parser(cmd_name, help=help_text)
        sub.add_argument(
            "--source",
            default="all",
            choices=["all", "truth_social", "binance"],
            help="Source connector to execute (default: all)",
        )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint."""
    args = _build_parser().parse_args(argv)
    settings = IngestionSettings()
    configure_logging(settings.log_file)
    if args.command == "run":
        return run_once(args.source, settings)
    return run_loop(args.source, settings)


if __name__ == "__main__":
    raise SystemExit(main())
