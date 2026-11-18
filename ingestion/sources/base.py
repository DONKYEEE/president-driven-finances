"""Shared connector contract and result types."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, Protocol

import httpx

from ingestion.config import IngestionSettings
from ingestion.state import StateStore
from ingestion.storage import BronzeStorage


@dataclass(slots=True)
class IngestionResult:
    """Summary returned by a single source connector run."""

    source: str
    status: Literal["success", "skipped", "partial_failure", "failed"]
    records_written: int = 0
    files_written: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)


class SourceConnector(Protocol):
    """Contract implemented by each data source connector (Truth Social, Binance, etc.)."""

    name: str

    def run(
        self,
        storage: BronzeStorage,
        state: StateStore,
        settings: IngestionSettings,
        client: httpx.Client,
        now_fn: Callable[[], datetime] | None = None,
    ) -> IngestionResult:
        """Fetch new or updated source data and persist it to Bronze storage."""
        ...
