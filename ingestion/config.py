"""Configuration schema and interval helpers for data ingestion."""

from __future__ import annotations

import json
from typing import Annotated

from pydantic import field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

SUPPORTED_BINANCE_INTERVALS: dict[str, int] = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
}


def interval_to_ms(interval: str) -> int:
    """Convert a supported Binance kline interval string to milliseconds."""
    if interval not in SUPPORTED_BINANCE_INTERVALS:
        supported = ", ".join(SUPPORTED_BINANCE_INTERVALS)
        raise ValueError(f"Unsupported Binance interval {interval!r}; expected one of: {supported}")
    return SUPPORTED_BINANCE_INTERVALS[interval]


class IngestionSettings(BaseSettings):
    """Runtime settings loaded from environment variables and optional .env file."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    storage_uri: str = "./data/bronze"
    log_file: str | None = "./data/logs/ingestion.log"

    binance_base_url: str = "https://data-api.binance.vision"
    binance_symbols: Annotated[list[str], NoDecode] = [
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "XRPUSDT",
        "TRUMPUSDT",
    ]
    binance_interval: str = "1m"
    binance_initial_lookback_hours: int = 24

    truth_social_url: str = "https://ix.cnn.io/data/truth-social/truth_archive.json"
    poll_interval_seconds: int = 300
    http_timeout_seconds: float = 30.0

    @field_validator("binance_symbols", mode="before")
    @classmethod
    def _parse_binance_symbols(cls, value: object) -> list[str]:
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("["):
                parsed = json.loads(stripped)
                if not isinstance(parsed, list):
                    raise ValueError("BINANCE_SYMBOLS JSON value must be a list")
                items = [str(item).strip() for item in parsed if str(item).strip()]
            else:
                items = [part.strip() for part in stripped.split(",") if part.strip()]
        elif isinstance(value, list):
            items = [str(item).strip() for item in value if str(item).strip()]
        else:
            raise ValueError("BINANCE_SYMBOLS must be a comma-separated string or list")
        if not items:
            raise ValueError("BINANCE_SYMBOLS cannot be empty")
        return items

    @field_validator("binance_interval")
    @classmethod
    def _validate_interval(cls, value: str) -> str:
        interval_to_ms(value)
        return value
