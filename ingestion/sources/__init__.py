"""Pluggable source connectors for Bronze data ingestion."""

from ingestion.sources.base import IngestionResult, SourceConnector
from ingestion.sources.binance import BinanceConnector
from ingestion.sources.truth_social import TruthSocialConnector

__all__ = [
    "BinanceConnector",
    "IngestionResult",
    "SourceConnector",
    "TruthSocialConnector",
]
