"""JSON state persistence inside BronzeStorage for incremental watermarks and ETags."""

from __future__ import annotations

import json
from typing import Any

from ingestion.storage import BronzeStorage


class StateStore:
    """Stores small per-source incremental metadata under _state/<key>.json."""

    def __init__(self, storage: BronzeStorage) -> None:
        self._storage = storage

    def _key_path(self, key: str) -> str:
        return f"_state/{key}.json"

    def get_json(self, key: str) -> dict[str, Any] | None:
        """Load JSON object for key, or None if not yet saved."""
        raw = self._storage.read_bytes(self._key_path(key))
        if raw is None:
            return None
        return json.loads(raw.decode("utf-8"))

    def put_json(self, key: str, payload: dict[str, Any]) -> None:
        """Atomically persist JSON state payload."""
        encoded = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        self._storage.write_bytes(self._key_path(key), encoded)
