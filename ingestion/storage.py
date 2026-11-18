"""Filesystem-agnostic Bronze Parquet and byte storage over fsspec."""

from __future__ import annotations

import uuid
from typing import Callable

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq
from fsspec.implementations.local import LocalFileSystem


class BronzeStorage:
    """Writes immutable Parquet artifacts and byte blobs atomically via fsspec."""

    def __init__(self, storage_uri: str) -> None:
        normalized = storage_uri.rstrip("/") or "/"
        self.storage_uri = normalized
        self.fs, self.base_path = fsspec.core.url_to_fs(normalized)

    def _resolve_path(self, relative_path: str) -> str:
        rel = relative_path.lstrip("/")
        if self.base_path == "/":
            return f"/{rel}"
        return f"{self.base_path.rstrip('/')}/{rel}"

    def _public_uri(self, resolved_path: str) -> str:
        if isinstance(self.fs, LocalFileSystem):
            return resolved_path
        protocol = self.fs.protocol[0] if isinstance(self.fs.protocol, tuple) else self.fs.protocol
        return f"{protocol}://{resolved_path.lstrip('/')}"

    def _write_atomic(self, relative_path: str, writer: Callable[[object], None]) -> str:
        target = self._resolve_path(relative_path)
        parent = target.rsplit("/", 1)[0]
        if parent:
            self.fs.makedirs(parent, exist_ok=True)
        tmp_path = f"{target}.{uuid.uuid4().hex}.tmp"
        try:
            with self.fs.open(tmp_path, "wb") as fh:
                writer(fh)
            self.fs.mv(tmp_path, target)
        except Exception:
            if self.fs.exists(tmp_path):
                self.fs.rm(tmp_path)
            raise
        return self._public_uri(target)

    def write_parquet(self, relative_path: str, table: pa.Table) -> str:
        """Atomically write an Arrow table as zstd-compressed Parquet."""
        return self._write_atomic(
            relative_path,
            lambda fh: pq.write_table(table, fh, compression="zstd"),
        )

    def read_bytes(self, relative_path: str) -> bytes | None:
        """Read raw bytes if present, otherwise return None."""
        target = self._resolve_path(relative_path)
        if not self.fs.exists(target):
            return None
        with self.fs.open(target, "rb") as fh:
            return fh.read()

    def write_bytes(self, relative_path: str, data: bytes) -> None:
        """Atomically write raw bytes."""
        self._write_atomic(relative_path, lambda fh: fh.write(data))
