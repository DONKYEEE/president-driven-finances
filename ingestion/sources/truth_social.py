"""Truth Social CNN archive connector writing immutable Bronze Parquet snapshots."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx
import pyarrow as pa

from ingestion.config import IngestionSettings
from ingestion.sources.base import IngestionResult
from ingestion.state import StateStore
from ingestion.storage import BronzeStorage

logger = logging.getLogger(__name__)

TRUTH_SOCIAL_ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False),
        pa.field("created_at", pa.string()),
        pa.field("content", pa.string()),
        pa.field("url", pa.string()),
        pa.field("media", pa.list_(pa.string())),
        pa.field("replies_count", pa.int64(), nullable=True),
        pa.field("reblogs_count", pa.int64(), nullable=True),
        pa.field("favourites_count", pa.int64(), nullable=True),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
        pa.field("source_etag", pa.string(), nullable=True),
    ]
)


def _records_to_arrow(
    records: list[dict[str, Any]],
    ingested_at: datetime,
    source_etag: str | None,
) -> pa.Table:
    n = len(records)
    ids: list[str] = []
    created_ats: list[str | None] = []
    contents: list[str | None] = []
    urls: list[str | None] = []
    media_lists: list[list[str]] = []
    replies: list[int | None] = []
    reblogs: list[int | None] = []
    favourites: list[int | None] = []

    for row in records:
        if not isinstance(row, dict) or row.get("id") is None:
            raise ValueError("Truth Social archive item must be an object with non-null 'id'")
        ids.append(str(row["id"]))
        created_ats.append(row.get("created_at"))
        contents.append(row.get("content"))
        urls.append(row.get("url"))
        raw_media = row.get("media") or []
        media_lists.append([str(item) for item in raw_media])
        replies.append(row.get("replies_count"))
        reblogs.append(row.get("reblogs_count"))
        favourites.append(row.get("favourites_count"))

    return pa.table(
        {
            "id": pa.array(ids, type=pa.string()),
            "created_at": pa.array(created_ats, type=pa.string()),
            "content": pa.array(contents, type=pa.string()),
            "url": pa.array(urls, type=pa.string()),
            "media": pa.array(media_lists, type=pa.list_(pa.string())),
            "replies_count": pa.array(replies, type=pa.int64()),
            "reblogs_count": pa.array(reblogs, type=pa.int64()),
            "favourites_count": pa.array(favourites, type=pa.int64()),
            "ingested_at": pa.array([ingested_at] * n, type=pa.timestamp("us", tz="UTC")),
            "source_etag": pa.array([source_etag] * n, type=pa.string()),
        },
        schema=TRUTH_SOCIAL_ARROW_SCHEMA,
    )


class TruthSocialConnector:
    """Fetches the CNN Truth Social archive conditionally and writes Parquet snapshots."""

    name = "truth_social"

    def run(
        self,
        storage: BronzeStorage,
        state: StateStore,
        settings: IngestionSettings,
        client: httpx.Client,
        now_fn: Callable[[], datetime] | None = None,
    ) -> IngestionResult:
        now = (now_fn or (lambda: datetime.now(UTC)))().astimezone(UTC)
        prev_state = state.get_json(self.name) or {}

        headers: dict[str, str] = {}
        if prev_state.get("etag"):
            headers["If-None-Match"] = str(prev_state["etag"])
        if prev_state.get("last_modified"):
            headers["If-Modified-Since"] = str(prev_state["last_modified"])

        try:
            response = client.get(settings.truth_social_url, headers=headers)
            if response.status_code == 304:
                logger.info("truth_social unchanged (HTTP 304 Not Modified), skipping snapshot")
                return IngestionResult(
                    source=self.name,
                    status="skipped",
                    records_written=0,
                    files_written=[],
                    details={"status_code": 304},
                )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            body_snippet = exc.response.text[:300]
            logger.error(
                "truth_social fetch failed: url=%s status=%s body=%r",
                settings.truth_social_url,
                exc.response.status_code,
                body_snippet,
            )
            raise
        except httpx.RequestError as exc:
            logger.error(
                "truth_social transport error: url=%s error=%s",
                settings.truth_social_url,
                exc,
            )
            raise

        try:
            payload = response.json()
            if not isinstance(payload, list):
                raise ValueError(
                    f"Expected JSON list from truth_social archive, got {type(payload).__name__}"
                )
            etag = response.headers.get("ETag")
            last_modified = response.headers.get("Last-Modified")
            table = _records_to_arrow(payload, ingested_at=now, source_etag=etag)
        except Exception as exc:
            logger.error(
                "truth_social payload parsing/conversion failed: url=%s error=%s",
                settings.truth_social_url,
                exc,
            )
            raise

        rel_path = (
            f"truth_social/snapshot_date={now.strftime('%Y-%m-%d')}/"
            f"snapshot_{now.strftime('%Y%m%dT%H%M%SZ')}.parquet"
        )
        written_path = storage.write_parquet(rel_path, table)

        new_state: dict[str, Any] = {
            "last_ingested_at": now.isoformat(),
            "record_count": len(payload),
        }
        if etag:
            new_state["etag"] = etag
        if last_modified:
            new_state["last_modified"] = last_modified
        state.put_json(self.name, new_state)

        logger.info("truth_social wrote %d records to %s", len(payload), written_path)
        return IngestionResult(
            source=self.name,
            status="success",
            records_written=len(payload),
            files_written=[written_path],
            details={"etag": etag, "last_modified": last_modified},
        )
