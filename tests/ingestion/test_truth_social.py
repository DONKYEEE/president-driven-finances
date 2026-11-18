from datetime import UTC, datetime

import httpx
import pyarrow.parquet as pq
import pytest

from ingestion.config import IngestionSettings
from ingestion.sources.truth_social import TruthSocialConnector
from ingestion.state import StateStore
from ingestion.storage import BronzeStorage

SAMPLE_POSTS = [
    {
        "id": "117634741427255539",
        "created_at": "2026-11-18T01:01:45.859Z",
        "content": "Hello",
        "url": "https://truthsocial.com/@realDonaldTrump/117634741427255539",
        "media": ["https://example.com/a.jpg"],
        "replies_count": 586,
        "reblogs_count": 1622,
        "favourites_count": 6329,
    },
    {
        "id": "107797156496908384",
        "created_at": "2022-02-14T15:54:32.528Z",
        "content": "",
        "url": "https://truthsocial.com/@realDonaldTrump/107797156496908384",
        "media": [],
        "replies_count": None,
        "reblogs_count": None,
        "favourites_count": None,
    },
]


def test_truth_social_writes_parquet_with_nulls_and_skips_304(tmp_path):
    calls: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(dict(request.headers))
        if request.headers.get("if-none-match") == '"etag-1"':
            return httpx.Response(304)
        return httpx.Response(
            200,
            json=SAMPLE_POSTS,
            headers={"ETag": '"etag-1"', "Last-Modified": "Wed, 18 Nov 2026 05:01:49 GMT"},
        )

    storage = BronzeStorage(str(tmp_path / "bronze"))
    state = StateStore(storage)
    settings = IngestionSettings(_env_file=None, storage_uri=str(tmp_path / "bronze"))
    connector = TruthSocialConnector()

    def fixed_now() -> datetime:
        return datetime(2026, 11, 18, 7, 0, 0, tzinfo=UTC)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        res1 = connector.run(storage, state, settings, client, now_fn=fixed_now)
        res2 = connector.run(storage, state, settings, client, now_fn=fixed_now)

    assert res1.status == "success"
    assert res1.records_written == 2
    assert len(res1.files_written) == 1
    assert (
        "truth_social/snapshot_date=2026-11-18/snapshot_20261118T070000Z.parquet"
        in res1.files_written[0]
    )

    table = pq.read_table(res1.files_written[0])
    assert table.column("replies_count").to_pylist() == [586, None]
    assert table.column("media").to_pylist() == [["https://example.com/a.jpg"], []]
    assert table.column("source_etag").to_pylist() == ['"etag-1"', '"etag-1"']

    assert res2.status == "skipped"
    assert res2.records_written == 0
    assert calls[1].get("if-none-match") == '"etag-1"'
    assert calls[1].get("if-modified-since") == "Wed, 18 Nov 2026 05:01:49 GMT"


def test_truth_social_missing_etag_and_corrupt_response_preserves_state(tmp_path, caplog):
    storage = BronzeStorage(str(tmp_path / "bronze"))
    state = StateStore(storage)
    state.put_json(
        "truth_social",
        {"etag": '"prev"', "last_modified": "Tue, 17 Nov 2026 00:00:00 GMT"},
    )
    settings = IngestionSettings(_env_file=None, storage_uri=str(tmp_path / "bronze"))
    connector = TruthSocialConnector()

    def fixed_now() -> datetime:
        return datetime(2026, 11, 18, 8, 0, 0, tzinfo=UTC)

    # HTTP 502 must log failure context and preserve saved state
    with caplog.at_level("ERROR"):
        with httpx.Client(
            transport=httpx.MockTransport(lambda r: httpx.Response(502, text="bad gateway"))
        ) as client:
            with pytest.raises(httpx.HTTPStatusError):
                connector.run(storage, state, settings, client, now_fn=fixed_now)
    assert "502" in caplog.text and "truth_social" in caplog.text
    assert state.get_json("truth_social")["etag"] == '"prev"'

    # Corrupt non-list JSON must fail without overwriting saved state
    with httpx.Client(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"error": "bad"}))
    ) as client:
        with pytest.raises(ValueError):
            connector.run(storage, state, settings, client, now_fn=fixed_now)
    assert state.get_json("truth_social")["etag"] == '"prev"'

    # Valid 200 without ETag/Last-Modified must still write snapshot cleanly
    with httpx.Client(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json=SAMPLE_POSTS))
    ) as client:
        res = connector.run(storage, state, settings, client, now_fn=fixed_now)
    assert res.status == "success"
    assert res.records_written == 2
    assert pq.read_table(res.files_written[0]).column("source_etag").to_pylist() == [None, None]
