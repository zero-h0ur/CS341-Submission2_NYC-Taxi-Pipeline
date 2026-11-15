import hashlib

from taxi_pipeline.ingest import build_batches, ingest_source
from taxi_pipeline.state import load_manifest


def ingest(config, server):
    batch = build_batches(config, "2024-01", "2024-01", ["green"])[0]
    manifest = load_manifest(config.path("state") / "manifest.json")
    result = ingest_source(config, server, manifest, batch.batch_id, batch.url, batch.raw_path)
    return batch, result


def test_stream_retry_discards_partial_download(config, server):
    batch = build_batches(config, "2024-01", "2024-01", ["green"])[0]
    server.fail_stream_once.add(batch.url)
    batch, result = ingest(config, server)
    expected = server.payloads[batch.url]
    assert result.status == "DOWNLOADED"
    assert server.get_count(batch.url) == 2
    assert batch.raw_path.read_bytes() == expected
    assert result.metadata["sha256"] == hashlib.sha256(expected).hexdigest()
    assert not list(config.path("raw").rglob("*.part"))


def test_unchanged_source_skips_body_download(config, server):
    batch, first = ingest(config, server)
    count = server.get_count(batch.url)
    _, second = ingest(config, server)
    assert second.status == "UNCHANGED"
    assert server.get_count(batch.url) == count
    assert first.metadata["sha256"] == second.metadata["sha256"]


def test_source_revision_preserves_old_bytes(config, server, trip_table):
    batch, first = ingest(config, server)
    original = batch.raw_path.read_bytes()
    server.set_trip(batch, trip_table(rows=4))
    _, second = ingest(config, server)
    assert second.status == "DOWNLOADED"
    assert second.metadata["sha256"] != first.metadata["sha256"]
    archives = list((config.path("raw") / "_archive").rglob("*.parquet"))
    assert len(archives) == 1
    assert archives[0].read_bytes() == original
    assert batch.raw_path.read_bytes() == server.payloads[batch.url]


def test_missing_etag_falls_back_to_content_check(config, server):
    server.omit_etag = True
    batch, first = ingest(config, server)
    _, second = ingest(config, server)
    assert second.status == "UNCHANGED"
    assert server.get_count(batch.url) == 2
    assert first.metadata["sha256"] == second.metadata["sha256"]
