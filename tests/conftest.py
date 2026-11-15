from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta
import hashlib

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from taxi_pipeline.config import load_config
from taxi_pipeline.ingest import build_batches


@pytest.fixture(autouse=True)
def block_real_http(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError("Tests must not call real HTTP endpoints.")
    monkeypatch.setattr(requests.sessions.Session, "request", blocked)


@pytest.fixture
def config(tmp_path):
    original = load_config()
    values = deepcopy(original.values)
    values["duckdb"].update(memory_limit="128MB", threads=1)
    values["ingest"].update(max_attempts=2, chunk_bytes=1024)
    root = tmp_path / "project"
    root.mkdir()
    result = replace(original, root=root, values=values)
    reference = result.path("raw") / "reference" / "taxi_zone_lookup.csv"
    reference.parent.mkdir(parents=True)
    reference.write_text(
        "LocationID,Borough,Zone,service_zone\n"
        "1,Manhattan,Fixture One,Yellow Zone\n"
        "2,Queens,Fixture Two,Boro Zone\n",
        encoding="utf-8",
    )
    return result


@pytest.fixture
def trip_table(config):
    def make(service="green", month="2024-01", rows=3):
        mapping = config.values["sources"]["services"][service]
        start = datetime.fromisoformat(f"{month}-15T12:00:00")
        schema = pa.schema([
            (mapping["pickup"], pa.timestamp("us")),
            (mapping["dropoff"], pa.timestamp("us")),
            (mapping["pu_location"], pa.int64()),
            (mapping["do_location"], pa.int64()),
        ])
        return pa.Table.from_pydict({
            mapping["pickup"]: [start] * rows,
            mapping["dropoff"]: [start + timedelta(minutes=10)] * rows,
            mapping["pu_location"]: [1] * rows,
            mapping["do_location"]: [1] * rows,
        }, schema=schema)
    return make


@pytest.fixture
def write_batch(config, trip_table):
    def write(table=None, service="green", month="2024-01"):
        batch = build_batches(config, month, month, [service])[0]
        batch.raw_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(trip_table(service, month) if table is None else table, batch.raw_path)
        return batch
    return write


class FakeResponse:
    def __init__(self, body, status=200, broken=False, omit_etag=False):
        self.body = body
        self.status = status
        self.broken = broken
        self.headers = {"Content-Length": str(len(body))}
        if not omit_etag:
            self.headers["ETag"] = '"' + hashlib.sha256(body).hexdigest() + '"'

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def raise_for_status(self):
        if self.status >= 400:
            raise requests.HTTPError(f"Fixture HTTP status {self.status}")

    def iter_content(self, chunk_size):
        if self.broken:
            yield self.body[:max(1, len(self.body) // 2)]
            raise requests.ConnectionError("Fixture stream interrupted.")
        for offset in range(0, len(self.body), chunk_size):
            yield self.body[offset:offset + chunk_size]


class FakeSource:
    def __init__(self):
        self.payloads = {}
        self.calls = []
        self.fail_stream_once = set()
        self.omit_etag = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def set_trip(self, batch, table):
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink)
        self.payloads[batch.url] = sink.getvalue().to_pybytes()

    def head(self, url, **kwargs):
        self.calls.append(("HEAD", url))
        return FakeResponse(
            self.payloads.get(url, b"not found"),
            status=200 if url in self.payloads else 404, omit_etag=self.omit_etag,
        )

    def get(self, url, **kwargs):
        self.calls.append(("GET", url))
        broken = url in self.fail_stream_once
        self.fail_stream_once.discard(url)
        return FakeResponse(
            self.payloads.get(url, b"not found"),
            status=200 if url in self.payloads else 404,
            broken=broken, omit_etag=self.omit_etag,
        )

    def get_count(self, url):
        return self.calls.count(("GET", url))


@pytest.fixture
def server(config, trip_table, monkeypatch):
    source = FakeSource()
    source.payloads[config.values["sources"]["zone_lookup_url"]] = (
        config.path("raw") / "reference" / "taxi_zone_lookup.csv"
    ).read_bytes()
    batch = build_batches(config, "2024-01", "2024-01", ["green"])[0]
    source.set_trip(batch, trip_table())
    monkeypatch.setattr("taxi_pipeline.pipeline.requests.Session", lambda: source)
    monkeypatch.setattr("taxi_pipeline.ingest.time.sleep", lambda seconds: None)
    return source
