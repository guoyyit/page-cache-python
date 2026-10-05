"""HTTP surface of the page cache service."""

from __future__ import annotations

import base64

import pytest
from fastapi.testclient import TestClient

from pagecache.api import create_app

from .conftest import make_config


@pytest.fixture
def client(data_dir):
    app = create_app(make_config(data_dir, capacity=64, dirty_high_watermark=32))
    with TestClient(app) as test_client:
        yield test_client


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_missing_page_is_404(client):
    assert client.get("/pages/123").status_code == 404


def test_write_and_read_round_trip(client):
    payload = base64.b64encode(b"binary\x00payload").decode()
    response = client.put("/pages/1", json={"data": payload})
    assert response.status_code == 200
    read = client.get("/pages/1").json()
    assert base64.b64decode(read["data"]) == b"binary\x00payload"


def test_invalid_base64_rejected(client):
    response = client.put("/pages/1", json={"data": "not_base64!!!"})
    assert response.status_code == 400


def test_flush_endpoints(client):
    client.put("/pages/1", json={"data": base64.b64encode(b"x").decode()})
    assert client.post("/pages/1/flush").status_code == 200
    assert client.post("/flush").status_code == 200


def test_stats_endpoint_shape(client):
    client.put("/pages/1", json={"data": base64.b64encode(b"y").decode()})
    client.get("/pages/1")
    stats = client.get("/stats").json()
    for key in (
        "hits",
        "misses",
        "dirty_pages",
        "cached_pages",
        "capacity",
        "evictions",
        "writebacks",
        "failed_writebacks",
    ):
        assert key in stats
    assert stats["hits"] >= 1


def test_invalid_pages_endpoint(client):
    assert client.get("/invalid-pages").json() == {"page_ids": []}
