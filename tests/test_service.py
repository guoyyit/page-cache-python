import base64

from conftest import PAGE_SIZE, make_cache
from fastapi.testclient import TestClient

from pagecache.service import create_app


def encode(data: bytes) -> str:
    return base64.b64encode(data).decode()


def test_http_read_write_flush_stats(tmp_path):
    cache = make_cache(tmp_path)
    client = TestClient(create_app(cache))

    response = client.get("/pages/0")
    assert response.status_code == 200
    assert base64.b64decode(response.json()["data"]) == bytes(PAGE_SIZE)

    payload = b"\x5A" * PAGE_SIZE
    response = client.put("/pages/0", json={"data": encode(payload)})
    assert response.status_code == 200

    response = client.get("/pages/0")
    assert base64.b64decode(response.json()["data"]) == payload

    assert client.post("/flush").status_code == 200
    assert client.post("/flush/0").status_code == 200

    stats = client.get("/stats").json()
    assert stats["hits"] == 1
    assert stats["misses"] == 1
    assert stats["dirty_pages"] == 0
    cache.close()
