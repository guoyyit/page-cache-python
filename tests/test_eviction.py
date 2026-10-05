from conftest import PAGE_SIZE, make_cache
from test_writeback import RecordingStorage

from pagecache.storage import LocalFileStorage


def page(byte: int) -> bytes:
    return bytes([byte]) * PAGE_SIZE


def test_clean_pages_evicted_at_capacity(tmp_path):
    cache = make_cache(tmp_path, max_pages=4)
    for page_id in range(4):
        cache.read_page(page_id)
    assert cache.stats()["cached_pages"] == 4
    cache.read_page(4)
    assert cache.stats()["cached_pages"] == 4
    cache.close()


def test_dirty_page_written_back_before_eviction(tmp_path):
    inner = LocalFileStorage(tmp_path / "pages", PAGE_SIZE)
    storage = RecordingStorage(inner)
    cache = make_cache(
        tmp_path,
        storage=storage,
        max_pages=4,
        dirty_low_watermark=100,
        dirty_high_watermark=200,
    )
    for page_id in range(4):
        cache.write_page(page_id, page(0xAA + page_id))
    cache.read_page(4)
    assert inner.read_page(0) == page(0xAA)
    assert cache.stats()["cached_pages"] <= 4
    cache.close()


def test_evicted_page_reload_returns_persisted_content(tmp_path):
    inner = LocalFileStorage(tmp_path / "pages", PAGE_SIZE)
    cache = make_cache(
        tmp_path,
        storage=RecordingStorage(inner),
        max_pages=2,
        dirty_low_watermark=100,
        dirty_high_watermark=200,
    )
    cache.write_page(0, page(1))
    cache.read_page(1)
    cache.read_page(2)
    assert cache.read_page(0) == page(1)
    cache.close()
