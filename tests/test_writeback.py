import time

from conftest import PAGE_SIZE, make_cache

from pagecache.errors import WritebackError
from pagecache.storage import LocalFileStorage


def page(byte: int) -> bytes:
    return bytes([byte]) * PAGE_SIZE


class RecordingStorage:
    """Wraps LocalFileStorage, records writes, can inject failures."""

    def __init__(self, inner):
        self._inner = inner
        self.writes: list[tuple[int, bytes]] = []
        self.fail_pages: set[int] = set()

    def read_page(self, page_id: int) -> bytes:
        return self._inner.read_page(page_id)

    def write_page(self, page_id: int, data: bytes) -> None:
        if page_id in self.fail_pages:
            raise OSError(f"injected failure for page {page_id}")
        self.writes.append((page_id, data))
        self._inner.write_page(page_id, data)

    def flush(self) -> None:
        self._inner.flush()


def make_recording(tmp_path):
    inner = LocalFileStorage(tmp_path / "pages", PAGE_SIZE)
    storage = RecordingStorage(inner)
    cache = make_cache(tmp_path, storage=storage)
    return cache, storage, inner


def test_flush_page_persists_dirty_page(tmp_path):
    cache, _storage, inner = make_recording(tmp_path)
    cache.write_page(0, page(7))
    cache.flush_page(0)
    assert inner.read_page(0) == page(7)
    assert cache.stats()["dirty_pages"] == 0
    cache.close()


def test_flush_all_persists_everything(tmp_path):
    cache, _storage, inner = make_recording(tmp_path)
    for page_id in range(4):
        cache.write_page(page_id, page(page_id))
    cache.flush_all()
    for page_id in range(4):
        assert inner.read_page(page_id) == page(page_id)
    cache.close()


def test_repeated_writes_persist_in_order(tmp_path):
    cache, storage, _inner = make_recording(tmp_path)
    for version in range(5):
        cache.write_page(0, page(version))
        cache.flush_page(0)
    page_zero_writes = [data for pid, data in storage.writes if pid == 0]
    assert page_zero_writes == [page(v) for v in range(5)]
    cache.close()


def test_concurrent_write_and_writeback_never_tears(tmp_path):
    cache, _storage, inner = make_recording(tmp_path)
    for version in range(50):
        cache.write_page(0, page(version))
    cache.flush_all()
    persisted = inner.read_page(0)
    assert persisted == page(49)
    assert len(set(persisted)) == 1
    cache.close()


def test_writeback_failure_is_reported(tmp_path):
    cache, storage, inner = make_recording(tmp_path)
    storage.fail_pages.add(0)
    cache.write_page(0, page(1))
    try:
        cache.flush_page(0)
    except WritebackError:
        pass
    else:
        raise AssertionError("expected WritebackError")
    assert cache.stats()["writeback_failures"] >= 1
    storage.fail_pages.clear()
    cache.write_page(0, page(2))
    cache.flush_all()
    assert inner.read_page(0) == page(2)
    cache.close()


def test_background_writeback_triggers_above_low_watermark(tmp_path):
    cache, storage, _inner = make_recording(tmp_path)
    for page_id in range(4):
        cache.write_page(page_id, page(page_id))
    deadline = time.monotonic() + 5
    while cache.stats()["dirty_pages"] > 0 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert cache.stats()["dirty_pages"] == 0
    assert len(storage.writes) >= 4
    cache.close()
