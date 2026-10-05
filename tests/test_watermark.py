import threading
import time

from conftest import PAGE_SIZE, make_cache
from test_writeback import RecordingStorage

from pagecache.errors import ThrottleTimeoutError
from pagecache.storage import LocalFileStorage


def page(byte: int) -> bytes:
    return bytes([byte]) * PAGE_SIZE


class BlockingStorage(RecordingStorage):
    """Storage whose writes block until released, to hold dirty pages."""

    def __init__(self, inner):
        super().__init__(inner)
        self.gate = threading.Event()
        self.gate.set()

    def write_page(self, page_id: int, data: bytes) -> None:
        self.gate.wait()
        super().write_page(page_id, data)


def test_writer_throttled_above_high_watermark(tmp_path):
    inner = LocalFileStorage(tmp_path / "pages", PAGE_SIZE)
    storage = BlockingStorage(inner)
    cache = make_cache(
        tmp_path,
        storage=storage,
        max_pages=32,
        dirty_low_watermark=2,
        dirty_high_watermark=4,
        throttle_timeout=0.3,
    )
    storage.gate.clear()
    for page_id in range(4):
        cache.write_page(page_id, page(page_id))
    started = time.monotonic()
    try:
        cache.write_page(99, page(99))
    except ThrottleTimeoutError:
        pass
    else:
        raise AssertionError("expected ThrottleTimeoutError")
    waited = time.monotonic() - started
    assert waited >= 0.3
    storage.gate.set()
    cache.close()


def test_throttled_writer_unblocks_after_writeback(tmp_path):
    inner = LocalFileStorage(tmp_path / "pages", PAGE_SIZE)
    storage = BlockingStorage(inner)
    cache = make_cache(
        tmp_path,
        storage=storage,
        max_pages=32,
        dirty_low_watermark=2,
        dirty_high_watermark=4,
        throttle_timeout=5.0,
    )
    storage.gate.clear()
    for page_id in range(4):
        cache.write_page(page_id, page(page_id))

    result: list[str] = []

    def writer():
        cache.write_page(99, page(99))
        result.append("done")

    thread = threading.Thread(target=writer)
    thread.start()
    time.sleep(0.2)
    assert result == []
    storage.gate.set()
    thread.join(timeout=5)
    assert result == ["done"]
    cache.close()
