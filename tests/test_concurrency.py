import threading

from conftest import PAGE_SIZE, make_cache
from test_writeback import RecordingStorage

from pagecache.storage import LocalFileStorage


def page(byte: int) -> bytes:
    return bytes([byte]) * PAGE_SIZE


def test_concurrent_reads_same_page_are_consistent(cache):
    cache.write_page(0, page(7))
    results: list[bytes] = []

    def reader():
        for _ in range(50):
            results.append(cache.read_page(0))

    threads = [threading.Thread(target=reader) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert all(data == page(7) for data in results)
    assert cache.stats()["hits"] + cache.stats()["misses"] == len(results)


def test_concurrent_writes_same_page_last_wins(tmp_path):
    cache = make_cache(tmp_path, dirty_low_watermark=100, dirty_high_watermark=1000)

    def writer(value: int):
        for _ in range(20):
            cache.write_page(0, page(value))

    threads = [threading.Thread(target=writer, args=(v,)) for v in (1, 2, 3, 4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    final = cache.read_page(0)
    assert final in {page(1), page(2), page(3), page(4)}
    assert len(set(final)) == 1
    cache.close()


def test_concurrent_reads_writes_different_pages(tmp_path):
    inner = LocalFileStorage(tmp_path / "pages", PAGE_SIZE)
    cache = make_cache(
        tmp_path,
        storage=RecordingStorage(inner),
        max_pages=32,
        dirty_low_watermark=100,
        dirty_high_watermark=1000,
    )

    def worker(page_id: int):
        for round_no in range(20):
            cache.write_page(page_id, page(page_id + round_no))
            assert cache.read_page(page_id) == page(page_id + round_no)

    threads = [threading.Thread(target=worker, args=(pid,)) for pid in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    cache.flush_all()
    for pid in range(8):
        assert inner.read_page(pid) == page(pid + 19)
    stats = cache.stats()
    assert stats["dirty_pages"] == 0
    assert stats["cached_pages"] == 8
    cache.close()


def test_stats_consistent_under_concurrency(cache):
    cache.read_page(5)

    def reader():
        for _ in range(100):
            cache.read_page(5)

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    stats = cache.stats()
    assert stats["hits"] + stats["misses"] == 401
    assert stats["misses"] == 1
