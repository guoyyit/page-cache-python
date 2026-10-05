"""Concurrent access across same and different pages, plus stat accuracy."""

from __future__ import annotations

import threading
import time


def test_concurrent_writes_one_full_version_per_read(service_factory):
    svc = service_factory(capacity=400, dirty_low_watermark=50, dirty_high_watermark=300)
    stop = threading.Event()
    torn: list[bytes] = []

    def writer(token: int) -> None:
        seq = 0
        while not stop.is_set():
            svc.write_page(
                1, bytes([token]) * 64 + seq.to_bytes(8, "little")
            )
            seq += 1

    def reader() -> None:
        while not stop.is_set():
            data = svc.read_page(1)
            if len(data) != 72 or len(set(data[:64])) != 1:
                torn.append(data)

    threads = [
        threading.Thread(target=writer, args=(0x41,)),
        threading.Thread(target=writer, args=(0x42,)),
        threading.Thread(target=reader),
        threading.Thread(target=reader),
    ]
    for thread in threads:
        thread.start()
    time.sleep(0.6)
    stop.set()
    for thread in threads:
        thread.join()
    assert not torn


def test_concurrent_access_to_distinct_pages(service_factory):
    svc = service_factory(capacity=200, dirty_low_watermark=5, dirty_high_watermark=100)
    stop = threading.Event()
    errors: list[Exception] = []

    def worker(token: int) -> None:
        page_id = token
        seq = 0
        while not stop.is_set():
            try:
                payload = bytes([token]) * 16 + seq.to_bytes(8, "little")
                svc.write_page(page_id, payload)
                assert svc.read_page(page_id) == payload
            except Exception as exc:  # pragma: no cover
                errors.append(exc)
            seq += 1

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    time.sleep(0.6)
    stop.set()
    for thread in threads:
        thread.join()
    assert not errors


def test_stats_remain_consistent_under_load(service_factory):
    svc = service_factory(capacity=100, dirty_low_watermark=4, dirty_high_watermark=50)
    stop = threading.Event()

    def writer() -> None:
        index = 0
        while not stop.is_set():
            svc.write_page(index % 100, b"s" * 8)
            index += 1

    def stat_reader() -> None:
        while not stop.is_set():
            stats = svc.stats()
            assert stats.dirty_pages <= stats.capacity
            assert stats.cached_pages <= stats.capacity
            assert stats.hits >= 0 and stats.misses >= 0

    threads = [threading.Thread(target=writer) for _ in range(3)]
    threads.append(threading.Thread(target=stat_reader))
    for thread in threads:
        thread.start()
    time.sleep(0.6)
    stop.set()
    for thread in threads:
        thread.join()

    svc.flush_all()
    final = svc.stats()
    assert final.dirty_pages == 0
    assert final.cached_pages <= final.capacity
