"""Dirty-page watermarks, write throttling and capacity eviction."""

from __future__ import annotations

import threading
import time

from pagecache.errors import ThrottleTimeoutError
from pagecache.storage import FileBlockStorage


def test_low_watermark_triggers_background_writeback(service_factory):
    svc = service_factory(
        capacity=20, dirty_low_watermark=2, dirty_high_watermark=10,
    )
    for page_id in range(3):
        svc.write_page(page_id, bytes([page_id]) * 4)
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if svc.stats().dirty_pages == 0:
            break
        time.sleep(0.02)
    assert svc.stats().dirty_pages == 0


def test_high_watermark_blocks_writer_until_drained(service_factory, monkeypatch):
    svc = service_factory(
        capacity=20,
        dirty_low_watermark=1,
        dirty_high_watermark=3,
        write_throttle_timeout=3.0,
        worker_count=1,
    )
    gate = threading.Event()
    original = FileBlockStorage.write_page_with_sequence

    def stalled(self, page_id, sequence, data):
        gate.wait(3)
        return original(self, page_id, sequence, data)

    monkeypatch.setattr(FileBlockStorage, "write_page_with_sequence", stalled)
    for page_id in range(3):
        svc.write_page(page_id, b"x" * 4)

    released = threading.Event()

    def blocked_writer() -> None:
        svc.write_page(99, b"blocked")
        released.set()

    thread = threading.Thread(target=blocked_writer)
    thread.start()
    time.sleep(0.3)
    assert not released.is_set()
    gate.set()
    thread.join(3)
    assert released.is_set()


def test_throttle_times_out_when_drain_stalls(service_factory, monkeypatch):
    svc = service_factory(
        capacity=20,
        dirty_low_watermark=1,
        dirty_high_watermark=2,
        write_throttle_timeout=0.2,
        worker_count=1,
    )

    def never(self, page_id, sequence, data):
        time.sleep(10)

    monkeypatch.setattr(FileBlockStorage, "write_page_with_sequence", never)
    svc.write_page(0, b"0")
    svc.write_page(1, b"1")
    start = time.monotonic()
    try:
        svc.write_page(2, b"2")
    except ThrottleTimeoutError:
        assert time.monotonic() - start < 1.0
    else:  # pragma: no cover
        raise AssertionError("expected throttle timeout")


def test_clean_pages_evicted_within_capacity(service_factory):
    svc = service_factory(capacity=3, dirty_low_watermark=2, dirty_high_watermark=3)
    for page_id in range(12):
        svc.write_page(page_id, bytes([page_id]) * 8)
        svc.flush_page(page_id)
    stats = svc.stats()
    assert stats.cached_pages <= 3
    assert stats.evictions >= 9
    for page_id in range(12):
        assert svc.read_page(page_id) == bytes([page_id]) * 8


def test_dirty_pages_are_not_evicted(service_factory, monkeypatch):
    svc = service_factory(
        capacity=4,
        dirty_low_watermark=4,
        dirty_high_watermark=4,
        write_throttle_timeout=0.3,
    )
    gate = threading.Event()
    original = FileBlockStorage.write_page_with_sequence

    def stalled(self, page_id, sequence, data):
        gate.wait(3)
        return original(self, page_id, sequence, data)

    monkeypatch.setattr(FileBlockStorage, "write_page_with_sequence", stalled)
    for page_id in range(4):
        svc.write_page(page_id, b"d" * 4)
    time.sleep(0.2)
    stats = svc.stats()
    assert stats.cached_pages == 4
    assert stats.dirty_pages == 4
    gate.set()
    svc.flush_all()


def test_pinned_page_cannot_be_evicted(service_factory):
    # A page actively written and immediately read must remain coherent even
    # when the cache is under heavy capacity pressure.
    svc = service_factory(
        capacity=2, dirty_low_watermark=1, dirty_high_watermark=2, worker_count=4
    )
    stop = threading.Event()
    errors: list[Exception] = []

    def churn() -> None:
        index = 100
        while not stop.is_set():
            try:
                svc.write_page(index, index.to_bytes(4, "little"))
                svc.flush_page(index)
            except Exception as exc:  # pragma: no cover
                errors.append(exc)
            index += 1

    def hot_reader() -> None:
        while not stop.is_set():
            svc.write_page(1, b"hot")
            assert svc.read_page(1) == b"hot"

    threads = [threading.Thread(target=churn), threading.Thread(target=hot_reader)]
    for thread in threads:
        thread.start()
    time.sleep(0.4)
    stop.set()
    for thread in threads:
        thread.join()
    assert not errors


def test_write_waits_for_frame_when_full_of_dirty_pages(service_factory):
    svc = service_factory(
        capacity=4,
        dirty_low_watermark=4,
        dirty_high_watermark=4,
        write_throttle_timeout=3.0,
        worker_count=2,
    )
    gate = threading.Event()
    original = FileBlockStorage.write_page_with_sequence

    def slow(self, page_id, sequence, data):
        gate.wait(3)
        return original(self, page_id, sequence, data)

    FileBlockStorage.write_page_with_sequence = slow
    for page_id in range(4):
        svc.write_page(page_id, bytes([page_id]) * 4)
    time.sleep(0.2)
    assert svc.stats().cached_pages == 4

    done = threading.Event()

    def fifth_write() -> None:
        # page 5 is a new page needing a frame while the cache is full.
        svc.write_page(5, b"new")
        done.set()

    thread = threading.Thread(target=fifth_write)
    thread.start()
    time.sleep(0.3)
    assert not done.is_set()
    gate.set()
    thread.join(4)
    assert done.is_set()
    assert svc.read_page(5) == b"new"
    FileBlockStorage.write_page_with_sequence = original
    svc.flush_all()
