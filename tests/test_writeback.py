"""Delayed write-back ordering, atomicity and error reporting."""

from __future__ import annotations

import threading
import time

import pytest

from pagecache.errors import WritebackFailedError
from pagecache.storage import FileBlockStorage


def test_flush_persists_to_backing_store(service_factory):
    svc = service_factory()
    svc.write_page(5, b"durable")
    svc.flush_page(5)
    assert svc.storage_on_disk(5) == b"durable"


def test_repeated_writes_flush_in_order(service_factory):
    svc = service_factory()
    for index in range(20):
        svc.write_page(9, bytes([index]) * 16)
    svc.flush_page(9)
    assert svc.storage_on_disk(9) == bytes([19]) * 16


def test_concurrent_same_page_flush_is_one_complete_version(service_factory):
    svc = service_factory(worker_count=4)
    stop = threading.Event()
    observed: set[int] = set()

    def writer(token: int) -> None:
        seq = 0
        while not stop.is_set():
            svc.write_page(1, bytes([token]) * 64 + seq.to_bytes(8, "little"))
            seq += 1

    def checker() -> None:
        while not stop.is_set():
            data = svc.storage_try_read(1)
            if data is None:
                continue
            observed.add(len(data))
            assert len(data) == 72
            assert len(set(data[:64])) == 1

    threads = [
        threading.Thread(target=writer, args=(0x41,)),
        threading.Thread(target=writer, args=(0x42,)),
        threading.Thread(target=checker),
    ]
    for thread in threads:
        thread.start()
    time.sleep(0.5)
    stop.set()
    for thread in threads:
        thread.join()
    svc.flush_page(1)
    final = svc.storage_on_disk(1)
    assert len(final) == 72 and len(set(final[:64])) == 1


def test_writeback_failure_is_reported(service_factory, monkeypatch):
    svc = service_factory(flush_timeout=1.0)
    svc.write_page(2, b"x")

    def always_fail(self, page_id, sequence, data):
        raise OSError("simulated disk failure")

    monkeypatch.setattr(FileBlockStorage, "write_page_with_sequence", always_fail)
    with pytest.raises(WritebackFailedError):
        svc.flush_page(2)


def test_writeback_retry_succeeds_after_recovery(service_factory, monkeypatch):
    svc = service_factory(flush_timeout=2.0, writeback_retry_interval=0.05)
    state = {"fail": True}
    original = FileBlockStorage.write_page_with_sequence

    def flaky(self, page_id, sequence, data):
        if state["fail"]:
            raise OSError("transient")
        return original(self, page_id, sequence, data)

    monkeypatch.setattr(FileBlockStorage, "write_page_with_sequence", flaky)
    svc.write_page(3, b"retry-me")
    with pytest.raises(WritebackFailedError):
        svc.flush_page(3)
    state["fail"] = False
    svc.flush_page(3)
    assert svc.storage_on_disk(3) == b"retry-me"


def test_flush_all_persists_every_dirty_page(service_factory):
    svc = service_factory()
    for page_id in range(10):
        svc.write_page(page_id, f"p{page_id}".encode())
    svc.flush_all()
    assert svc.stats().dirty_pages == 0
    for page_id in range(10):
        assert svc.storage_on_disk(page_id) == f"p{page_id}".encode()
