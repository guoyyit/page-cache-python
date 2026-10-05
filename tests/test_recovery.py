"""Crash recovery and corrupt/unconfirmed page handling."""

from __future__ import annotations

import glob
import os

import pytest

from pagecache.errors import CorruptPageError


def test_recovers_last_confirmed_version_after_crash(data_dir, service_factory):
    svc = service_factory(capacity=20000, dirty_low_watermark=10_000, dirty_high_watermark=10_000)
    svc.write_page(1, b"v1")
    svc.write_page(1, b"v2")
    svc.write_page(1, b"v3")
    svc.crash_close()

    reopened = service_factory(directory=data_dir)
    assert reopened.read_page(1) == b"v3"


def test_uncommitted_tail_is_discarded(data_dir, service_factory):
    svc = service_factory(capacity=20000, dirty_low_watermark=10_000, dirty_high_watermark=10_000)
    svc.write_page(4, b"committed")
    svc.crash_close()

    journal = os.path.join(data_dir, "journal.log")
    with open(journal, "ab") as handle:
        handle.write(b"PCJWAL01garbage-tail-bytes")

    reopened = service_factory(directory=data_dir)
    assert reopened.read_page(4) == b"committed"


def test_wal_repairs_torn_page_file(data_dir, service_factory):
    svc = service_factory(capacity=20000, dirty_low_watermark=10_000, dirty_high_watermark=10_000)
    svc.write_page(7, b"confirmed")
    svc.crash_close()

    for path in glob.glob(os.path.join(data_dir, "pages", "*.page")):
        with open(path, "r+b") as handle:
            handle.truncate(12)

    reopened = service_factory(directory=data_dir)
    assert reopened.read_page(7) == b"confirmed"
    reopened.flush_page(7)
    assert reopened.storage_on_disk(7) == b"confirmed"


def test_unfinished_writeback_detected_and_recovered(data_dir, service_factory):
    svc = service_factory(capacity=400, dirty_low_watermark=10, dirty_high_watermark=200)
    svc.write_page(3, b"wal-version")
    svc.crash_close()

    # Simulate a torn in-flight page file left by the interrupted write-back.
    for path in glob.glob(os.path.join(data_dir, "pages", "*.page")):
        with open(path, "r+b") as handle:
            handle.truncate(5)

    reopened = service_factory(directory=data_dir, capacity=400)
    assert reopened.read_page(3) == b"wal-version"


def test_corrupt_page_without_journal_is_quarantined(data_dir, service_factory):
    svc = service_factory()
    svc.write_page(9, b"durable")
    svc.flush_page(9)
    svc.close()

    for path in glob.glob(os.path.join(data_dir, "pages", "*.page")):
        with open(path, "r+b") as handle:
            handle.truncate(12)

    reopened = service_factory(directory=data_dir)
    assert 9 in reopened.invalid_pages()
    with pytest.raises(CorruptPageError):
        reopened.read_page(9)


def test_clean_shutdown_discards_journal(data_dir, service_factory):
    svc = service_factory()
    svc.write_page(2, b"clean")
    svc.close()
    journal = os.path.join(data_dir, "journal.log")
    assert os.path.getsize(journal) == 0


def test_flushed_version_survives_restart(data_dir, service_factory):
    svc = service_factory()
    svc.write_page(11, b"on-disk")
    svc.flush_page(11)
    svc.write_page(12, b"also-disk")
    svc.flush_all()
    svc.close()

    reopened = service_factory(directory=data_dir)
    assert reopened.read_page(11) == b"on-disk"
    assert reopened.read_page(12) == b"also-disk"
    assert reopened.stats().dirty_pages == 0


def test_sequence_monotonic_across_restarts(data_dir, service_factory):
    svc = service_factory(capacity=400, dirty_low_watermark=10, dirty_high_watermark=200)
    for index in range(5):
        svc.write_page(1, f"old-{index}".encode())
    svc.flush_all()
    svc.close()

    reopened = service_factory(directory=data_dir)
    reopened.write_page(1, b"after-restart")
    reopened.flush_page(1)
    assert reopened.storage_on_disk(1) == b"after-restart"

    reopened_again = service_factory(directory=data_dir)
    assert reopened_again.read_page(1) == b"after-restart"
