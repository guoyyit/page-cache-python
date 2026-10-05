from conftest import PAGE_SIZE, make_cache

from pagecache.errors import StorageCorruptionError
from pagecache.journal import WritebackJournal
from pagecache.storage import LocalFileStorage


def page(byte: int) -> bytes:
    return bytes([byte]) * PAGE_SIZE


def test_restart_recovers_flushed_data(tmp_path):
    cache = make_cache(tmp_path)
    cache.write_page(0, page(1))
    cache.write_page(1, page(2))
    cache.flush_all()
    cache.close()
    reopened = make_cache(tmp_path)
    assert reopened.read_page(0) == page(1)
    assert reopened.read_page(1) == page(2)
    reopened.close()


def test_unflushed_write_may_be_lost_but_flush_never_rolls_back(tmp_path):
    cache = make_cache(tmp_path)
    cache.write_page(0, page(1))
    cache.flush_page(0)
    cache.write_page(0, page(2))
    cache._stop.set()
    cache._writeback_thread.join()
    reopened = make_cache(tmp_path)
    assert reopened.read_page(0) == page(1)
    reopened.close()


def test_crash_mid_writeback_replays_journal(tmp_path):
    journal = WritebackJournal(tmp_path / "journal.bin")
    journal.record_intent(3, page(9))
    reopened = make_cache(tmp_path)
    storage = LocalFileStorage(tmp_path / "pages", PAGE_SIZE)
    assert storage.read_page(3) == page(9)
    assert journal.pending() == []
    reopened.close()


def test_torn_journal_tail_is_ignored(tmp_path):
    journal = WritebackJournal(tmp_path / "journal.bin")
    journal.record_intent(4, page(5))
    journal.record_committed(4)
    with open(tmp_path / "journal.bin", "ab") as fh:
        fh.write(b"\x00\x01partial")
    assert journal.pending() == []


def test_corrupted_stored_page_is_not_served(tmp_path):
    cache = make_cache(tmp_path)
    cache.write_page(0, page(3))
    cache.flush_all()
    cache.close()
    stored = tmp_path / "pages" / "page_0.bin"
    raw = bytearray(stored.read_bytes())
    raw[-1] ^= 0xFF
    stored.write_bytes(bytes(raw))
    reopened = make_cache(tmp_path)
    try:
        reopened.read_page(0)
    except StorageCorruptionError:
        pass
    else:
        raise AssertionError("expected StorageCorruptionError")
    reopened.close()
