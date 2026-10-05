"""Core in-memory page cache: lookup, read/write, eviction, statistics."""

from __future__ import annotations

import itertools
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from threading import Condition

from pagecache.config import CacheConfig
from pagecache.errors import (
    CacheCapacityError,
    CorruptPageError,
    PageNotFoundError,
)
from pagecache.journal import Journal
from pagecache.page import Page
from pagecache.storage import FileBlockStorage


@dataclass
class CacheStats:
    hits: int = 0
    misses: int = 0
    dirty_pages: int = 0
    cached_pages: int = 0
    capacity: int = 0
    evictions: int = 0
    writebacks: int = 0
    failed_writebacks: int = 0


class InvalidPages:
    """Page ids whose on-disk state could not be verified at startup."""

    def __init__(self, ids: set[int] | None = None) -> None:
        self._ids = set(ids or ())

    def __contains__(self, page_id: int) -> bool:
        return page_id in self._ids

    def __iter__(self):
        return iter(sorted(self._ids))

    def __len__(self) -> int:
        return len(self._ids)


class PageCache:
    def __init__(self, config: CacheConfig | None = None) -> None:
        self._config = config or CacheConfig()
        self._config.validate()
        self._pages: dict[int, Page] = {}
        self._invalid = InvalidPages()
        self._lru: OrderedDict[int, None] = OrderedDict()
        self._lock = threading.RLock()
        self._throttle = Condition(self._lock)
        self._sequences = itertools.count(1)
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._writebacks = 0
        self._failed_writebacks = 0
        self._storage = FileBlockStorage(self._config)
        self._journal = Journal(self._config)
        self._submit_writeback = lambda _page_id: None
        self._running = False

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._recover()
            self._running = True

    def set_writeback_submitter(self, submitter) -> None:
        self._submit_writeback = submitter

    def _recover(self) -> None:
        journal_versions = self._journal.replay()
        invalid: set[int] = set()
        max_sequence = 0
        for page_id, (sequence, data) in journal_versions.items():
            max_sequence = max(max_sequence, sequence)
            try:
                stored = self._storage.read_page_with_sequence(page_id)
            except CorruptPageError:
                stored = None
            if stored is None:
                page = Page(page_id, data)
                page.sequence = sequence
                page.on_disk_sequence = 0
                page.dirty = True
                self._pages[page_id] = page
                self._lru[page_id] = None
                continue
            stored_sequence, stored_data = stored
            if stored_sequence > sequence:
                page = Page(page_id, stored_data)
                page.sequence = stored_sequence
                page.on_disk_sequence = stored_sequence
                self._pages[page_id] = page
            elif stored_sequence == sequence and stored_data == data:
                page = Page(page_id, data)
                page.sequence = sequence
                page.on_disk_sequence = sequence
                self._pages[page_id] = page
                self._journal.append_checkpoint(page_id, sequence)
            else:
                page = Page(page_id, data)
                page.sequence = sequence
                page.on_disk_sequence = stored_sequence
                page.dirty = True
                self._pages[page_id] = page
                self._lru[page_id] = None
        readable_pages, corrupt_files = self._storage.scan_pages()
        for page_id, _sequence in readable_pages:
            if page_id in self._pages:
                continue
            try:
                result = self._storage.read_page_with_sequence(page_id)
            except CorruptPageError:
                invalid.add(page_id)
                continue
            if result is None:
                continue
            stored_sequence, data = result
            max_sequence = max(max_sequence, stored_sequence)
            page = Page(page_id, data)
            page.sequence = stored_sequence
            page.on_disk_sequence = stored_sequence
            self._pages[page_id] = page
        # A corrupt file is acceptable when the journal holds a confirmed,
        # newer version: the dirty page will overwrite it. Without such a
        # version the page is quarantined and never served as valid data.
        for page_id in corrupt_files:
            if page_id not in self._pages:
                invalid.add(page_id)
        self._invalid = InvalidPages(invalid)
        self._sequences = itertools.count(max_sequence + 1)

    def _touch_lru_locked(self, page_id: int) -> None:
        if page_id in self._lru:
            self._lru.move_to_end(page_id)

    def _dirty_count_locked(self) -> int:
        return sum(1 for page in self._pages.values() if page.dirty)

    def _get_or_load_locked(self, page_id: int) -> Page:
        page = self._pages.get(page_id)
        if page is not None:
            return page
        stored = self._storage.read_page_with_sequence(page_id)
        if stored is None:
            raise PageNotFoundError(f"page {page_id} does not exist")
        sequence, data = stored
        page = Page(page_id, data)
        page.sequence = sequence
        page.on_disk_sequence = sequence
        self._pages[page_id] = page
        return page

    def _evict_clean_locked(self) -> bool:
        """Drop one clean, unpinned page. Return True if a frame was freed."""
        for candidate_id in list(self._lru):
            candidate = self._pages.get(candidate_id)
            if candidate is None:
                self._lru.pop(candidate_id, None)
                continue
            if candidate.dirty or candidate.pin_count > 0 or candidate.io_inflight:
                continue
            del self._pages[candidate_id]
            self._lru.pop(candidate_id, None)
            self._evictions += 1
            return True
        return False

    def _ensure_frame_locked(self, page_id: int, deadline: float) -> None:
        """Make room for one new page, waiting on dirty-page write-back."""
        if len(self._pages) < self._config.capacity:
            return
        while len(self._pages) >= self._config.capacity:
            if self._evict_clean_locked():
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise CacheCapacityError(
                    "no free page frame before timeout"
                )
            for dirty_id, dirty_page in self._pages.items():
                if dirty_id != page_id and dirty_page.dirty:
                    self._submit_writeback(dirty_id)
            self._throttle.wait(
                min(remaining, 0.05)
            )

    def read_page(self, page_id: int) -> bytes:
        with self._lock:
            if page_id in self._invalid:
                raise CorruptPageError(
                    f"page {page_id} is corrupt and unconfirmed"
                )
            page = self._pages.get(page_id)
            if page is not None:
                with page.rw:
                    page.pin_count += 1
                    self._hits += 1
                    self._touch_lru_locked(page_id)
                    payload = data_copy(page.data)
                    page.pin_count -= 1
                return payload
            self._misses += 1
            deadline = time.monotonic() + self._config.write_throttle_timeout
            self._ensure_frame_locked(page_id, deadline)
            page = self._get_or_load_locked(page_id)
            with page.rw:
                page.pin_count += 1
                payload = data_copy(page.data)
                page.pin_count -= 1
            self._lru[page_id] = None
            return payload

    def write_page(self, page_id: int, data: bytes) -> None:
        data = bytes(data)
        if len(data) > self._config.page_size:
            raise ValueError(
                f"payload {len(data)} exceeds page size {self._config.page_size}"
            )
        with self._lock:
            if page_id in self._invalid:
                raise CorruptPageError(
                    f"page {page_id} is corrupt and unconfirmed"
                )
            deadline = time.monotonic() + self._config.write_throttle_timeout
            self._wait_below_high_watermark_locked(deadline)
            page = self._pages.get(page_id)
            if page is None:
                self._ensure_frame_locked(page_id, deadline)
                existing = self._storage.read_page_with_sequence(page_id)
                if existing is not None:
                    disk_sequence, disk_data = existing
                    page = Page(page_id, disk_data)
                    page.sequence = disk_sequence
                    page.on_disk_sequence = disk_sequence
                else:
                    page = Page(page_id)
                self._pages[page_id] = page
                self._lru[page_id] = None
            with page.rw:
                page.pin_count += 1
                sequence = next(self._sequences)
                page.data = data
                page.dirty = True
                page.sequence = sequence
                page.last_used = time.monotonic_ns()
                self._touch_lru_locked(page_id)
                self._journal.append_write(page_id, sequence, data)
                self._journal.append_commit(page_id, sequence)
                page.pin_count -= 1
            dirty = self._dirty_count_locked()
            should_kick = dirty >= self._config.dirty_low_watermark
        if should_kick:
            for dirty_id, dirty_page in self._pages.items():
                if dirty_page.dirty and not dirty_page.io_inflight:
                    self._submit_writeback(dirty_id)

    def _wait_below_high_watermark_locked(self, deadline: float) -> None:
        high = self._config.dirty_high_watermark
        while self._dirty_count_locked() >= high:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                from pagecache.errors import ThrottleTimeoutError

                raise ThrottleTimeoutError(
                    "dirty pages above high watermark after timeout"
                )
            for dirty_id, dirty_page in self._pages.items():
                if dirty_page.dirty:
                    self._submit_writeback(dirty_id)
            self._throttle.wait(min(remaining, 0.05))

    def wait_page_stable(self, page: Page, deadline: float) -> None:
        """Wait until the page has no dirty content and no writeback running."""
        with page.io_done:
            while page.dirty or page.io_inflight:
                if page.io_error is not None:
                    raise page.io_error
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    from pagecache.errors import FlushTimeoutError

                    raise FlushTimeoutError(
                        f"page {page.page_id} not flushed before timeout"
                    )
                page.io_done.wait(min(remaining, 0.05))
            if page.io_error is not None:
                raise page.io_error

    def snapshot_dirty_ids(self) -> list[int]:
        with self._lock:
            return [
                page_id
                for page_id, page in self._pages.items()
                if page.dirty and not page.io_inflight
            ]

    def flush_page(self, page_id: int) -> None:
        deadline = time.monotonic() + self._config.flush_timeout
        with self._lock:
            page = self._pages.get(page_id)
            if page is None and page_id not in self._invalid:
                page = self._get_or_load_locked(page_id)
                self._lru.setdefault(page_id, None)
        if page is None:
            return
        submitted = False
        while True:
            with self._lock:
                with page.rw:
                    if not submitted and page.dirty and not page.io_inflight:
                        self._submit_writeback(page_id)
                        submitted = True
            with page.io_done:
                while page.io_inflight:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    page.io_done.wait(min(remaining, 0.02))
            now = time.monotonic()
            if not page.dirty:
                if page.io_error is not None:
                    raise page.io_error
                return
            if now >= deadline:
                if page.io_error is not None:
                    raise page.io_error
                from pagecache.errors import FlushTimeoutError

                raise FlushTimeoutError(
                    f"page {page_id} not flushed before timeout"
                )
            time.sleep(
                min(
                    deadline - time.monotonic(),
                    max(0.005, self._config.writeback_retry_interval),
                )
            )

    def flush_all(self) -> None:
        deadline = time.monotonic() + self._config.flush_timeout
        while True:
            with self._lock:
                dirty_ids = self.snapshot_dirty_ids()
                inflight = [
                    page for page in self._pages.values() if page.io_inflight
                ]
                for page_id in dirty_ids:
                    self._submit_writeback(page_id)
                pending = [
                    page
                    for page in self._pages.values()
                    if page.dirty or page.io_inflight
                ]
            for page in pending:
                self.wait_page_stable(page, deadline)
            with self._lock:
                remaining_dirty = self.snapshot_dirty_ids()
                remaining_inflight = [
                    page
                    for page in self._pages.values()
                    if page.io_inflight
                ]
                if not remaining_dirty and not remaining_inflight:
                    errors = [
                        page.io_error
                        for page in self._pages.values()
                        if page.io_error is not None
                    ]
                    if errors:
                        raise errors[0]
                    self._storage.flush()
                    return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                from pagecache.errors import FlushTimeoutError

                raise FlushTimeoutError("flush_all timed out")

    def stats(self) -> CacheStats:
        with self._lock:
            return CacheStats(
                hits=self._hits,
                misses=self._misses,
                dirty_pages=self._dirty_count_locked(),
                cached_pages=len(self._pages),
                capacity=self._config.capacity,
                evictions=self._evictions,
                writebacks=self._writebacks,
                failed_writebacks=self._failed_writebacks,
            )

    @property
    def config(self) -> CacheConfig:
        return self._config

    @property
    def invalid_pages(self) -> InvalidPages:
        return self._invalid

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    @property
    def pages(self) -> dict[int, Page]:
        return self._pages

    @property
    def storage(self) -> FileBlockStorage:
        return self._storage

    @property
    def journal(self) -> Journal:
        return self._journal

    def frame_free(self) -> None:
        with self._throttle:
            self._throttle.notify_all()

    def complete_writeback(
        self,
        page: Page,
        sequence: int,
        error: BaseException | None,
    ) -> None:
        """Called by the write-back pool after a persistence attempt."""
        with self._lock:
            with page.io_done:
                page.io_inflight = False
                if error is None:
                    page.io_error = None
                    page.on_disk_sequence = max(
                        page.on_disk_sequence, sequence
                    )
                    if page.sequence == sequence:
                        page.dirty = False
                    self._writebacks += 1
                    self._journal.append_checkpoint(page.page_id, sequence)
                else:
                    page.io_error = error
                    self._failed_writebacks += 1
                page.io_done.notify_all()
            self._throttle.notify_all()

    def close(self, flush: bool = True, discard_journal: bool = False) -> None:
        with self._lock:
            self._running = False
        if flush:
            self.flush_all()
            if discard_journal:
                self._journal.reset()
        self._journal.close()

    def discard_journal(self) -> None:
        """Remove the journal after every confirmed version is checkpointed."""
        self._journal.reset()


def data_copy(data: bytes) -> bytes:
    return bytes(data)
