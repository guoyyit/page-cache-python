"""Background write-back worker pool.

The pool drains a global de-duplicating queue. For every queued page a worker
snapshots the latest confirmed content under the page's ``rw`` lock, then
persists that snapshot while holding the page's ``io`` lock. Because every
persistence attempt for one page takes the same lock, same-page writes land
strictly in sequence and never interleave, so the backing file always
contains one complete version rather than a torn mix.
"""

from __future__ import annotations

import queue
import threading
import time

from pagecache.config import CacheConfig
from pagecache.errors import WritebackFailedError
from pagecache.page import Page


class WritebackPool:
    _SHUTDOWN = object()

    def __init__(self, config: CacheConfig, cache) -> None:
        self._config = config
        self._cache = cache
        self._queue: queue.Queue = queue.Queue()
        self._queued: set[int] = set()
        self._not_before: dict[int, float] = {}
        self._queue_lock = threading.Lock()
        self._workers: list[threading.Thread] = []
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        for index in range(self._config.worker_count):
            thread = threading.Thread(
                target=self._run_worker,
                name=f"writeback-{index}",
                daemon=True,
            )
            thread.start()
            self._workers.append(thread)

    def submit(self, page_id: int) -> None:
        with self._queue_lock:
            if page_id in self._queued:
                self._not_before[page_id] = 0.0
                return
            self._queued.add(page_id)
            self._not_before[page_id] = 0.0
        self._queue.put(page_id)

    def _run_worker(self) -> None:
        while True:
            timeout = None
            with self._queue_lock:
                delays = [
                    value - time.monotonic()
                    for value in self._not_before.values()
                    if value is not None
                ]
            if delays:
                timeout = max(0.01, min(delays))
            try:
                item = self._queue.get(timeout=timeout)
            except queue.Empty:
                continue
            if item is self._SHUTDOWN:
                self._queue.task_done()
                return
            page_id = item
            with self._queue_lock:
                wait_until = self._not_before.pop(page_id, 0.0)
            delay = wait_until - time.monotonic()
            if delay > 0:
                if delay > 0.2:
                    delay = 0.2
                time.sleep(delay)
            try:
                self._write_one(page_id)
            except BaseException as exc:
                with self._cache.lock:
                    page = self._cache.pages.get(page_id)
                    if page is not None:
                        self._cache.complete_writeback(
                            page,
                            page.sequence,
                            WritebackFailedError(str(exc)),
                        )
            finally:
                with self._queue_lock:
                    self._queued.discard(page_id)
                self._queue.task_done()
                self._cache.frame_free()

    def _write_one(self, page_id: int) -> bool:
        pages = self._cache.pages
        with self._cache.lock:
            page: Page | None = pages.get(page_id)
            if page is None or not page.dirty or page.io_inflight:
                return False
            page.io_inflight = True
            page.io_error = None
            with page.rw:
                snapshot = bytes(page.data)
                sequence = page.sequence
        error: BaseException | None = None
        with page.io:
            try:
                self._cache.storage.write_page_with_sequence(
                    page_id, sequence, snapshot
                )
                self._cache.storage.flush()
            except BaseException as exc:
                error = WritebackFailedError(
                    f"writeback of page {page_id} seq {sequence} failed: {exc}"
                )
                error.__cause__ = exc
        self._cache.complete_writeback(page, sequence, error)
        fresh = pages.get(page_id)
        if fresh is not None and fresh.dirty:
            with self._queue_lock:
                if page_id not in self._queued:
                    self._queued.add(page_id)
                    self._not_before[page_id] = (
                        time.monotonic()
                        + (
                            self._config.writeback_retry_interval
                            if error is not None
                            else 0.0
                        )
                    )
                    self._queue.put(page_id)
        return True

    def drain(self, timeout: float) -> None:
        if not self._started:
            return
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._cache.lock:
                pending = any(
                    page.dirty or page.io_inflight
                    for page in self._cache.pages.values()
                )
            with self._queue_lock:
                queued = bool(self._queued)
            if not pending and not queued:
                return
            time.sleep(0.02)

    def shutdown(self) -> None:
        if not self._started:
            return
        deadline = time.monotonic() + self._config.flush_timeout
        while time.monotonic() < deadline:
            with self._queue_lock:
                for page_id in self._not_before:
                    self._not_before[page_id] = 0.0
                busy = bool(self._queued)
            with self._cache.lock:
                inflight = any(
                    page.io_inflight for page in self._cache.pages.values()
                )
            if not busy and not inflight:
                break
            time.sleep(0.02)
        for _ in self._workers:
            self._queue.put(self._SHUTDOWN)
        for worker in self._workers:
            worker.join(timeout=self._config.flush_timeout)
        self._started = False
