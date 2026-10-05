"""Core page cache: reads, writes, write-back, eviction, stats."""

import threading
import time
from collections import OrderedDict

from .config import CacheConfig
from .errors import PageCacheError, ThrottleTimeoutError, WritebackError
from .journal import WritebackJournal
from .page import PageFrame, PageState
from .storage import BlockStorage


class PageCache:
    """Thread-safe page cache sitting between callers and block storage."""

    def __init__(
        self,
        storage: BlockStorage,
        journal: WritebackJournal,
        config: CacheConfig,
    ) -> None:
        self._storage = storage
        self._journal = journal
        self._config = config
        self._cond = threading.Condition(threading.Lock())
        self._frames: OrderedDict[int, PageFrame] = OrderedDict()
        self._dirty_count = 0
        self._hits = 0
        self._misses = 0
        self._writeback_failure_total = 0
        self._writeback_errors: list[Exception] = []
        self._stop = threading.Event()
        self._writeback_thread: threading.Thread | None = None

    def start(self) -> None:
        for page_id, data in self._journal.pending():
            self._storage.write_page(page_id, data)
        self._journal.clear()
        self._writeback_thread = threading.Thread(
            target=self._writeback_loop, name="pagecache-writeback", daemon=True
        )
        self._writeback_thread.start()

    def close(self) -> None:
        self._stop.set()
        with self._cond:
            self._cond.notify_all()
        if self._writeback_thread is not None:
            self._writeback_thread.join()
        self.flush_all()

    def read_page(self, page_id: int) -> bytes:
        with self._cond:
            frame = self._frames.get(page_id)
            if frame is not None:
                self._hits += 1
                self._frames.move_to_end(page_id)
                return frame.data
            self._misses += 1
        data = self._storage.read_page(page_id)
        with self._cond:
            existing = self._frames.get(page_id)
            if existing is not None:
                self._frames.move_to_end(page_id)
                return existing.data
            self._ensure_capacity_locked()
            frame = PageFrame(page_id, data)
            self._frames[page_id] = frame
            return data

    def write_page(self, page_id: int, data: bytes) -> None:
        if len(data) != self._config.page_size:
            raise ValueError(f"page data must be {self._config.page_size} bytes")
        deadline = time.monotonic() + self._config.throttle_timeout
        with self._cond:
            while self._dirty_count >= self._config.dirty_high_watermark:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ThrottleTimeoutError(
                        "dirty pages above high watermark "
                        f"{self._config.dirty_high_watermark}"
                    )
                self._cond.wait(remaining)
            frame = self._frames.get(page_id)
            if frame is None:
                self._ensure_capacity_locked()
                frame = PageFrame(page_id, data)
                frame.state = PageState.DIRTY
                self._frames[page_id] = frame
                self._dirty_count += 1
            else:
                frame.data = data
                if frame.state is PageState.CLEAN:
                    frame.state = PageState.DIRTY
                    self._dirty_count += 1
                elif frame.state is PageState.WRITING_BACK:
                    frame.state = PageState.DIRTY
            self._frames.move_to_end(page_id)
            if self._dirty_count > self._config.dirty_low_watermark:
                self._cond.notify_all()

    def flush_page(self, page_id: int) -> None:
        with self._cond:
            frame = self._frames.get(page_id)
            if frame is None:
                return
            while frame.state is PageState.WRITING_BACK:
                self._cond.wait()
            if frame.state is PageState.DIRTY:
                frame.state = PageState.WRITING_BACK
                snapshot = frame.data
            else:
                snapshot = None
            error = frame.writeback_error
        if snapshot is not None:
            self._writeback_frame(frame, snapshot)
            error = frame.writeback_error
        if error is not None:
            with self._cond:
                frame.writeback_error = None
                self._writeback_errors = [
                    e for e in self._writeback_errors if e is not error
                ]
            raise WritebackError(f"page {page_id} failed to persist") from error

    def flush_all(self) -> None:
        while True:
            with self._cond:
                if any(
                    f.state is PageState.WRITING_BACK for f in self._frames.values()
                ):
                    self._cond.wait()
                    continue
                batch = [
                    f for f in self._frames.values() if f.state is PageState.DIRTY
                ]
                if not batch:
                    break
                for frame in batch:
                    frame.state = PageState.WRITING_BACK
                snapshots = [(frame, frame.data) for frame in batch]
            for frame, snapshot in snapshots:
                self._writeback_frame(frame, snapshot)
        with self._cond:
            if self._writeback_errors:
                errors = self._writeback_errors
                self._writeback_errors = []
                raise WritebackError(
                    f"{len(errors)} page(s) failed to persist"
                ) from errors[0]

    def stats(self) -> dict[str, int]:
        with self._cond:
            return {
                "hits": self._hits,
                "misses": self._misses,
                "dirty_pages": self._dirty_count,
                "cached_pages": len(self._frames),
                "max_pages": self._config.max_pages,
                "writeback_failures": self._writeback_failure_total,
            }

    def _persist(self, page_id: int, data: bytes) -> None:
        self._journal.record_intent(page_id, data)
        self._storage.write_page(page_id, data)
        self._journal.record_committed(page_id)

    def _writeback_frame(self, frame: PageFrame, snapshot: bytes) -> None:
        error: Exception | None = None
        try:
            self._persist(frame.page_id, snapshot)
        except Exception as exc:  # noqa: BLE001 - reported to flush callers
            error = exc
        with self._cond:
            if error is None:
                if frame.state is PageState.WRITING_BACK:
                    frame.state = PageState.CLEAN
                    self._dirty_count -= 1
                frame.writeback_error = None
            else:
                frame.state = PageState.DIRTY
                frame.writeback_error = error
                self._writeback_errors.append(error)
                self._writeback_failure_total += 1
            self._cond.notify_all()

    def _ensure_capacity_locked(self) -> None:
        while len(self._frames) >= self._config.max_pages:
            victim = next(
                (
                    f
                    for f in self._frames.values()
                    if f.state is PageState.CLEAN and f.pin_count == 0
                ),
                None,
            )
            if victim is not None:
                del self._frames[victim.page_id]
                continue
            victim = next(
                (
                    f
                    for f in self._frames.values()
                    if f.state is PageState.DIRTY and f.pin_count == 0
                ),
                None,
            )
            if victim is not None:
                victim.state = PageState.WRITING_BACK
                try:
                    self._persist(victim.page_id, victim.data)
                except Exception as exc:  # noqa: BLE001 - surfaced to caller
                    victim.state = PageState.DIRTY
                    victim.writeback_error = exc
                    self._writeback_errors.append(exc)
                    self._writeback_failure_total += 1
                    raise WritebackError(
                        f"page {victim.page_id} failed to persist during eviction"
                    ) from exc
                del self._frames[victim.page_id]
                self._dirty_count -= 1
                continue
            if all(f.state is PageState.WRITING_BACK for f in self._frames.values()):
                self._cond.wait(self._config.writeback_interval)
                continue
            raise PageCacheError("no evictable page available")

    def _writeback_loop(self) -> None:
        while not self._stop.is_set():
            with self._cond:
                self._cond.wait(self._config.writeback_interval)
                if self._dirty_count <= self._config.dirty_low_watermark:
                    continue
                batch = [
                    f for f in self._frames.values() if f.state is PageState.DIRTY
                ]
                for frame in batch:
                    frame.state = PageState.WRITING_BACK
                snapshots = [(frame, frame.data) for frame in batch]
            for frame, snapshot in snapshots:
                self._writeback_frame(frame, snapshot)
