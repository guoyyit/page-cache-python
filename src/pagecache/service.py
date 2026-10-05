"""Service facade wiring the cache and the background write-back pool."""

from __future__ import annotations

import atexit

from pagecache.cache import CacheStats, PageCache
from pagecache.config import CacheConfig
from pagecache.writeback import WritebackPool


class PageCacheService:
    def __init__(self, config: CacheConfig | None = None) -> None:
        self._config = config or CacheConfig()
        self._cache = PageCache(self._config)
        self._pool = WritebackPool(self._config, self._cache)
        self._cache.set_writeback_submitter(self._pool.submit)
        self._started = False
        self._closed = False
        atexit.register(self._atexit_close)

    def start(self) -> None:
        if self._started:
            return
        self._cache.start()
        self._pool.start()
        self._started = True

    def read_page(self, page_id: int) -> bytes:
        self.start()
        return self._cache.read_page(page_id)

    def write_page(self, page_id: int, data: bytes) -> None:
        self.start()
        self._cache.write_page(page_id, data)

    def flush_page(self, page_id: int) -> None:
        self.start()
        self._cache.flush_page(page_id)

    def flush_all(self) -> None:
        self.start()
        self._cache.flush_all()

    def stats(self) -> CacheStats:
        return self._cache.stats()

    def invalid_pages(self) -> list[int]:
        return list(self._cache.invalid_pages)

    def storage_on_disk(self, page_id: int) -> bytes:
        data = self._cache.storage.read_page(page_id)
        if data is None:
            from pagecache.errors import PageNotFoundError

            raise PageNotFoundError(f"page {page_id} missing on disk")
        return data

    def storage_try_read(self, page_id: int) -> bytes | None:
        return self._cache.storage.read_page(page_id)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._started:
            try:
                self._cache.flush_all()
            finally:
                self._pool.shutdown()
            self._cache.discard_journal()
        self._cache.close(flush=False)

    def crash_close(self) -> None:
        """Stop without flushing, simulating a process crash."""
        if self._closed:
            return
        self._closed = True
        self._pool.shutdown()
        self._cache.close(flush=False)

    def _atexit_close(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def __enter__(self) -> PageCacheService:
        self.start()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()
