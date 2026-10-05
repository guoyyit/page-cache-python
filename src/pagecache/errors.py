"""Exception types raised by the page cache service."""

from __future__ import annotations


class PageCacheError(Exception):
    """Base class for all page cache errors."""


class PageNotFoundError(PageCacheError):
    """Raised when a page id exists neither in cache nor backing store."""


class CacheCapacityError(PageCacheError):
    """Raised when no free page frame can be obtained within the timeout."""


class ThrottleTimeoutError(PageCacheError):
    """Raised when a writer waits too long above the high watermark."""


class WritebackFailedError(PageCacheError):
    """Raised when persisting a dirty page to the backing store fails."""


class FlushTimeoutError(PageCacheError):
    """Raised when a flush cannot finish within the configured timeout."""


class CorruptPageError(PageCacheError):
    """Raised when recovered page data is torn or cannot be verified."""
