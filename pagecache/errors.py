"""Exception types raised by the page cache."""


class PageCacheError(Exception):
    """Base class for page cache errors."""


class WritebackError(PageCacheError):
    """A dirty page failed to be persisted to the underlying storage."""


class StorageCorruptionError(PageCacheError):
    """A stored page failed integrity checks and must not be served."""


class ThrottleTimeoutError(PageCacheError):
    """A writer waited too long above the dirty high watermark."""
