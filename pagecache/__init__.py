"""Local page cache with delayed write-back."""

from .cache import PageCache
from .config import CacheConfig

__all__ = ["PageCache", "CacheConfig"]
