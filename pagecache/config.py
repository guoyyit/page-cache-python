"""Configuration for the page cache service."""

from dataclasses import dataclass


@dataclass(frozen=True)
class CacheConfig:
    """Tunable parameters of the page cache."""

    page_size: int = 4096
    max_pages: int = 64
    dirty_low_watermark: int = 8
    dirty_high_watermark: int = 16
    throttle_timeout: float = 5.0
    writeback_interval: float = 0.05
