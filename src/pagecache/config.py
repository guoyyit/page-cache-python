"""Configuration types for the page cache service."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CacheConfig:
    page_size: int = 4096
    capacity: int = 1024
    dirty_low_watermark: int = 128
    dirty_high_watermark: int = 256
    write_throttle_timeout: float = 30.0
    flush_timeout: float = 60.0
    worker_count: int = 2
    writeback_retry_interval: float = 0.2
    data_dir: str = "./cache-data"
    wal_fsync: bool = True

    def validate(self) -> None:
        if self.page_size <= 0:
            raise ValueError("page_size must be positive")
        if self.capacity <= 0:
            raise ValueError("capacity must be positive")
        if not 0 < self.dirty_low_watermark <= self.dirty_high_watermark:
            raise ValueError(
                "dirty watermarks must satisfy 0 < low <= high"
            )
        if self.dirty_high_watermark > self.capacity:
            raise ValueError("dirty_high_watermark must not exceed capacity")
        if self.write_throttle_timeout <= 0:
            raise ValueError("write_throttle_timeout must be positive")
        if self.flush_timeout <= 0:
            raise ValueError("flush_timeout must be positive")
        if self.worker_count <= 0:
            raise ValueError("worker_count must be positive")
        if self.writeback_retry_interval < 0:
            raise ValueError("writeback_retry_interval must not be negative")
        if not self.data_dir:
            raise ValueError("data_dir must not be empty")
