"""Shared fixtures for the page cache test-suite."""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Iterator

import pytest

from pagecache.config import CacheConfig
from pagecache.service import PageCacheService


@pytest.fixture
def data_dir() -> Iterator[str]:
    path = tempfile.mkdtemp(prefix="pagecache-")
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def make_config(data_dir: str, **overrides) -> CacheConfig:
    defaults = dict(
        data_dir=data_dir,
        page_size=4096,
        capacity=32,
        dirty_low_watermark=4,
        dirty_high_watermark=16,
        write_throttle_timeout=5.0,
        flush_timeout=5.0,
        writeback_retry_interval=0.02,
        worker_count=2,
    )
    defaults.update(overrides)
    return CacheConfig(**defaults)


@pytest.fixture
def service_factory(data_dir):
    created: list[PageCacheService] = []

    def _factory(directory: str | None = None, **overrides) -> PageCacheService:
        config = make_config(directory or data_dir, **overrides)
        svc = PageCacheService(config)
        svc.start()
        created.append(svc)
        return svc

    yield _factory

    for svc in created:
        try:
            svc.close()
        except Exception:
            pass
