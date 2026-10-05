import pathlib

import pytest

from pagecache import CacheConfig, PageCache
from pagecache.journal import WritebackJournal
from pagecache.storage import LocalFileStorage

PAGE_SIZE = 64


def make_cache(
    directory: pathlib.Path,
    storage=None,
    **config_overrides,
) -> PageCache:
    config = CacheConfig(
        page_size=PAGE_SIZE,
        max_pages=config_overrides.pop("max_pages", 8),
        dirty_low_watermark=config_overrides.pop("dirty_low_watermark", 3),
        dirty_high_watermark=config_overrides.pop("dirty_high_watermark", 6),
        throttle_timeout=config_overrides.pop("throttle_timeout", 2.0),
        writeback_interval=config_overrides.pop("writeback_interval", 0.01),
    )
    if storage is None:
        storage = LocalFileStorage(directory / "pages", PAGE_SIZE)
    cache = PageCache(storage, WritebackJournal(directory / "journal.bin"), config)
    cache.start()
    return cache


@pytest.fixture
def cache(tmp_path):
    instance = make_cache(tmp_path)
    yield instance
    instance.close()
