from pathlib import Path

from pagecache import CacheConfig, PageCache
from pagecache.journal import WritebackJournal
from pagecache.service import create_app
from pagecache.storage import LocalFileStorage

DATA_DIR = Path(__file__).parent / "data"

config = CacheConfig()
storage = LocalFileStorage(DATA_DIR / "pages", config.page_size)
journal = WritebackJournal(DATA_DIR / "writeback.journal")
cache = PageCache(storage, journal, config)
cache.start()

app = create_app(cache)
