"""In-memory page frame bookkeeping."""

import enum
import threading


class PageState(enum.Enum):
    CLEAN = "clean"
    DIRTY = "dirty"
    WRITING_BACK = "writing_back"


class PageFrame:
    """One cached page with its own lock and metadata."""

    def __init__(self, page_id: int, data: bytes) -> None:
        self.page_id = page_id
        self.data = data
        self.state = PageState.CLEAN
        self.lock = threading.Lock()
        self.pin_count = 0
        self.writeback_error: Exception | None = None
