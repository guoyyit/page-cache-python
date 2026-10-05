"""Per-page value object and its synchronization primitives."""

from __future__ import annotations

from threading import Condition, Lock, RLock


class Page:
    """Cached page state.

    Two locks guard a page:

    * ``rw`` is reentrant and serializes cache-side read/write/eviction
      decisions for the page (all fields except the writeback bookkeeping).
    * ``io`` serializes actual writes to the backing store so that same-page
      writebacks land strictly in order and never overlap.
    """

    __slots__ = (
        "page_id",
        "data",
        "dirty",
        "sequence",
        "pin_count",
        "on_disk_sequence",
        "valid",
        "rw",
        "io",
        "io_done",
        "io_inflight",
        "io_error",
        "last_used",
    )

    def __init__(self, page_id: int, data: bytes = b"") -> None:
        self.page_id = page_id
        self.data = data
        self.dirty = False
        self.sequence = 0
        self.pin_count = 0
        self.on_disk_sequence = 0
        self.valid = True
        self.rw: RLock = RLock()
        self.io: Lock = Lock()
        self.io_done: Condition = Condition()
        self.io_inflight = False
        self.io_error: BaseException | None = None
        self.last_used = 0

    def is_stable(self) -> bool:
        """True when the in-memory content matches the persisted content."""
        return not self.dirty and self.sequence == self.on_disk_sequence
