"""Write-ahead journal guarding in-flight write-backs."""

import os
import struct
import zlib
from pathlib import Path

_MAGIC = b"JNL1"
_INTENT = 1
_COMMIT = 2
_HEADER = struct.Struct(">4sBQII")  # magic, type, page_id, length, crc32


class WritebackJournal:
    """Records write-back intents so a crash can be rolled forward."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def _append(self, record_type: int, page_id: int, data: bytes) -> None:
        header = _HEADER.pack(_MAGIC, record_type, page_id, len(data), zlib.crc32(data))
        with open(self._path, "ab") as fh:
            fh.write(header)
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())

    def record_intent(self, page_id: int, data: bytes) -> None:
        self._append(_INTENT, page_id, data)

    def record_committed(self, page_id: int) -> None:
        self._append(_COMMIT, page_id, b"")

    def pending(self) -> list[tuple[int, bytes]]:
        """Return intents never committed; a torn tail record is ignored."""
        if not self._path.exists():
            return []
        raw = self._path.read_bytes()
        intents: dict[int, bytes] = {}
        offset = 0
        while offset + _HEADER.size <= len(raw):
            magic, record_type, page_id, length, crc = _HEADER.unpack_from(raw, offset)
            end = offset + _HEADER.size + length
            if magic != _MAGIC or end > len(raw):
                break
            data = raw[offset + _HEADER.size : end]
            if zlib.crc32(data) != crc:
                break
            if record_type == _INTENT:
                intents[page_id] = data
            elif record_type == _COMMIT:
                intents.pop(page_id, None)
            offset = end
        return list(intents.items())

    def clear(self) -> None:
        with open(self._path, "wb") as fh:
            fh.flush()
            os.fsync(fh.fileno())
