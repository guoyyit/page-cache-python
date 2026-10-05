"""Write-ahead journal used for crash recovery of confirmed writes.

Record layout (all integers big-endian):

    magic(8) type(1) flags(1) length(2) page_id(8) sequence(8) payload crc(4)

where ``crc`` covers every preceding byte of the record. Record types:

* ``WRITE``      - payload for a new page version (followed by COMMIT)
* ``COMMIT``     - marks the WRITE with the same (page_id, sequence) confirmed
* ``CHECKPOINT`` - that version is now persisted in the backing store

A write is acknowledged only after its WRITE+COMMIT records have been fsynced.
Replay returns the highest confirmed, not-yet-checkpointed version per page.
"""

from __future__ import annotations

import os
import struct
import zlib
from threading import RLock

from pagecache.config import CacheConfig
from pagecache.errors import CorruptPageError

MAGIC = b"PCJWAL01"
REC_WRITE = 1
REC_COMMIT = 2
REC_CHECKPOINT = 3

_HEADER = struct.Struct(">8sBHHQQ")
_CRC = struct.Struct(">I")
_RECORD_SIZE = _HEADER.size + _CRC.size


class Journal:
    def __init__(self, config: CacheConfig) -> None:
        self._config = config
        self._dir = config.data_dir
        os.makedirs(self._dir, exist_ok=True)
        self._path = os.path.join(self._dir, "journal.log")
        self._lock = RLock()
        self._file = open(self._path, "ab")

    def _append(self, rec_type: int, page_id: int, sequence: int,
                payload: bytes = b"") -> None:
        if len(payload) > 0xFFFF:
            raise ValueError("journal payload too large")
        prefix = _HEADER.pack(
            MAGIC, rec_type, 0, len(payload), page_id, sequence
        )
        crc = zlib.crc32(prefix + payload) & 0xFFFFFFFF
        record = prefix + payload + _CRC.pack(crc)
        with self._lock:
            self._file.write(record)
            self._file.flush()
            os.fsync(self._file.fileno())

    def append_write(self, page_id: int, sequence: int, data: bytes) -> None:
        self._append(REC_WRITE, page_id, sequence, data)

    def append_checkpoint(self, page_id: int, sequence: int) -> None:
        self._append(REC_CHECKPOINT, page_id, sequence)

    def append_commit(self, page_id: int, sequence: int) -> None:
        self._append(REC_COMMIT, page_id, sequence)

    def replay(self) -> dict[int, tuple[int, bytes]]:
        """Return confirmed versions newer than the last checkpoint per page."""
        with self._lock:
            self._file.flush()
            try:
                with open(self._path, "rb") as handle:
                    blob = handle.read()
            except FileNotFoundError:
                return {}
        pending: dict[tuple[int, int], bytes] = {}
        committed: dict[tuple[int, int], bool] = {}
        checkpointed: dict[int, int] = {}
        offset = 0
        while offset < len(blob):
            if len(blob) - offset < _RECORD_SIZE:
                break
            prefix = blob[offset : offset + _HEADER.size]
            magic, rec_type, _flags, length, page_id, sequence = (
                _HEADER.unpack(prefix)
            )
            end = offset + _RECORD_SIZE + length
            if magic != MAGIC or end > len(blob):
                self._truncate(offset)
                break
            payload = blob[offset + _HEADER.size : offset + _HEADER.size + length]
            (crc,) = _CRC.unpack(
                blob[offset + _HEADER.size + length : end]
            )
            expected = zlib.crc32(prefix + payload) & 0xFFFFFFFF
            if crc != expected:
                self._truncate(offset)
                break
            if rec_type == REC_WRITE:
                pending[(page_id, sequence)] = payload
            elif rec_type == REC_COMMIT:
                committed[(page_id, sequence)] = (
                    (page_id, sequence) in pending
                )
            elif rec_type == REC_CHECKPOINT:
                checkpointed[page_id] = max(
                    sequence, checkpointed.get(page_id, -1)
                )
            offset = end
        result: dict[int, tuple[int, bytes]] = {}
        for (page_id, sequence), has_payload in committed.items():
            if sequence <= checkpointed.get(page_id, -1):
                continue
            if not has_payload:
                continue
            if page_id in result and result[page_id][0] >= sequence:
                continue
            result[page_id] = (sequence, pending[(page_id, sequence)])
        return result

    def _truncate(self, offset: int) -> None:
        """Drop a torn/corrupt tail discovered during replay."""
        with self._lock:
            self._file.close()
            with open(self._path, "r+b") as handle:
                handle.truncate(offset)
            self._file = open(self._path, "ab")

    def reset(self) -> None:
        with self._lock:
            self._file.close()
            with open(self._path, "wb"):
                pass
            self._file = open(self._path, "ab")
            self._file.flush()
            os.fsync(self._file.fileno())

    def close(self) -> None:
        with self._lock:
            self._file.flush()
            os.fsync(self._file.fileno())
            self._file.close()

    def destroy(self) -> None:
        """Remove the journal file (used after a clean shutdown)."""
        with self._lock:
            try:
                self._file.close()
            except OSError:
                pass
            try:
                os.unlink(self._path)
            except FileNotFoundError:
                pass
            self._file = open(self._path, "ab")
