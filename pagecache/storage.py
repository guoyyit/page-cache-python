"""Underlying block storage abstraction and a local file implementation."""

import os
import struct
import zlib
from pathlib import Path
from typing import Protocol

from .errors import StorageCorruptionError

_MAGIC = b"PGC1"
_HEADER = struct.Struct(">4sQII")  # magic, page_id, length, crc32


class BlockStorage(Protocol):
    """Page-granularity durable storage."""

    def read_page(self, page_id: int) -> bytes: ...

    def write_page(self, page_id: int, data: bytes) -> None: ...

    def flush(self) -> None: ...


class LocalFileStorage:
    """Stores each page as a checksummed file, written atomically."""

    def __init__(self, directory: Path, page_size: int) -> None:
        self._directory = directory
        self._page_size = page_size
        self._directory.mkdir(parents=True, exist_ok=True)

    def _page_path(self, page_id: int) -> Path:
        return self._directory / f"page_{page_id}.bin"

    def read_page(self, page_id: int) -> bytes:
        path = self._page_path(page_id)
        if not path.exists():
            return bytes(self._page_size)
        raw = path.read_bytes()
        if len(raw) < _HEADER.size:
            raise StorageCorruptionError(f"page {page_id}: truncated header")
        magic, stored_id, length, crc = _HEADER.unpack_from(raw)
        data = raw[_HEADER.size :]
        if magic != _MAGIC or stored_id != page_id:
            raise StorageCorruptionError(f"page {page_id}: bad header")
        if length != len(data) or zlib.crc32(data) != crc:
            raise StorageCorruptionError(f"page {page_id}: checksum mismatch")
        return data

    def write_page(self, page_id: int, data: bytes) -> None:
        if len(data) != self._page_size:
            raise ValueError(f"page data must be {self._page_size} bytes")
        header = _HEADER.pack(_MAGIC, page_id, len(data), zlib.crc32(data))
        tmp = self._directory / f"page_{page_id}.tmp"
        with open(tmp, "wb") as fh:
            fh.write(header)
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self._page_path(page_id))
        self.flush()

    def flush(self) -> None:
        dir_fd = os.open(self._directory, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
