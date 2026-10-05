"""Backing block storage abstraction and a file-based implementation.

Each page is stored as a separate file containing a small header and the
page payload. Writes are atomic: the payload goes to a temp file in the same
directory, is fsynced, and then renamed over the target, so a crash can only
leave the old complete file or the new complete file, never a torn mix.
"""

from __future__ import annotations

import os
import struct
import tempfile
import zlib
from typing import Protocol

from pagecache.config import CacheConfig
from pagecache.errors import CorruptPageError

_MAGIC = b"PCPAGE01"
_HEADER = struct.Struct(">8sQQQII")
# magic, page_id, sequence, data_length, reserved, crc32(header_and_payload)
_HEADER_RESERVED = 0


class BlockStorage(Protocol):
    def read_page(self, page_id: int) -> bytes | None:
        ...

    def write_page(self, page_id: int, data: bytes) -> None:
        ...

    def flush(self) -> None:
        ...

    def close(self) -> None:
        ...


class FileBlockStorage:
    def __init__(self, config: CacheConfig) -> None:
        self._config = config
        self._dir = os.path.join(config.data_dir, "pages")
        os.makedirs(self._dir, exist_ok=True)

    def _path(self, page_id: int) -> str:
        return os.path.join(self._dir, f"{page_id:020d}.page")

    @staticmethod
    def _encode(page_id: int, sequence: int, data: bytes) -> bytes:
        prefix = _HEADER.pack(
            _MAGIC,
            page_id,
            sequence,
            len(data),
            _HEADER_RESERVED,
            0,
        )
        crc = zlib.crc32(prefix + data) & 0xFFFFFFFF
        header = _HEADER.pack(
            _MAGIC,
            page_id,
            sequence,
            len(data),
            _HEADER_RESERVED,
            crc,
        )
        return header + data

    @staticmethod
    def _decode(blob: bytes) -> tuple[int, int, bytes]:
        if len(blob) < _HEADER.size:
            raise CorruptPageError("page file shorter than header")
        magic, page_id, sequence, length, _reserved, crc = _HEADER.unpack(
            blob[: _HEADER.size]
        )
        if magic != _MAGIC:
            raise CorruptPageError("bad page magic")
        payload = blob[_HEADER.size :]
        if len(payload) != length:
            raise CorruptPageError("page length mismatch (torn file)")
        prefix = _HEADER.pack(
            _MAGIC, page_id, sequence, length, _HEADER_RESERVED, 0
        )
        if (zlib.crc32(prefix + payload) & 0xFFFFFFFF) != crc:
            raise CorruptPageError("page checksum mismatch")
        return page_id, sequence, payload

    def read_page(self, page_id: int) -> bytes | None:
        path = self._path(page_id)
        try:
            with open(path, "rb") as handle:
                blob = handle.read()
        except FileNotFoundError:
            return None
        stored_id, _sequence, data = self._decode(blob)
        if stored_id != page_id:
            raise CorruptPageError(
                f"page id mismatch: file holds {stored_id}, want {page_id}"
            )
        return data

    def read_page_with_sequence(self, page_id: int) -> tuple[int, bytes] | None:
        path = self._path(page_id)
        try:
            with open(path, "rb") as handle:
                blob = handle.read()
        except FileNotFoundError:
            return None
        stored_id, sequence, data = self._decode(blob)
        if stored_id != page_id:
            raise CorruptPageError(
                f"page id mismatch: file holds {stored_id}, want {page_id}"
            )
        return sequence, data

    def list_pages(self) -> list[tuple[int, int]]:
        """Return ``(page_id, sequence)`` for every readable page file."""
        readable, _corrupt = self.scan_pages()
        return readable

    def scan_pages(self) -> tuple[list[tuple[int, int]], list[int]]:
        """Enumerate page files, separating readable ones from corrupt ones.

        Corrupt files are reported by the page id encoded in their filename
        so recovery can quarantine them rather than silently ignoring them.
        """
        found: list[tuple[int, int]] = []
        corrupt: list[int] = []
        for name in os.listdir(self._dir):
            if not name.endswith(".page"):
                continue
            path = os.path.join(self._dir, name)
            try:
                with open(path, "rb") as handle:
                    blob = handle.read()
                page_id, sequence, _data = self._decode(blob)
            except (OSError, CorruptPageError):
                stem = name[: -len(".page")]
                try:
                    corrupt.append(int(stem))
                except ValueError:
                    pass
                continue
            found.append((page_id, sequence))
        return found, corrupt

    def write_page(self, page_id: int, data: bytes) -> None:
        self.write_page_with_sequence(page_id, 0, data)

    def write_page_with_sequence(
        self, page_id: int, sequence: int, data: bytes
    ) -> None:
        blob = self._encode(page_id, sequence, data)
        target = self._path(page_id)
        fd, tmp_path = tempfile.mkstemp(
            prefix=f".{page_id:020d}.", suffix=".tmp", dir=self._dir
        )
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(blob)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, target)
            self._fsync_dir()
        except BaseException:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass
            raise

    def _fsync_dir(self) -> None:
        if not hasattr(os, "O_DIRECTORY"):
            return
        dir_fd = os.open(self._dir, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    def flush(self) -> None:
        self._fsync_dir()

    def close(self) -> None:
        self.flush()
