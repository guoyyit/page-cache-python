"""HTTP API exposing the page cache service over FastAPI.

Page payloads are transported as base64 strings so arbitrary binary content
round-trips safely through JSON.
"""

from __future__ import annotations

import base64
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from pagecache.config import CacheConfig
from pagecache.errors import (
    CorruptPageError,
    FlushTimeoutError,
    PageCacheError,
    PageNotFoundError,
    ThrottleTimeoutError,
)
from pagecache.service import PageCacheService


class _WriteBody(BaseModel):
    data: str = Field(..., description="base64-encoded page payload")


class PageResponse(BaseModel):
    page_id: int
    data: str


class StatsResponse(BaseModel):
    hits: int = 0
    misses: int = 0
    dirty_pages: int = 0
    cached_pages: int = 0
    capacity: int = 0
    evictions: int = 0
    writebacks: int = 0
    failed_writebacks: int = 0


def create_app(config: CacheConfig | None = None) -> FastAPI:
    service = PageCacheService(config)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        service.start()
        try:
            yield
        finally:
            service.close()

    app = FastAPI(title="Local Page Cache", version="0.1.0", lifespan=lifespan)
    app.state.service = service

    def _register(code: type[PageCacheError], status: int) -> None:
        def handler(_request: Request, exc: PageCacheError):
            raise HTTPException(status_code=status, detail=str(exc))

        app.add_exception_handler(code, handler)

    _register(PageNotFoundError, 404)
    _register(CorruptPageError, 409)
    _register(ThrottleTimeoutError, 503)
    _register(FlushTimeoutError, 504)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/pages/{page_id}", response_model=PageResponse)
    def read_page(page_id: int) -> PageResponse:
        data = service.read_page(page_id)
        return PageResponse(
            page_id=page_id,
            data=base64.b64encode(data).decode("ascii"),
        )

    @app.put("/pages/{page_id}")
    def write_page(page_id: int, body: _WriteBody) -> dict[str, object]:
        try:
            payload = base64.b64decode(body.data, validate=True)
        except (ValueError, TypeError) as exc:
            raise HTTPException(
                status_code=400, detail=f"invalid base64 payload: {exc}"
            )
        service.write_page(page_id, payload)
        return {"page_id": page_id, "accepted": True}

    @app.post("/pages/{page_id}/flush")
    def flush_page(page_id: int) -> dict[str, object]:
        service.flush_page(page_id)
        return {"page_id": page_id, "flushed": True}

    @app.post("/flush")
    def flush_all() -> dict[str, object]:
        service.flush_all()
        return {"flushed": True}

    @app.get("/stats")
    def stats() -> StatsResponse:
        current = service.stats()
        return StatsResponse(
            hits=current.hits,
            misses=current.misses,
            dirty_pages=current.dirty_pages,
            cached_pages=current.cached_pages,
            capacity=current.capacity,
            evictions=current.evictions,
            writebacks=current.writebacks,
            failed_writebacks=current.failed_writebacks,
        )

    @app.get("/invalid-pages")
    def invalid_pages() -> dict[str, list[int]]:
        return {"page_ids": service.invalid_pages()}

    return app



def _default_config() -> CacheConfig | None:
    data_dir = os.environ.get("PAGE_CACHE_DATA_DIR")
    if data_dir is None:
        return None
    return CacheConfig(data_dir=data_dir)


app = create_app(_default_config())
