"""HTTP service exposing the page cache over FastAPI."""

import base64

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from .cache import PageCache
from .errors import PageCacheError


def create_app(cache: PageCache) -> FastAPI:
    """Build the FastAPI application wrapping the given cache."""
    app = FastAPI()

    @app.exception_handler(PageCacheError)
    def cache_error_handler(_request, exc: PageCacheError) -> JSONResponse:
        return JSONResponse(status_code=500, content={"error": str(exc)})

    @app.get("/pages/{page_id}")
    def read_page(page_id: int) -> dict:
        data = cache.read_page(page_id)
        return {"page_id": page_id, "data": base64.b64encode(data).decode()}

    @app.put("/pages/{page_id}")
    def write_page(page_id: int, body: dict) -> dict:
        data = base64.b64decode(body["data"])
        cache.write_page(page_id, data)
        return {"page_id": page_id, "written": len(data)}

    @app.post("/flush")
    def flush_all() -> dict:
        cache.flush_all()
        return {"flushed": "all"}

    @app.post("/flush/{page_id}")
    def flush_page(page_id: int) -> dict:
        cache.flush_page(page_id)
        return {"flushed": page_id}

    @app.get("/stats")
    def stats() -> dict:
        return cache.stats()

    return app
