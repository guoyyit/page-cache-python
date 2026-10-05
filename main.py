from pagecache.api import app


@app.get("/")
def read_root():
    return {"service": "page-cache", "status": "ok"}
