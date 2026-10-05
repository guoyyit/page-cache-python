from conftest import PAGE_SIZE


def page(byte: int) -> bytes:
    return bytes([byte]) * PAGE_SIZE


def test_read_miss_loads_zeros_from_storage(cache):
    assert cache.read_page(0) == bytes(PAGE_SIZE)
    stats = cache.stats()
    assert stats["misses"] == 1
    assert stats["hits"] == 0


def test_read_hit_returns_cached_content(cache):
    cache.read_page(1)
    cache.read_page(1)
    stats = cache.stats()
    assert stats["misses"] == 1
    assert stats["hits"] == 1


def test_write_is_immediately_visible(cache):
    cache.write_page(2, page(0xAB))
    assert cache.read_page(2) == page(0xAB)


def test_read_after_write_returns_last_confirmed_write(cache):
    cache.write_page(3, page(1))
    cache.write_page(3, page(2))
    cache.write_page(3, page(3))
    assert cache.read_page(3) == page(3)


def test_write_marks_page_dirty(cache):
    cache.write_page(4, page(9))
    assert cache.stats()["dirty_pages"] == 1


def test_write_rejects_wrong_size(cache):
    try:
        cache.write_page(5, b"too-short")
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError")
