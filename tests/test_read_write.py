"""Hit/miss semantics and immediate visibility of confirmed writes."""

from __future__ import annotations

import pytest

from pagecache.errors import PageNotFoundError


def test_read_missing_page_raises(service_factory):
    svc = service_factory()
    with pytest.raises(PageNotFoundError):
        svc.read_page(4242)


def test_write_then_read_is_immediately_visible(service_factory):
    svc = service_factory()
    svc.write_page(1, b"first")
    assert svc.read_page(1) == b"first"


def test_last_confirmed_write_wins(service_factory):
    svc = service_factory()
    for index in range(50):
        svc.write_page(7, f"version-{index}".encode())
    assert svc.read_page(7) == b"version-49"


def test_hit_and_miss_counters(service_factory):
    svc = service_factory()
    svc.write_page(1, b"a")
    svc.read_page(1)  # hit
    svc.read_page(1)  # hit
    stats = svc.stats()
    assert stats.hits == 2


def test_miss_loads_from_backing_store(service_factory):
    svc = service_factory()
    svc.write_page(3, b"persisted")
    svc.flush_page(3)
    assert svc.read_page(3) == b"persisted"


def test_payload_size_limited_to_page_size(service_factory):
    svc = service_factory(page_size=8)
    with pytest.raises(ValueError):
        svc.write_page(1, b"x" * 9)
    svc.write_page(1, b"x" * 8)
    assert svc.read_page(1) == b"x" * 8


def test_distinct_pages_are_independent(service_factory):
    svc = service_factory()
    svc.write_page(1, b"one")
    svc.write_page(2, b"two")
    assert svc.read_page(1) == b"one"
    assert svc.read_page(2) == b"two"
