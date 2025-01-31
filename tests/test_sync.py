import enum
import time
from typing import Type
from uuid import uuid4

import pytest

from rapide import Cache, create_cache
from rapide.backend.disk import DiskBackend
from rapide.backend.redis import RedisBackend
from rapide.shared import Backend


def create_test_cache(backend: Type[Backend]):
    return create_cache(
        domain_key=str(uuid4()),
        backend=backend,
        default_ttl=1,
    )


@pytest.fixture(params=["disk", "redis"])
def cache(request):
    test_cache = create_test_cache(
        DiskBackend if request.param == "disk" else RedisBackend
    )
    yield test_cache
    test_cache.invalidate_all()


def test_basic_ops(cache: Cache):
    cache.invalidate_all()

    customers = {
        1: "John Doe",
        2: "Ben Smith",
    }

    class Key(str, enum.Enum):
        GET_CUSTOMER = "GET_CUSTOMER"

    @cache.wrap(Key.GET_CUSTOMER)
    def get_customer(index: int) -> str:
        return customers[index]

    assert get_customer(1) == "John Doe"
    assert get_customer(2) == "Ben Smith"

    customers[1] = "John Doe II"
    customers[2] = "Ben Smith II"

    assert get_customer(1) == "John Doe"
    assert get_customer(2) == "Ben Smith"

    cache.invalidate_func(Key.GET_CUSTOMER)

    assert get_customer(1) == "John Doe II"
    assert get_customer(2) == "Ben Smith II"

    customers[1] = "John Doe III"
    customers[2] = "Ben Smith III"

    cache.invalidate_result(Key.GET_CUSTOMER, 2)

    assert get_customer(1) == "John Doe II"
    assert get_customer(2) == "Ben Smith III"

    time.sleep(1.5)

    assert get_customer(1) == "John Doe III"

    with cache.lock("testing..."):
        pass

    # TODO do tests for below properly

    cache.flush_expired()
