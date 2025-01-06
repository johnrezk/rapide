import enum
import time

from rapide import Cache, create_cache
from rapide.backend.disk import DiskBackend
from rapide.backend.redis import RedisBackend


def cache_basic(cache: Cache) -> None:
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

    time.sleep(0.75)

    assert get_customer(1) == "John Doe III"

    with cache.lock("testing..."):
        pass

    # TODO do tests for below properly

    cache.flush_expired()
    # cache.invalidate_domain()


def test_disk():
    cache = create_cache(
        domain_key=__name__,
        backend=DiskBackend,
        default_ttl=0.5,
    )
    cache_basic(cache)


def test_redis():
    cache = create_cache(
        domain_key=__name__,
        backend=RedisBackend,
        default_ttl=0.5,
    )
    cache_basic(cache)
