# rapide 🐎

Intuitive function caching library for Python

## Features

- Extensible, swappable backend system (choose between SQLite, Redis, or implement your own)
- Multithreading and multiprocessing support out of the box
- Granular, transactional cache invalidations at the domain, function, and argument level
- Auto-generates function result keys from passed arguments
- Handles argument edge cases such as optional args, default args, and inclusion/exclusion of `self` in method functions
- Uses modern Python best practices, fully type hinted

## Use Cases

Rapide will work best in medium sized IO-bound applications.

It allows developers to quickly add and manage caching in their business logic, without having to worry about the particular backend implementation being used to store the cache. You can change the backend in a couple lines of code, enabling easy upgrades as the application grows and infrastructure changes.

This library focuses on functionality over pure performance. It uses `pickle` for serializing objects and `inspect.Signature` for parsing function arguments, so it's not the most bare-metal approach.

## Example Usage

Let's look at a toy example, mimicking common CRUD operations

```python
data = {
    1: "Anna",
    2: "Bethany",
}

def get_customer_name(cid: int) -> str:
    return data[cid]

def set_customer_name(cid: int, name: str) -> None:
    data[cid] = name

def delete_all_customers() -> None:
    data.clear()
```

We can quickly implement caching with the proper invalidation.

```python
# Import rapide

from rapide import create_cache
from rapide.backend.disk import DiskBackend

# Instantiate a disk cache

cache = create_cache(
    domain_key="EXAMPLE_DOMAIN",
    backend=DiskBackend,
    default_ttl=30,
)

# Wrap the GET function, assigning it a function key

@cache.wrap("GET_CUSTOMER_NAME")
def get_customer_name(cid: int) -> str:
    return data[cid]

# Add invalidation to the SET function
# This invalidates only the result associated with that customer id

def set_customer_name(cid: int, name: str) -> None:
    data[cid] = name
    cache.invalidate_result("GET_CUSTOMER_NAME", cid=cid)

# Add invalidation to the DELETE function
# This invalidates all results associated with the GET function

def delete_all_customers() -> None:
    data.clear()
    cache.invalidate_func("GET_CUSTOMER_NAME")
```

Switching to Redis is as simple as changing two lines and updating your ENV variables.

```python
# from rapide.backend.disk import DiskBackend
from rapide.backend.redis import RedisBackend

cache = create_cache(
    domain_key="EXAMPLE_DOMAIN",
#   backend=DiskBackend,
    backend=RedisBackend,
    default_ttl=30,
)
```
