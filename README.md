# rapide

Intuitive function caching library for Python

## Features

- Developer-friendly interface
- Extensible, swappable backend system (choose between SQLite, Redis, or implement your own)
- Multithreading and multiprocessing support
- Granular, transactional cache invalidation at the domain, function, and argument level
- Handles argument edge cases, such as optional keyword arguments and inclusion/exclusion of `self` in method functions

## Use Cases

Rapide will work best in small to medium sized IO-bound applications.

It allows developers to quickly add and manage caching in their business logic, without having to worry about the particular backend implementation being used to store the cache. You can change the backend in a single line of code, enabling easy upgrades as the application grows and infrastructure changes.

This library focuses on functionality over pure performance. It uses `pickle` for serializing objects and `inspect.Signature` for parsing function arguments, so it's not the most bare-metal approach.
