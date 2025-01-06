import inspect
import logging
import pickle
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import partial, wraps
from typing import Any, Callable, Generic, Iterable, ParamSpec, Type, TypeVar, cast

import xxhash

from .shared import (
    ArgKey,
    Backend,
    BackendConfig,
    DomainKey,
    FuncKey,
    GroupKey,
    ResultKey,
    UnlockKey,
)

_logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class ArgKeyGen:
    sig: inspect.Signature
    is_method: bool
    param_cnt: int

    @classmethod
    def from_func(cls, func: Callable[..., Any]) -> "ArgKeyGen":
        sig = inspect.signature(func)
        param_names = tuple(sig.parameters.keys())
        is_method = len(param_names) > 0 and param_names[0] == "self"
        if hasattr(func, "__self__"):
            raise RuntimeError("unexpected bound function")
        return cls(sig, is_method, len(param_names))

    def get_key(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> "ArgKey":
        """
        If func is a method, never pass self argument
        """
        if self.is_method:
            args = (None,) + args
        bound_args = self.sig.bind(*args, **kwargs)
        bound_args.apply_defaults()

        arg_dict = dict(bound_args.arguments)
        if self.is_method:
            arg_dict.pop("self", None)

        arg_pairs = [(k, v) for k, v in arg_dict.items()]
        arg_pairs.sort(key=lambda x: x[0])
        key = xxhash.xxh3_128_hexdigest(repr(arg_pairs), seed=42)
        return ArgKey(key)


@dataclass(slots=True)
class CacheLock:
    lock_key: str
    backend: Backend[Any]
    unlock_key: UnlockKey | None = None

    def __enter__(self) -> None:
        self.unlock_key = self.backend.aquire_lock(self.lock_key)

    def __exit__(self, *_: Any) -> None:
        assert self.unlock_key is not None
        self.backend.release_lock(self.unlock_key)


_P = ParamSpec("_P")
_R = TypeVar("_R")
_C = TypeVar("_C", bound=BackendConfig)


@dataclass
class Cache(Generic[_C]):
    _domain_key: DomainKey
    _backend_type: Type[Backend[_C]]
    _backend_val: Backend[_C] | None
    _backend_config: _C | None
    _backend_lock: threading.Lock
    _default_ttl: timedelta
    _arg_key_gens: dict[GroupKey, ArgKeyGen]
    _include_self: bool

    def _get_backend(self) -> Backend[_C]:
        with self._backend_lock:
            if self._backend_val:
                return self._backend_val
            cfg = self._backend_config or self._backend_type.get_config_cls().from_env()
            self._backend_val = self._backend_type.create(cfg)
            return self._backend_val

    def _get_result(
        self,
        result_key: ResultKey,
        partial_func: Callable[[], _R],
        ttl: timedelta,
    ) -> _R:
        backend = self._get_backend()
        raw = backend.get_result(result_key)
        if raw:
            return cast(_R, pickle.loads(raw))
        goal_res = backend.get_result_or_aquire_lock(result_key)
        if not isinstance(goal_res, UnlockKey):
            return cast(_R, pickle.loads(goal_res))
        try:
            start_time = time.time()
            result = partial_func()
            duration = time.time() - start_time
            if duration > 3:
                # TODO make this warning optional
                _logger.warning(
                    f"cache miss for {result_key.func_key} took {duration:.2f} seconds",
                )
            raw = pickle.dumps(result)
            backend.set_result(
                result_key=result_key,
                contents=raw,
                expires_at=datetime.now(UTC) + ttl,
            )
            return result
        finally:
            backend.release_lock(goal_res)

    def wrap(
        self,
        func_key: Any,
        ttl: timedelta | None = None,
        include_self: bool | None = None,
    ) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]:
        ttl = ttl or self._default_ttl
        if include_self is None:
            include_self = self._include_self

        group_key = self._get_group_key(func_key)
        get_result = self._get_result

        def deco(func: Callable[_P, _R]) -> Callable[_P, _R]:
            if group_key in self._arg_key_gens:
                raise RuntimeError("cache already exists with this func key", func_key)
            arg_key_gen = ArgKeyGen.from_func(func)
            self._arg_key_gens[group_key] = arg_key_gen

            trim_first_arg = arg_key_gen.is_method and not include_self

            @wraps(func)
            def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> Any:
                args_t = cast(tuple[Any, ...], args)
                arg_key = arg_key_gen.get_key(
                    args=(args_t[1:] if trim_first_arg else args_t),
                    kwargs=kwargs,
                )
                result_key = ResultKey(
                    domain_key=group_key.domain_key,
                    func_key=group_key.func_key,
                    arg_key=arg_key,
                )
                partial_func = partial(func, *args, **kwargs)
                return get_result(result_key, partial_func, ttl)

            return wrapped

        return deco

    def _get_group_key(self, func_key: Any) -> GroupKey:
        return GroupKey(
            domain_key=self._domain_key,
            func_key=FuncKey(repr(func_key)),
        )

    def _get_all_known_group_keys(self) -> set[GroupKey]:
        return set(self._arg_key_gens.keys())

    def get_result_key(self, func_key: Any, *args: Any, **kwargs: Any) -> ResultKey:
        group_key = self._get_group_key(func_key)
        arg_key_gen = self._arg_key_gens.get(group_key)
        if arg_key_gen is None:
            raise RuntimeError("no cache exists with this func key", func_key)
        return ResultKey(
            domain_key=group_key.domain_key,
            func_key=group_key.func_key,
            arg_key=arg_key_gen.get_key(args, kwargs),
        )

    def invalidate_all(self) -> None:
        backend = self._get_backend()
        backend.invalidate_many(
            group_keys=self._get_all_known_group_keys(),
            result_keys=set(),
        )

    def invalidate_many(
        self,
        func_keys: Iterable[Any] | None = None,
        result_keys: Iterable[ResultKey] | None = None,
    ) -> None:
        unq_group_keys: set[GroupKey] = set()
        if func_keys:
            for func_key in func_keys:
                group_key = self._get_group_key(func_key)
                if group_key not in self._arg_key_gens:
                    raise RuntimeError("no cache exists with this func key", func_key)
                unq_group_keys.add(group_key)
        unq_result_keys = set(result_keys or [])
        if len(unq_group_keys) + len(unq_result_keys) == 0:
            return
        backend = self._get_backend()
        backend.invalidate_many(
            group_keys=unq_group_keys,
            result_keys=unq_result_keys,
        )

    def invalidate_func(self, func_key: Any) -> None:
        self.invalidate_many(func_keys=(func_key,))

    def invalidate_result(self, func_key: Any, *args: Any, **kwargs: Any) -> None:
        result_key = self.get_result_key(func_key, *args, **kwargs)
        self.invalidate_many(result_keys=(result_key,))

    def flush_expired(self) -> None:
        backend = self._get_backend()
        backend.flush_expired()

    def lock(self, key: str) -> CacheLock:
        backend = self._get_backend()
        return CacheLock(lock_key=key, backend=backend)


def create_cache(
    domain_key: str,
    backend: Type[Backend[_C]],
    default_ttl: float | timedelta,
    backend_config: _C | None = None,
    include_self: bool = False,
) -> "Cache[_C]":
    if not isinstance(default_ttl, timedelta):
        default_ttl = timedelta(seconds=default_ttl)
    return Cache(
        _domain_key=DomainKey(domain_key),
        _backend_type=backend,
        _backend_val=None,
        _backend_config=backend_config,
        _backend_lock=threading.Lock(),
        _default_ttl=default_ttl,
        _arg_key_gens={},
        _include_self=include_self,
    )
