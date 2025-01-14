import math
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Type, cast
from uuid import uuid1

import yarl
from redis import ConnectionPool, Redis
from redis.client import Pipeline
from redis.exceptions import LockNotOwnedError
from redis.lock import Lock

from rapide.env import EnvUtil
from rapide.shared import GroupKey, ResultKey, UnlockKey

# UTILS


def _rk_to_rkstr(rk: ResultKey) -> str:
    return f"result:{rk.domain_key}:{rk.func_key}:{rk.arg_key}"


def _rk_to_fkstr(rk: ResultKey) -> str:
    return f"func:{rk.domain_key}:{rk.func_key}"


def _rk_to_dmstr(rk: ResultKey) -> str:
    return f"domain:{rk.domain_key}"


# MAIN CLASSES


@dataclass(slots=True)
class RedisBackendConfig:
    dsn: str | None = None
    host: str | None = None
    port: int | None = None
    username: str | None = None
    password: str | None = None
    db_num: int | None = None
    secure: bool | None = None

    @classmethod
    def from_env(cls) -> "RedisBackendConfig":
        env = EnvUtil("REDIS")
        return RedisBackendConfig(
            dsn=env.get_str("DSN"),
            host=env.get_str("HOST"),
            port=env.get_int("PORT"),
            username=env.get_str("USERNAME"),
            password=env.get_str("PASSWORD"),
            db_num=env.get_int("DB_NUM"),
            secure=env.get_bool("SECURE"),
        )


@dataclass(slots=True)
class RedisBackend:
    _dsn: str
    _pool: ConnectionPool | None
    _pool_lock: threading.Lock

    @classmethod
    def get_config_cls(cls) -> Type[RedisBackendConfig]:
        return RedisBackendConfig

    @classmethod
    def create(cls, config: RedisBackendConfig) -> "RedisBackend":
        dsn: str | None = config.dsn
        if dsn is None:
            dsn_url = yarl.URL.build(
                scheme="rediss" if config.secure is True else "redis",
                host=config.host or "localhost",
                port=config.port or 6379,
                user=config.username,
                password=config.password,
                path=f"/{config.db_num or 0}",
            )
            dsn = str(dsn_url)
        return RedisBackend(
            _dsn=dsn,
            _pool=None,
            _pool_lock=threading.Lock(),
        )

    def _get_pool(self) -> ConnectionPool:
        with self._pool_lock:
            if not self._pool:
                self._pool = ConnectionPool.from_url(self._dsn)
            return self._pool

    def _get_client(self) -> Redis:
        pool = self._get_pool()
        return Redis(connection_pool=pool)

    def _invalidate_groups(self, pipe: Pipeline, group_keys: set[GroupKey]) -> None:
        if len(group_keys) == 0:
            return

        func_set_keys: set[str] = set()
        for gk in group_keys:
            func_set_key = f"func:{gk.domain_key}:{gk.func_key}"
            func_set_keys.add(func_set_key)
            pipe.smembers(func_set_key)

        res_raw = pipe.execute()
        subsets = cast(list[set[str]], res_raw)

        keys_to_delete: set[str] = set()
        for subset in subsets:
            keys_to_delete.update(subset)

        for result_key in keys_to_delete:
            pipe.delete(result_key)
        for func_set_key in func_set_keys:
            pipe.delete(func_set_key)
        pipe.execute()

    def _invalidate_results(self, pipe: Pipeline, result_keys: set[ResultKey]):
        for rk in result_keys:
            result_str = _rk_to_rkstr(rk)
            func_str = _rk_to_fkstr(rk)
            domain_str = _rk_to_dmstr(rk)
            pipe.delete(result_str)
            pipe.srem(domain_str, result_str)
            pipe.srem(func_str, result_str)
        pipe.execute()

    def aquire_lock(self, lock_key: str) -> UnlockKey:
        token = str(uuid1())
        client = self._get_client()
        redis_lock: Lock = client.lock(lock_key, thread_local=False)
        redis_lock.acquire(token=token)
        return UnlockKey(
            lock_key_hash=lock_key,
            token=token,
        )

    def release_lock(self, unlock_key: UnlockKey) -> None:
        client = self._get_client()
        redis_lock: Lock = client.lock(unlock_key.lock_key_hash, thread_local=False)
        try:
            redis_lock.do_release(unlock_key.token)
        except LockNotOwnedError:
            pass

    def get_result(self, result_key: ResultKey) -> bytes | None:
        rk_str = _rk_to_rkstr(result_key)
        client = self._get_client()
        with client.pipeline() as pipe:
            pipe.get(rk_str)
            result = pipe.execute()
            value = result[0]
            return value if value is not None else None

    def get_result_or_aquire_lock(self, result_key: ResultKey) -> bytes | UnlockKey:
        lock_key = f"lock:{_rk_to_rkstr(result_key)}"
        unlock_key = self.aquire_lock(lock_key)
        res = self.get_result(result_key)
        if res is None:
            return unlock_key
        self.release_lock(unlock_key)
        return res

    def set_result(
        self,
        result_key: ResultKey,
        contents: bytes,
        expires_at: datetime,
    ) -> None:
        key = _rk_to_rkstr(result_key)
        domain_set_key = _rk_to_dmstr(result_key)
        func_set_key = _rk_to_fkstr(result_key)

        expire_ts = math.floor(expires_at.astimezone(UTC).timestamp())

        client = self._get_client()
        with client.pipeline() as pipe:
            pipe.set(key, contents, exat=expire_ts)
            pipe.sadd(domain_set_key, key)
            pipe.sadd(func_set_key, key)
            pipe.execute()

    def invalidate_many(
        self,
        group_keys: set[GroupKey],
        result_keys: set[ResultKey],
    ) -> None:
        client = self._get_client()
        with client.pipeline() as pipe:
            self._invalidate_groups(pipe, group_keys)
            self._invalidate_results(pipe, result_keys)

    def flush_expired(self) -> None:
        # Redis does this automatically
        return
