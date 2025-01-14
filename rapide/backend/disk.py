import math
import sqlite3
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from queue import Queue
from time import sleep
from types import TracebackType
from typing import Self, Type, TypeVar

import xxhash

from rapide.env import EnvUtil
from rapide.shared import GroupKey, ResultKey, UnlockKey


def get_time_ms() -> int:
    return math.floor(datetime.now(UTC).timestamp() * 1000)


def conn_sqlite3(db_file: Path) -> sqlite3.Connection:
    return sqlite3.connect(
        db_file,
        isolation_level=None,
        cached_statements=30,
        check_same_thread=False,
        timeout=20,
    )


@dataclass(slots=True)
class ConnPool:
    _db_file: Path
    _pool_size: int
    _is_setup: bool
    _setup_lock: threading.Lock
    _q: Queue[sqlite3.Connection]

    @classmethod
    def create(cls, db_file: Path, pool_size: int) -> "ConnPool":
        if pool_size < 1:
            raise ValueError("pool size must be greater than or equal to 1")
        return ConnPool(
            db_file,
            pool_size,
            False,
            threading.Lock(),
            Queue(),
        )

    def _setup(self) -> None:
        with self._setup_lock:
            if self._is_setup:
                return
            self._db_file.parent.mkdir(exist_ok=True, parents=True)
            conn = conn_sqlite3(self._db_file)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS lock ("
                "   lock_key TEXT PRIMARY KEY ASC,"
                "   expires_at INTEGER NOT NULL"
                ")"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS result ("
                "    domain_key TEXT NOT NULL,"
                "    func_key TEXT NOT NULL,"
                "    arg_key TEXT NOT NULL,"
                "    contents BLOB NOT NULL,"
                "    expires_at INTEGER NOT NULL,"
                "    PRIMARY KEY (domain_key, func_key, arg_key)"
                ")"
            )
            conn.close()
            for _ in range(self._pool_size):
                self._q.put(conn_sqlite3(self._db_file))
            self._is_setup = True

    def aquire_conn(self) -> sqlite3.Connection:
        if self._is_setup:
            return self._q.get()
        self._setup()
        return self._q.get()

    def release_conn(self, conn: sqlite3.Connection) -> None:
        self._q.put(conn)


T = TypeVar("T")


@dataclass(slots=True)
class Database:
    _pool: ConnPool
    _c: sqlite3.Connection | None = None

    def __enter__(self) -> Self:
        self._c = self._pool.aquire_conn()
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_inst: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        assert self._c
        self._pool.release_conn(self._c)
        self._c = None

    @property
    def _conn(self) -> sqlite3.Connection:
        if self._c is None:
            raise RuntimeError("Cannot access sqlite conn outside of with ctx")
        return self._c

    def get_result(self, rk: ResultKey) -> bytes | None:
        cursor = self._conn.execute(
            (
                "SELECT contents FROM result "
                "WHERE domain_key = ? AND func_key = ? "
                "AND arg_key = ? AND expires_at > ?"
            ),
            (rk.domain_key, rk.func_key, rk.arg_key, get_time_ms()),
        )
        row: tuple[bytes] | None = cursor.fetchone()
        if row is None:
            return None
        return row[0]


@dataclass
class Transaction(Database):
    __slots__ = ()  # resolves subtle issue w/ super() and auto-gen slots

    def set_result(
        self,
        rk: ResultKey,
        contents: bytes,
        expires_at: int,
    ) -> None:
        self._conn.execute(
            (
                "INSERT INTO result (domain_key, func_key, arg_key, contents, expires_at) "
                "VALUES (:domain_key, :func_key, :arg_key, :contents, :expires_at) "
                "ON CONFLICT (domain_key, func_key, arg_key) DO UPDATE "
                "SET contents = excluded.contents, expires_at = excluded.expires_at"
            ),
            {
                "domain_key": rk.domain_key,
                "func_key": rk.func_key,
                "arg_key": rk.arg_key,
                "contents": contents,
                "expires_at": expires_at,
            },
        )

    def invalidate_all(self) -> None:
        self._conn.execute("DELETE FROM result")

    def invalidate_funcs(self, group_keys: set[GroupKey]) -> None:
        if len(group_keys) == 0:
            return
        self._conn.executemany(
            "DELETE FROM result WHERE domain_key = ? AND func_key = ?",
            [(gk.domain_key, gk.func_key) for gk in group_keys],
        )

    def invalidate_results(self, result_keys: set[ResultKey]) -> None:
        if len(result_keys) == 0:
            return
        self._conn.executemany(
            "DELETE FROM result "
            "WHERE domain_key = ? AND func_key = ? AND arg_key = ?",
            [(rk.domain_key, rk.func_key, rk.arg_key) for rk in result_keys],
        )

    def flush_expired(self) -> None:
        now = get_time_ms()
        self._conn.execute("DELETE FROM result WHERE expires_at < ?", (now,))

    def try_lock(self, lock_key_hash: str) -> UnlockKey | None:
        now = get_time_ms()
        expires_at = now + 60 * 1000
        cursor = self._conn.execute(
            (
                "INSERT INTO lock (lock_key, expires_at) "
                "VALUES (:lock_key, :expires_at) "
                "ON CONFLICT (lock_key) DO UPDATE "
                "SET expires_at = excluded.expires_at "
                "WHERE expires_at < :now"
            ),
            {
                "lock_key": lock_key_hash,
                "expires_at": expires_at,
                "now": now,
            },
        )
        if cursor.rowcount == 1:
            return UnlockKey(
                lock_key_hash=lock_key_hash,
                token=str(expires_at),
            )
        return None

    def release_lock(self, unlock_key: UnlockKey) -> None:
        expires_at = int(unlock_key.token)
        self._conn.execute(
            "DELETE FROM lock WHERE lock_key = ? AND expires_at = ?",
            (unlock_key.lock_key_hash, expires_at),
        )

    def __enter__(self) -> Self:
        super().__enter__()
        while True:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                break
            except sqlite3.OperationalError:
                continue
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_inst: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        try:
            if exc_type is None:
                self._conn.execute("COMMIT")
            else:
                self._conn.execute("ROLLBACK")
        finally:
            super().__exit__(exc_type, exc_inst, exc_tb)


_LOCK_SLEEP_SEC = 0.003


@dataclass(slots=True)
class DiskBackendConfig:
    db_file: Path | str | None = None
    pool_size: int | None = None

    @classmethod
    def from_env(cls) -> "DiskBackendConfig":
        env = EnvUtil("DISK")
        return DiskBackendConfig(
            db_file=env.get_str("DB_FILE"),
            pool_size=env.get_int("POOL_SIZE"),
        )


@dataclass(slots=True)
class DiskBackend:
    pool: ConnPool

    @classmethod
    def get_config_cls(cls) -> Type[DiskBackendConfig]:
        return DiskBackendConfig

    @classmethod
    def create(cls, config: DiskBackendConfig) -> "DiskBackend":
        # TODO check for valid config

        db_file: Path | None = None
        if isinstance(config.db_file, str):
            db_file = Path(config.db_file)
        if db_file is None:
            db_file = Path.home() / ".rapide-cache" / "db"

        pool_size = config.pool_size or 3

        pool = ConnPool.create(db_file, pool_size)
        return cls(pool)

    def _open_db(self) -> Database:
        return Database(self.pool)

    def _start_tx(self) -> Transaction:
        return Transaction(self.pool)

    def _hash_lock_key(self, lock_key: str) -> str:
        return xxhash.xxh3_128_hexdigest(lock_key, seed=42)

    def aquire_lock(self, lock_key: str) -> UnlockKey:
        lock_key_hash = self._hash_lock_key(lock_key)
        while True:
            with self._start_tx() as tx:
                unlock_key = tx.try_lock(lock_key_hash)
                if unlock_key:
                    return unlock_key
            sleep(_LOCK_SLEEP_SEC)

    def release_lock(self, unlock_key: UnlockKey) -> None:
        with self._start_tx() as tx:
            tx.release_lock(unlock_key)

    def get_result(self, result_key: ResultKey) -> bytes | None:
        with self._open_db() as db:
            return db.get_result(result_key)

    def get_result_or_aquire_lock(self, result_key: ResultKey) -> bytes | UnlockKey:
        rk = result_key
        lock_key = f"get_result:{rk.domain_key}:{rk.func_key}:{rk.arg_key}"
        lock_key_hash = self._hash_lock_key(lock_key)
        while True:
            with self._start_tx() as tx:
                unlock_key = tx.try_lock(lock_key_hash)
                if unlock_key:
                    res = tx.get_result(result_key)
                    if res is None:
                        return unlock_key
                    tx.release_lock(unlock_key)
                    return res
            sleep(_LOCK_SLEEP_SEC)

    def set_result(
        self,
        result_key: ResultKey,
        contents: bytes,
        expires_at: datetime,
    ) -> None:
        expires_at_ms = math.floor(expires_at.timestamp() * 1000)
        with self._start_tx() as tx:
            tx.set_result(
                rk=result_key,
                contents=contents,
                expires_at=expires_at_ms,
            )

    def invalidate_all(self) -> None:
        with self._start_tx() as tx:
            tx.invalidate_all()

    def invalidate_many(
        self,
        group_keys: set[GroupKey],
        result_keys: set[ResultKey],
    ) -> None:
        with self._start_tx() as tx:
            tx.invalidate_funcs(group_keys)
            tx.invalidate_results(result_keys)

    def flush_expired(self) -> None:
        with self._start_tx() as tx:
            tx.flush_expired()
