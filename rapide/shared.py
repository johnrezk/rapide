from abc import abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Generic, NewType, Protocol, Self, Type, TypeVar

DomainKey = NewType("DomainKey", str)
FuncKey = NewType("FuncKey", str)
ArgKey = NewType("ArgKey", str)


@dataclass(slots=True, frozen=True)
class GroupKey:
    domain_key: DomainKey
    func_key: FuncKey


@dataclass(slots=True, frozen=True)
class ResultKey:
    domain_key: DomainKey
    func_key: FuncKey
    arg_key: ArgKey


@dataclass(slots=True, frozen=True)
class UnlockKey:
    lock_key_hash: str
    token: str


class BackendConfig(Protocol):
    @classmethod
    @abstractmethod
    def from_env(cls) -> Self:
        pass


_C = TypeVar("_C", bound=BackendConfig)


class Backend(Protocol, Generic[_C]):
    @classmethod
    @abstractmethod
    def get_config_cls(cls) -> Type[_C]:
        pass

    @classmethod
    @abstractmethod
    def create(cls, config: _C) -> "Backend[_C]":
        pass

    @abstractmethod
    def aquire_lock(self, lock_key: str) -> UnlockKey:
        pass

    @abstractmethod
    def release_lock(self, unlock_key: UnlockKey) -> None:
        pass

    @abstractmethod
    def get_result(self, result_key: ResultKey) -> bytes | None:
        pass

    @abstractmethod
    def get_result_or_aquire_lock(self, result_key: ResultKey) -> bytes | UnlockKey:
        pass

    @abstractmethod
    def set_result(
        self,
        result_key: ResultKey,
        contents: bytes,
        expires_at: datetime,
    ) -> None:
        pass

    @abstractmethod
    def invalidate_many(
        self,
        group_keys: set[GroupKey],
        result_keys: set[ResultKey],
    ) -> None:
        pass

    @abstractmethod
    def flush_expired(self) -> None:
        pass
