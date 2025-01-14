from dataclasses import dataclass
from os import environ


@dataclass
class EnvUtil:
    prefix: str

    def _get_key(self, subkey: str) -> str:
        return f"RAPIDE_{self.prefix}_{subkey.upper()}"

    def get_str(self, subkey: str) -> str | None:
        key = self._get_key(subkey)
        return environ.get(key)

    def get_int(self, subkey: str) -> int | None:
        key = self._get_key(subkey)
        val = environ.get(key, "").strip()
        if len(val) == 0:
            return None
        return int(val)

    def get_bool(self, subkey: str) -> bool | None:
        key = self._get_key(subkey)
        val = environ.get(key, "").strip().upper()
        if val == "TRUE":
            return True
        if val == "FALSE":
            return False
        return None
