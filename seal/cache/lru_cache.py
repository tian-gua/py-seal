from collections import OrderedDict
from typing import Any
from ..exception import UnsupportedException


class LRUCache:
    def __init__(self, capacity: int):
        self._cache = OrderedDict()
        self._capacity = capacity

    def get(self, key: str) -> Any:
        if key not in self._cache:
            return None
        else:
            self._cache.move_to_end(key)  # Mark as recently used
            return self._cache[key]

    def set(self, key: str, value: Any) -> None:
        if key in self._cache:
            self._cache.move_to_end(key)  # Mark as recently used
        self._cache[key] = value
        if len(self._cache) > self._capacity:
            self._cache.popitem(last=False)  # Remove least recently used item

    def remove(self, key: str) -> None:
        if key in self._cache:
            del self._cache[key]

    def remove_prefix(self, prefix: str) -> None:
        if prefix == '':
            raise UnsupportedException('Prefix cannot be empty')

        for key in list(self._cache.keys()):
            if key.startswith(prefix):
                del self._cache[key]
