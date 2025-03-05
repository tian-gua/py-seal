from typing import Protocol, Tuple, List

from ..result import Result, Results


class IExecutor(Protocol):

    def find(self, sql: str, args: Tuple[any, ...], model: object) -> Result | None:
        ...

    def find_list(self, sql: str, args: Tuple[any, ...], model: object) -> Results | None:
        ...

    def count(self, sql: str, args: Tuple[any, ...]) -> int | None:
        ...

    def update(self, sql: str, args: Tuple[any, ...]) -> int | None:
        ...

    def insert(self, sql: str, args: Tuple[any, ...]) -> int | None:
        ...

    def insert_bulk(self, sql: str, args: List[Tuple[any, ...]]) -> int | None:
        ...

    def custom_query(self, sql: str, args=Tuple[any, ...]) -> Results | None:
        ...

    def custom_update(self, sql: str, args=Tuple[any, ...]) -> int | None:
        ...
