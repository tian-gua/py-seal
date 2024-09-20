from typing import Protocol, Tuple, Any, List

from ..model.result import Result, Results


class ExecutorProtocol(Protocol):

    def find(self, sql: str, args: Tuple[Any, ...], bean_type: Any) -> Result:
        ...

    def find_list(self, sql: str, args: Tuple[Any, ...], bean_type: Any) -> Results:
        ...

    def count(self, sql: str, args: Tuple[Any, ...]) -> int | None:
        ...

    def update(self, sql: str, args: Tuple[Any, ...]) -> int | None:
        ...

    def insert(self, sql: str, args: Tuple[Any, ...]) -> int | None:
        ...

    def insert_bulk(self, sql: str, args: List[Tuple[Any, ...]]) -> int | None:
        ...

    def custom_query(self, sql: str, args=Tuple[Any, ...]) -> Results:
        ...

    def custom_update(self, sql: str, args=Tuple[Any, ...]) -> int | None:
        ...
