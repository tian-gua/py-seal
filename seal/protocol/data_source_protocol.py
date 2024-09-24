from typing import Protocol, Any

from .executor_protocol import IExecutor


class IDataSource(Protocol):
    def get_name(self) -> str:
        ...

    def get_type(self) -> str:
        ...

    def get_executor(self) -> IExecutor:
        ...

    def load_structure(self, database: str, table: str) -> Any:
        ...

    def get_connection(self):
        ...

    def get_default_database(self) -> str:
        ...
