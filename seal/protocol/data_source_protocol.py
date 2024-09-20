from typing import Protocol, Any

from .executor_protocol import ExecutorProtocol


class DataSourceProtocol(Protocol):
    def get_name(self) -> str:
        ...

    def get_type(self) -> str:
        ...

    def get_executor(self) -> ExecutorProtocol:
        ...

    def load_structure(self, database: str, table: str) -> Any:
        ...

    def get_connection(self):
        ...

    def get_default_database(self) -> str:
        ...
