from typing import Protocol, Any

from .database_connection_protocol import IDatabaseConnection
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

    def get_connection(self) -> IDatabaseConnection:
        ...

    def get_default_database(self) -> str:
        ...

    def ping(self, seconds):
        ...
