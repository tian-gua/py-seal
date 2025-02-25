from typing import Protocol


class IDatabaseConnection(Protocol):
    def commit(self):
        ...

    def rollback(self):
        ...

    def begin(self):
        ...

    def cursor(self):
        ...

    def close(self):
        ...

    def ping(self, seconds):
        ...
