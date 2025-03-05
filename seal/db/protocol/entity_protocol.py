from typing import Protocol, runtime_checkable


@runtime_checkable
class IEntity(Protocol):
    __table_name__: str = ''
