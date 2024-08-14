from typing import Any, Dict, List


class Result:
    def __init__(self, row: dict = None, bean_type=None):
        self.row = row
        self.bean_type = bean_type

    @staticmethod
    def empty() -> 'Result':
        return Result()

    def is_empty(self) -> bool:
        return self.row is None

    def bean(self) -> Any:
        if self.bean_type is None:
            raise Exception('no type specified')
        if self.row is not None:
            return self.bean_type(**self.row)
        return None

    def dict(self) -> Dict | None:
        if self.row is not None:
            return self.row
        return None


class Results:
    def __init__(self, rows: list[dict] = None, bean_type=None):
        self.rows = rows
        self.bean_type = bean_type

    @staticmethod
    def empty() -> 'Results':
        return Results()

    def is_empty(self) -> bool:
        return self.rows is None or len(self.rows) == 0

    def bean(self) -> Any | List[Any]:
        if self.bean_type is None:
            raise Exception('no type specified')
        if self.rows is not None:
            return [self.bean_type(**row) for row in self.rows]
        return []

    def dict(self) -> Dict | list[Dict]:
        if self.rows is not None:
            return self.rows
        return []
