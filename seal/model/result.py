from typing import Any, Dict, List


class Result:
    def __init__(self, row: Dict[str, Any] = None, bean_type=None):
        self.row = row
        self.bean_type = bean_type

    @staticmethod
    def empty() -> 'Result':
        return Result()

    def is_present(self) -> bool:
        return self.row is not None

    def get(self) -> Any:
        if self.row is not None:
            if self.bean_type is None:
                raise Exception('no type specified')
            return self.bean_type(**self.row)
        return None

    def as_dict(self) -> Dict[str, Any] | None:
        if self.row is not None:
            return self.row
        return None


class Results:
    def __init__(self, rows: List[Dict[str, Any]] = None, bean_type=None):
        self.rows = rows
        self.bean_type = bean_type

    @staticmethod
    def empty() -> 'Results':
        return Results()

    def is_present(self) -> bool:
        return self.rows is not None and len(self.rows) > 0

    def get(self) -> List[Any]:
        if self.rows is not None:
            if self.bean_type is None:
                raise Exception('no type specified')
            return [self.bean_type(**row) for row in self.rows]
        return []

    def as_dict(self) -> List[Dict]:
        if self.rows is not None:
            return self.rows
        return []
