class Result:
    def __init__(self, row: dict[str, any] | None = None, model: any = None):
        self.row: dict[str, any] = row
        self.model: any = model

    @staticmethod
    def empty() -> 'Result':
        return Result()

    def get(self) -> any:
        if self.row is not None:
            if self.model is None:
                raise Exception('no type specified')
            return self.model(**self.row)
        return None

    def as_dict(self) -> dict[str, any] | None:
        if self.row is not None:
            return self.row
        return None


class Results:
    def __init__(self, rows: list[dict[str, any]] = None, model: any = None):
        self.rows = rows
        self.model = model

    @staticmethod
    def empty() -> 'Results':
        return Results()

    def is_present(self) -> bool:
        return self.rows is not None and len(self.rows) > 0

    def get(self) -> list[object]:
        if self.rows is not None:
            if self.model is None:
                raise Exception('no type specified')
            return [self.model(**row) for row in self.rows]
        return []

    def as_dict(self) -> list[dict[str, any]]:
        if self.rows is not None:
            return self.rows
        return []
