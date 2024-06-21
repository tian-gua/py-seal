class TableInfo:
    def __init__(self):
        self.columns: list[tuple[str, str]] = []
        self.model_fields: list[tuple[str, any]] = []

    def __str__(self):
        return f'columns: {self.columns}, model_fields: {self.model_fields}'
