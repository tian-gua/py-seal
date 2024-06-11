class TableInfo:
    def __init__(self):
        self.columns: list[tuple[str, str]] = []
        self.model_fields: list[tuple[str, any]] = []
