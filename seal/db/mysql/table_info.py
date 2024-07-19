from dataclasses import make_dataclass, field
from typing import Any


class TableField:

    def __init__(self, field_, type_, null_, key_, default_, extra):
        self.field_ = field_
        self.type_ = type_
        self.null_ = null_
        self.key_ = key_
        self.default_ = default_
        self.extra = extra


class TableInfo:

    def __init__(self, table, table_fields: list[TableField]):
        self.table = table
        self.table_fields = table_fields
        self.model = None

    def parse_model(self):
        if self.model is None:
            self.model = make_dataclass(self.table,
                                        [(table_field.field_, Any, field(default=None)) for table_field in
                                         self.table_fields])
        return self.model
