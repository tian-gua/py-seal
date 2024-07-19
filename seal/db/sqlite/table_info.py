from dataclasses import make_dataclass, field
from typing import Any


class TableField:

    def __init__(self, cid, name, type_, notnull, dflt_value, pk):
        self.cid = cid
        self.name = name
        self.type_ = type_
        self.notnull = notnull
        self.dflt_value = dflt_value
        self.pk = pk


class TableInfo:

    def __init__(self, table, table_fields: list[TableField]):
        self.table = table
        self.table_fields = table_fields
        self.model = None

    def parse_model(self):
        if self.model is None:
            self.model = make_dataclass(self.table,
                                        [(table_field.name, Any, field(default=None)) for table_field in
                                         self.table_fields])
        return self.model
