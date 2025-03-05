from dataclasses import make_dataclass, field


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

        fields = [(table_field.name, any, field(default=None)) for table_field in self.table_fields]
        self.model: object = make_dataclass(self.table, fields)
