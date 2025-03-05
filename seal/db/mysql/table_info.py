from dataclasses import make_dataclass, field


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

        fields = [(table_field.field_, any, field(default=None)) for table_field in self.table_fields]
        self.model: object = make_dataclass(self.table, fields=fields)
