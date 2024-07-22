from .base_wrapper import BaseWrapper
from .data_source import DataSource
from .sql_builder import build_select, build_count
from dataclasses import fields


class QueryWrapper(BaseWrapper):
    def __init__(self, table: str, data_source: DataSource, logical_delete=None):
        super().__init__(table, data_source, logical_delete=logical_delete)
        self.result_type = data_source.get_data_structure(table)
        self.limit = None
        self.offset = None
        self.order_by = None
        self.field_list = []
        self.ignore_fields = []

    def select(self, *field_list):
        self.field_list = field_list
        return self

    def ignore(self, *field_list):
        self.ignore_fields = field_list
        return self

    def sort(self, *order_by):
        self.order_by = order_by
        return self

    def limit(self, limit: int):
        self.limit = limit
        return self

    def offset(self, offset: int):
        self.offset = offset
        return self

    def find_one(self, **options):
        self._handle_logical_delete(**options)

        sql, args = self._build_select()
        return self.data_source.get_executor().find_one(sql, args, self.field_list, self.result_type, **options)

    def find_list(self, **options):
        self._handle_logical_delete(**options)

        sql, args = self._build_select()
        return self.data_source.get_executor().find_list(sql, args, self.field_list, self.result_type)

    def find_page(self, page: int, page_size: int, **options):
        self._handle_logical_delete(**options)

        self.limit = page_size
        self.offset = (page - 1) * page_size
        sql, args = self._build_select()
        records = self.data_source.get_executor().find_list(sql, args, self.field_list, self.result_type)
        count = self.count()
        return records, count

    def count(self):
        sql, args = build_count(self)
        return self.data_source.get_executor().count(sql, args)

    def _build_select(self):
        if len(self.field_list) == 0:
            self.field_list = [field.name for field in fields(self.result_type) if field.name not in self.ignore_fields]
        return build_select(self)
