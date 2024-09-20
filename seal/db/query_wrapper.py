from typing import Any, List, Tuple

from .structures import structures
from .wrapper import Wrapper
from .sql_builder import build_select, build_count
from seal.model.result import Result, Results
from dataclasses import fields

from ..protocol.data_source_protocol import DataSourceProtocol


class QueryWrapper(Wrapper):
    def __init__(self,
                 table: str,
                 database=None,
                 data_source: DataSourceProtocol = None,
                 tenant_field=None,
                 tenant_value=None,
                 logic_delete_field=None,
                 logic_delete_true=None,
                 logic_delete_false=None):
        super().__init__(tenant_field=tenant_field,
                         tenant_value=tenant_value,
                         logic_delete_field=logic_delete_field,
                         logic_delete_true=logic_delete_true,
                         logic_delete_false=logic_delete_false)
        self.table = table
        if database is not None:
            self.table = f'{database}.{table}'
        self.data_source = data_source

        self.result_type = structures.get(data_source=self.data_source.get_name(), database=database, table=table)
        if self.result_type is None:
            self.result_type = self.data_source.load_structure(database, table)
            structures.register(data_source=self.data_source.get_name(), database=database, table=table, structure=self.result_type)

        self.limit_ = None
        self.offset = None
        self.order_by = None
        self.field_list = []
        self.ignore_fields = []

    def select(self, *field_list) -> 'QueryWrapper':
        self.field_list = field_list
        return self

    def ignore(self, *field_list) -> 'QueryWrapper':
        self.ignore_fields = field_list
        return self

    def sort(self, *order_by) -> 'QueryWrapper':
        self.order_by = order_by
        return self

    def limit(self, limit: int) -> 'QueryWrapper':
        self.limit_ = limit
        return self

    def offset(self, offset: int) -> 'QueryWrapper':
        self.offset = offset
        return self

    def one(self, as_dict=False, **options) -> Any:
        sql, args = self.build_statement(**options)
        result: Result = self.data_source.get_executor().find(sql, args, self.result_type)
        if as_dict:
            return result.as_dict()
        return result.get()

    def one_dict(self, **options) -> dict:
        return self.one(as_dict=True, **options)

    def list(self, as_dict=False, **options) -> List[Any]:
        sql, args = self.build_statement(**options)
        results: Results = self.data_source.get_executor().find_list(sql, args, self.result_type)
        if as_dict:
            return results.as_dict()
        return results.get()

    def list_dict(self, **options) -> List[dict]:
        return self.list(as_dict=True, **options)

    def page(self, page: int, page_size: int, as_dict=False, **options) -> Tuple[List[Any], int]:
        self.limit_ = page_size
        self.offset = (page - 1) * page_size
        sql, args = self.build_statement(**options)
        results: Results = self.data_source.get_executor().find_list(sql, args, self.result_type)
        count = self.count()
        if as_dict:
            return results.as_dict(), count
        return results.get(), count

    def page_dict(self, page: int, page_size: int, **options) -> Tuple[List[dict], int]:
        return self.page(page, page_size, as_dict=True, **options)

    def count(self):
        sql, args = build_count(self)
        return self.data_source.get_executor().count(sql, args)

    def build_statement(self, **options) -> Tuple[str, Tuple[Any, ...]]:
        self.handle_public_fields(**options)

        if len(self.field_list) == 0:
            self.field_list = [field.name for field in fields(self.result_type) if field.name not in self.ignore_fields]
        return build_select(self)

    def build_sql(self, **options) -> str:
        sql, args = self.build_statement(**options)
        for arg in args:
            if isinstance(arg, str):
                sql = sql.replace('?', f"'{arg}'", 1)
            else:
                sql = sql.replace('?', str(arg), 1)
        return sql
