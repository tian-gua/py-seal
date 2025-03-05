from dataclasses import fields
from typing import List

from seal.db.protocol import IDataSource
from seal.db.result import Result, Results
from .protocol import IEntity
from .sql_builder import build_select, build_count
from .structures import structures
from .wrapper import Wrapper
from ..enum import ResultType
from ..types import Column


class QueryWrapper(Wrapper):
    def __init__(self,
                 table: str | IEntity,
                 database: str = None,
                 data_source: IDataSource = None,
                 tenant_field: Column | None = None,
                 tenant_value: any = None,
                 logical_deleted_field: Column | None = None,
                 logical_deleted_value_true: any = None,
                 logical_deleted_value_false: any = None):

        super().__init__(table=table,
                         tenant_field=tenant_field,
                         tenant_value=tenant_value,
                         logical_deleted_field=logical_deleted_field,
                         logical_deleted_value_true=logical_deleted_value_true,
                         logical_deleted_value_false=logical_deleted_value_false)

        if self.table is None or self.table == '':
            raise ValueError('table is required')

        if database is not None and database != '':
            self.table = f'{database}.{table}'

        self.data_source: IDataSource = data_source

        self.model = structures.get(data_source=self.data_source.get_name(),
                                    database=database or data_source.get_default_database(),
                                    table=table)
        if self.model is None:
            self.model = self.data_source.load_structure(database, table)
            structures.register(data_source=self.data_source.get_name(),
                                database=database or data_source.get_default_database(),
                                table=table,
                                structure=self.model)

        self.limit_ = None
        self.offset = None
        self.order_by = None
        self.field_list = []
        self.ignore_fields = []

    def select(self, *field_list) -> 'QueryWrapper':
        if self.entity is not None:
            for field in field_list:
                if not hasattr(self.entity, field):
                    raise ValueError(f'table {self.table} has no field {field}')

        self.field_list = field_list
        return self

    def ignore(self, *field_list) -> 'QueryWrapper':
        if self.entity is not None:
            for field in field_list:
                if not hasattr(self.entity, field):
                    raise ValueError(f'table {self.table} has no field {field}')

        self.ignore_fields = field_list
        return self

    def sort(self, *order_by) -> 'QueryWrapper':
        self.order_by = order_by
        return self

    def desc(self, *order_by) -> 'QueryWrapper':
        self.order_by = [f'{field} desc' for field in order_by]
        return self

    def asc(self, *order_by) -> 'QueryWrapper':
        self.order_by = [f'{field} asc' for field in order_by]
        return self

    def limit(self, limit: int) -> 'QueryWrapper':
        self.limit_ = limit
        return self

    def offset(self, offset: int) -> 'QueryWrapper':
        self.offset = offset
        return self

    def one(self, result_type: ResultType = ResultType.DICT, **options) -> dict | object:
        sql, args = self.build_statement(**options)
        result: Result | None = self.data_source.get_executor().find(sql, args, self.model)
        if result_type == ResultType.DICT:
            return result.as_dict()
        return result.get()

    def list(self, result_type: ResultType = ResultType.DICT, **options) -> List[dict] | List[object]:
        sql, args = self.build_statement(**options)
        results: Results = self.data_source.get_executor().find_list(sql, args, self.model)
        if result_type == ResultType.DICT:
            return results.as_dict()
        return results.get()

    def page(self, page: int, page_size: int, result_type: ResultType = ResultType.DICT, **options) -> (
            tuple[List[dict[str, any]], int] | tuple[List[any], int]):
        self.limit_ = page_size
        self.offset = (page - 1) * page_size
        sql, args = self.build_statement(**options)
        results: Results = self.data_source.get_executor().find_list(sql, args, self.model)
        count = self.count()
        if result_type == ResultType.DICT:
            return results.as_dict(), count
        return results.get(), count

    def d_page(self, page: int, page_size: int, **options) -> tuple[List[dict], int]:
        return self.page(page, page_size, as_dict=True, **options)

    def count(self):
        sql, args = build_count(self)
        return self.data_source.get_executor().count(sql, args)

    def build_statement(self, **options) -> tuple[str, tuple[any, ...]]:
        self.handle_public_fields(**options)

        if len(self.field_list) == 0:
            self.field_list = [field.name for field in fields(self.model) if field.name not in self.ignore_fields]
        return build_select(self)

    def build_sql(self, **options) -> str:
        sql, args = self.build_statement(**options)
        for arg in args:
            if isinstance(arg, str):
                sql = sql.replace('?', f"'{arg}'", 1)
            else:
                sql = sql.replace('?', str(arg), 1)
        return sql
