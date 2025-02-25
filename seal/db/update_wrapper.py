import datetime
from dataclasses import fields

from . import structures
from .protocol import IDataSource
from .sql_builder import build_update, build_delete
from .wrapper import Wrapper
from ..types import Column


class UpdateWrapper(Wrapper):

    def __init__(self,
                 table: str,
                 database: None | str = None,
                 data_source: IDataSource = None,
                 tenant_field: Column | None = None,
                 tenant_value: any = None,
                 updated_by_field: Column | None = None,
                 updated_at_field: any = None,
                 logical_deleted_field: Column | None = None,
                 logical_deleted_value_true: any = None,
                 logical_deleted_value_false: any = None):
        super().__init__(tenant_field=tenant_field,
                         tenant_value=tenant_value,
                         logical_deleted_field=logical_deleted_field,
                         logical_deleted_value_true=logical_deleted_value_true,
                         logical_deleted_value_false=logical_deleted_value_false)
        self.database: None | str = database
        self.table: str = table
        if database is not None and database != '':
            self.table = f'{database}.{table}'
        self.data_source: IDataSource | None = data_source
        self.updated_by_field: any = updated_by_field
        self.updated_at_field: any = updated_at_field
        self.update_fields: dict = {}

        self.model = structures.get(data_source=self.data_source.get_name(),
                                    database=database or data_source.get_default_database(),
                                    table=table)
        if self.model is None:
            self.model = self.data_source.load_structure(database, table)
            structures.register(data_source=self.data_source.get_name(),
                                database=database or data_source.get_default_database(),
                                table=table,
                                structure=self.model)

    def set(self, field: Column, value: any) -> 'UpdateWrapper':
        self.update_fields[field] = value
        return self

    def set_all(self, entity: any) -> 'UpdateWrapper':
        if isinstance(entity, dict):
            for field in fields(self.model):
                if field.name != 'id' and field.name in entity:
                    self.update_fields[field.name] = entity[field.name]
            return self

        for field in fields(entity):
            if field.name != 'id':
                continue
            self.update_fields[field.name] = getattr(entity, field.name)
        return self

    def update(self, **options):
        if len(self.condition_tree.conditions) == 0:
            raise ValueError('unsupported update all')

        self.handle_public_fields(**options)
        sql, args = build_update(self)
        return self.data_source.get_executor().update(sql, args)

    def delete(self, **options):
        if len(self.condition_tree.conditions) == 0:
            raise ValueError('unsupported delete all')

        self.handle_public_fields(**options)

        if options.get('logical_delete', False):
            self.set(options['logical_delete'], 1)
            return self.update(**options)
        else:
            sql, args = build_delete(self)
            return self.data_source.get_executor().update(sql, args)

    def handle_update_fields(self, **options) -> 'UpdateWrapper':
        if self.updated_at_field is not None:
            self.set(self.updated_at_field, datetime.datetime.now())
        if self.updated_by_field is not None:
            self.set(self.updated_by_field, options.get('updated_by', 'system'))
        return self
