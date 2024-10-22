import datetime

from .wrapper import Wrapper
from .sql_builder import build_update, build_delete
from dataclasses import fields


class UpdateWrapper(Wrapper):

    def __init__(self,
                 table,
                 database=None,
                 data_source=None,
                 tenant_field=None,
                 tenant_value=None,
                 updated_by_field=None,
                 updated_at_field=None,
                 logical_deleted_field=None,
                 logical_deleted_value_true=None,
                 logical_deleted_value_false=None):
        super().__init__(tenant_field=tenant_field,
                         tenant_value=tenant_value,
                         logical_deleted_field=logical_deleted_field,
                         logical_deleted_value_true=logical_deleted_value_true,
                         logical_deleted_value_false=logical_deleted_value_false)
        self.table = table
        if database is not None and database != '':
            self.table = f'{database}.{table}'
        self.data_source = data_source
        self.updated_by_field = updated_by_field
        self.updated_at_field = updated_at_field
        self.update_fields = {}

    def set(self, field, value) -> 'UpdateWrapper':
        self.update_fields[field] = value
        return self

    def read(self, entity) -> 'UpdateWrapper':
        if isinstance(entity, dict):
            for field in fields(self.data_source.get_data_structure(self.table)):
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
