import datetime

from .wrapper import Wrapper
from .sql_builder import build_update, build_delete
from dataclasses import fields


class UpdateWrapper(Wrapper):

    def __init__(self,
                 table,
                 data_source,
                 tenant_field=None,
                 tenant_value=None,
                 update_by_field=None,
                 update_at_field=None,
                 logic_delete_field=None,
                 logic_delete_true=None,
                 logic_delete_false=None):
        super().__init__(tenant_field=tenant_field,
                         tenant_value=tenant_value,
                         logic_delete_field=logic_delete_field,
                         logic_delete_true=logic_delete_true,
                         logic_delete_false=logic_delete_false)
        self.update_by_field = update_by_field
        self.update_at_field = update_at_field
        self.table = table
        self.data_source = data_source
        self.update_fields = {}

    def set(self, field, value):
        self.update_fields[field] = value
        return self

    def read(self, entity):
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
            raise ValueError('不支持全量更新')

        self.handle_public_fields(**options)

        sql, args = build_update(self)
        return self.data_source.get_executor().update(sql, args)

    def delete(self, **options):
        if len(self.condition_tree.conditions) == 0:
            raise ValueError('不支持全量更新')

        self.handle_public_fields(**options)

        if options.get('logical_delete', False):
            self.set(options['logical_delete'], 1)
            return self.update(**options)
        else:
            sql, args = build_delete(self)
            return self.data_source.get_executor().update(sql, args)

    def handle_update_fields(self, **options):
        if self.update_at_field is not None:
            self.set(self.update_at_field, datetime.datetime.now())
        if self.update_by_field is not None:
            self.set(self.update_by_field, options.get('update_by', 'system'))
        return self
