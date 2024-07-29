from .wrapper import Wrapper
from .sql_builder import build_update, build_delete
from dataclasses import fields


class UpdateWrapper(Wrapper):

    def __init__(self, table, data_source, logical_delete=None):
        super().__init__(logical_delete=logical_delete)
        self.table = table
        self.data_source = data_source
        self.update_fields = {}

    def set(self, field, value):
        self.update_fields[field] = value
        return self

    def set_model(self, entity):
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

        self._handle_logical_delete(**options)

        sql, args = build_update(self)
        return self.data_source.get_executor().update(sql, args)

    def delete(self, **options):
        if len(self.condition_tree.conditions) == 0:
            raise ValueError('不支持全量更新')

        self._handle_logical_delete(**options)

        if options.get('logical_delete', False):
            self.set(options['logical_delete'], 1)
            return self.update(**options)
        else:
            sql, args = build_delete(self)
            return self.data_source.get_executor().update(sql, args)
