from .wrapper import Wrapper
from .sql_builder import build_update, build_delete


class UpdateWrapper(Wrapper):

    def __init__(self, table, data_source, logical_delete=None):
        super().__init__(logical_delete=logical_delete)
        self.table = table
        self.data_source = data_source
        self.update_fields = {}

    def set(self, field, value):
        self.update_fields[field] = value
        return self

    def update(self, **options):
        self._handle_logical_delete(**options)

        sql, args = build_update(self)
        return self.data_source.get_executor().update(sql, args)

    def delete(self, **options):
        self._handle_logical_delete(**options)

        if options.get('logical_delete', False):
            self.set(options['logical_delete'], 1)
            return self.update(**options)
        else:
            sql, args = build_delete(self)
            return self.data_source.get_executor().update(sql, args)
