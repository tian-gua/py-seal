from dataclasses import fields
from .sql_builder import build_insert, build_insert_bulk, build_insert_iterator


class InsertWrapper:
    def __init__(self, table, data_source):
        self.table = table
        self.data_source = data_source
        self.param_type = data_source.get_data_structure(table)
        self.insert_fields = []

    def insert(self, data, **options):
        if data is None:
            raise ValueError('null data')

        if isinstance(data, dict):
            keys = data.keys()
            self.insert_fields = [f.name for f in fields(self.param_type) if
                                  f.name != 'id' and f.name in keys]
        else:
            self.insert_fields = [f.name for f in fields(self.param_type) if
                                  f.name != 'id' and getattr(data, f.name) is not None]

        if 'duplicated_key_update' in options:
            sql, args = build_insert(self, data, options['duplicated_key_update'])
        else:
            sql, args = build_insert(self, data)
        return self.data_source.get_executor().insert(sql, args)

    def insert_bulk(self, data_list, **options):
        if data_list is None or len(data_list) == 0:
            raise ValueError('null data')

        if isinstance(data_list[0], dict):
            keys = data_list[0].keys()
            self.insert_fields = [f.name for f in fields(self.param_type) if
                                  f.name != 'id' and f.name in keys]
        else:
            self.insert_fields = [f.name for f in fields(self.param_type) if
                                  f.name != 'id' and getattr(data_list[0], f.name) is not None]

        if 'duplicated_key_update' in options:
            sql, args = build_insert_bulk(self, data_list, duplicated_key_update=options['duplicated_key_update'])
        elif 'duplicated_key_ignore' in options:
            sql, args = build_insert_bulk(self, data_list, duplicated_key_ignore=options['duplicated_key_ignore'])
        else:
            sql, args = build_insert_bulk(self, data_list)
        return self.data_source.get_executor().insert_bulk(sql, args)

    def insert_iterator(self, data_list, **options):
        if data_list is None or len(data_list) == 0:
            raise ValueError('null data')

        if isinstance(data_list[0], dict):
            keys = data_list[0].keys()
            self.insert_fields = [f.name for f in fields(self.param_type) if
                                  f.name != 'id' and f.name in keys]
        else:
            self.insert_fields = [f.name for f in fields(self.param_type) if
                                  f.name != 'id' and getattr(data_list[0], f.name) is not None]

        data_iterator = build_insert_iterator(self, data_list, **options)
        return self.data_source.get_executor().insert_interator(data_iterator)
