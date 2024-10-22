from dataclasses import fields
from .sql_builder import build_insert, build_insert_bulk
from .structures import structures
from ..protocol.data_source_protocol import IDataSource


class InsertWrapper:
    def __init__(self,
                 table,
                 database=None,
                 data_source: IDataSource = None,
                 tenant_field=None,
                 tenant_value=None,
                 logical_deleted_field=None,
                 logical_deleted_value_true=None,
                 logical_deleted_value_false=None):
        self.table = table
        if database is not None and database != '':
            self.table = f'{database}.{table}'
        self.data_source = data_source

        self.param_type = structures.get(data_source=self.data_source.get_name(),
                                         database=database or data_source.get_default_database(),
                                         table=table)
        if self.param_type is None:
            self.param_type = self.data_source.load_structure(database, table)
            structures.register(data_source=self.data_source.get_name(),
                                database=database or data_source.get_default_database(),
                                table=table,
                                structure=self.param_type)

        self.tenant_field = tenant_field
        self.tenant_value = tenant_value
        self.logical_deleted_field = logical_deleted_field
        self.logical_deleted_value_true = logical_deleted_value_true
        self.logical_deleted_value_false = logical_deleted_value_false
        self.insert_fields = []

    def insert(self, data, **options):
        if data is None:
            raise ValueError('null data')

        if isinstance(data, dict):
            self.handle_data_public_fields(data, True)
        else:
            self.handle_data_public_fields(data, False)
        if 'duplicated_key_update' in options:
            sql, args = build_insert(self, data, options['duplicated_key_update'])
        else:
            sql, args = build_insert(self, data)
        return self.data_source.get_executor().insert(sql, args)

    def insert_bulk(self, data_list, **options):
        if data_list is None or len(data_list) == 0:
            raise ValueError('null data')

        if isinstance(data_list[0], dict):
            self.handle_data_list_public_fields(data_list, True)
        else:
            self.handle_data_list_public_fields(data_list, False)

        if 'duplicated_key_update' in options:
            sql, args = build_insert_bulk(self, data_list, duplicated_key_update=options['duplicated_key_update'])
        elif 'duplicated_key_ignore' in options:
            sql, args = build_insert_bulk(self, data_list, duplicated_key_ignore=options['duplicated_key_ignore'])
        else:
            sql, args = build_insert_bulk(self, data_list)
        return self.data_source.get_executor().insert_bulk(sql, args)

    # def insert_iterator(self, data_list, **options):
    #     if data_list is None or len(data_list) == 0:
    #         raise ValueError('null data')
    #
    #     if isinstance(data_list[0], dict):
    #         self.handle_data_list_public_fields(data_list, True)
    #     else:
    #         self.handle_data_list_public_fields(data_list, False)
    #
    #     data_iterator = build_insert_iterator(self, data_list, **options)
    #     return self.data_source.get_executor().insert_interator(data_iterator)

    def handle_data_public_fields(self, data, is_dict):
        if is_dict:
            if self.logical_deleted_field is not None:
                if self.logical_deleted_value_true is None or self.logical_deleted_value_false is None:
                    raise ValueError('logic delete field and value is required')
                data[self.logical_deleted_field] = self.logical_deleted_value_false
            if self.tenant_field is not None:
                if self.tenant_value is None:
                    raise ValueError('tenant value not set')
                data[self.tenant_field] = self.tenant_value

            keys = data.keys()
            self.insert_fields = [f.name for f in fields(self.param_type) if
                                  f.name != 'id' and f.name in keys]
        else:
            if self.logical_deleted_field is not None:
                if self.logical_deleted_value_true is None or self.logical_deleted_value_false is None:
                    raise ValueError('logic delete field and value is required')
                setattr(data, self.logical_deleted_field, self.logical_deleted_value_false)
            if self.tenant_field is not None:
                if self.tenant_value is None:
                    raise ValueError('tenant value not set')
                setattr(data, self.tenant_field, self.tenant_value)

            self.insert_fields = [f.name for f in fields(self.param_type) if
                                  f.name != 'id' and getattr(data, f.name) is not None]

    def handle_data_list_public_fields(self, data_list, is_dict):
        if is_dict:
            if self.logical_deleted_field is not None:
                if self.logical_deleted_value_true is None or self.logical_deleted_value_false is None:
                    raise ValueError('logic delete field and value is required')
                map(lambda x: x.update({self.logical_deleted_field: self.logical_deleted_value_false}), data_list)
            if self.tenant_field is not None:
                if self.tenant_value is None:
                    raise ValueError('tenant value not set')
                map(lambda x: x.update({self.tenant_field: self.tenant_value}), data_list)

            keys = data_list[0].keys()
            self.insert_fields = [f.name for f in fields(self.param_type) if
                                  f.name != 'id' and f.name in keys]
        else:
            if self.logical_deleted_field is not None:
                if self.logical_deleted_value_true is None or self.logical_deleted_value_false is None:
                    raise ValueError('logic delete field and value is required')
                map(lambda x: setattr(x, self.logical_deleted_field, self.logical_deleted_value_false), data_list)
            if self.tenant_field is not None:
                if self.tenant_value is None:
                    raise ValueError('tenant value not set')
                map(lambda x: setattr(x, self.tenant_field, self.tenant_value), data_list)

            self.insert_fields = [f.name for f in fields(self.param_type) if
                                  f.name != 'id' and getattr(data_list[0], f.name) is not None]
