from abc import ABC
from .table_info import TableField, TableInfo
from ..data_source import DataSource
from .connection_pool import ConnectionPool
from .executor import MysqlExecutor


class MysqlDataSource(DataSource, ABC):
    def __init__(self, conf):
        self.connection_pool = ConnectionPool(conf)
        self.table_info_dict = {}
        self.executor = MysqlExecutor(self)

    def get_connection(self):
        return self.connection_pool.get_connection(10)

    def get_data_structure(self, table):
        if table in self.table_info_dict:
            return self.table_info_dict[table].parse_model()

        conn = self.get_connection()
        c = conn.cursor()
        try:
            c.execute(f'show columns from {table}')
            rows = c.fetchall()
            table_fields = []
            for row in rows:
                table_field = TableField(field_=row['Field'],
                                         type_=row['Type'],
                                         null_=row['Null'],
                                         key_=row['Key'],
                                         default_=row['Default'],
                                         extra=row['Extra'])
                table_fields.append(table_field)

            table_info = TableInfo(table=table, table_fields=table_fields)
            return table_info.parse_model()
        finally:
            c.close()
            conn.close()

    def get_executor(self):
        return self.executor

    def get_type(self):
        return 'mysql'
