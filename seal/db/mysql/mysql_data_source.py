from typing import Dict, Any

from .table_info import TableField, TableInfo
from .connection_pool import ConnectionPool
from .executor import MysqlExecutor
from ...protocol.executor_protocol import ExecutorProtocol


class MysqlDataSource:
    def __init__(self, name: str, conf: Dict[str, Any]):
        self.name = name
        self.default_database = conf.get('database')
        self.connection_pool = ConnectionPool(conf)
        self.executor: ExecutorProtocol = MysqlExecutor(self)

    def get_name(self) -> str:
        return self.name

    # noinspection PyMethodMayBeStatic
    def get_type(self) -> str:
        return 'mysql'

    def get_connection(self):
        return self.connection_pool.get_connection(10)

    def get_executor(self) -> ExecutorProtocol:
        return self.executor

    def get_default_database(self) -> str:
        return self.default_database

    def load_structure(self, data_source: str, table: str) -> Any:
        conn = self.get_connection()
        c = conn.cursor()
        try:
            c.execute(f'show columns from {data_source}.{table}')
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
