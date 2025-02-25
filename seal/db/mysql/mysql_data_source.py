from typing import Dict, Any

from seal.db.protocol import IExecutor
from .executor import MysqlExecutor
from .mysql_connection import ConnectionPool
from .table_info import TableField, TableInfo


class MysqlDataSource:
    def __init__(self, name: str, conf: Dict[str, Any]):
        self.name = name
        self.default_database = conf.get('database')
        self.connection_pool = ConnectionPool(conf)
        self.executor: IExecutor = MysqlExecutor(self)

    def get_name(self) -> str:
        return self.name

    # noinspection PyMethodMayBeStatic
    def get_type(self) -> str:
        return 'mysql'

    def get_connection(self):
        return self.connection_pool.get_connection()

    def get_executor(self) -> IExecutor:
        return self.executor

    def get_default_database(self) -> str:
        return self.default_database

    def load_structure(self, database: str, table: str) -> Any:
        conn = self.get_connection()
        conn.begin()
        c = conn.cursor()
        try:
            c.execute(f'show columns from {database}.{table}')
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
            conn.commit()
            conn.close()

    def ping(self, seconds):
        self.connection_pool.ping(seconds)
