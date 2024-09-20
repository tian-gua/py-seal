import sqlite3
from typing import Any, Dict

from .table_info import TableField, TableInfo
from .executor import SqliteExecutor
from ...protocol.executor_protocol import ExecutorProtocol


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class SqliteDataSource:
    def __init__(self, name: str, conf: Dict[str, Any]):
        self.name = name
        self.src = conf['src']
        self.default_database = conf.get('database')
        self.executor: ExecutorProtocol = SqliteExecutor(self)

    def get_name(self) -> str:
        return self.name

    # noinspection PyMethodMayBeStatic
    def get_type(self) -> str:
        return 'sqlite'

    def get_executor(self) -> ExecutorProtocol:
        return self.executor

    def get_connection(self):
        conn = sqlite3.connect(self.src)
        conn.row_factory = dict_factory
        return conn

    def get_default_database(self) -> str:
        return self.default_database

    def load_structure(self, database: str, table: str) -> Any:
        conn = self.get_connection()
        c = conn.cursor()
        try:
            result = c.execute(f'PRAGMA table_info({database}.{table})')
            rows = result.fetchall()
            table_fields = []
            for row in rows:
                table_field = TableField(cid=row['cid'],
                                         name=row['name'],
                                         type_=row['type'],
                                         notnull=row['notnull'],
                                         dflt_value=row['dflt_value'],
                                         pk=row['pk'])
                table_fields.append(table_field)

            table_info = TableInfo(table=table, table_fields=table_fields)
            return table_info.parse_model()
        finally:
            c.close()
            conn.close()
