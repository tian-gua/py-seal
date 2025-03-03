import sqlite3
from sqlite3 import Connection
from typing import Any, Dict

from seal.db.protocol import IExecutor, IDatabaseConnection
from .executor import SqliteExecutor
from .sqlite_connection import SqliteConnection
from .table_info import TableField, TableInfo


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class SqliteDataSource:
    def __init__(self, name: str, conf: Dict[str, Any]):
        self.name = name
        self.src = conf['src']
        self.executor: IExecutor = SqliteExecutor(self)

    def get_name(self) -> str:
        return self.name

    # noinspection PyMethodMayBeStatic
    def get_type(self) -> str:
        return 'sqlite'

    def get_executor(self) -> IExecutor:
        return self.executor

    def get_connection(self) -> IDatabaseConnection:
        conn: Connection = sqlite3.connect(self.src)
        conn.row_factory = dict_factory
        sqlite_connection: SqliteConnection = SqliteConnection(conn)
        return sqlite_connection

    # noinspection PyMethodMayBeStatic
    def get_default_database(self) -> str:
        return ''

    def load_structure(self, database: str, table: str) -> Any:
        conn = self.get_connection()
        c = conn.cursor()
        try:
            result = c.execute(f'PRAGMA table_info({table})')
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

    def ping(self, seconds):
        pass
