import sqlite3
from abc import ABC
from typing import Any, Dict

from .table_info import TableField, TableInfo
from .executor import SqliteExecutor
from ..data_source import DataSource


class SqliteDataSource(DataSource, ABC):
    def __init__(self, conf):
        self.src = conf['src']
        self.models: Dict[str, Any] = {}
        self.executor = SqliteExecutor(self)

    def get_connection(self):
        return sqlite3.connect(self.src)

    def get_data_structure(self, table):
        if table in self.models:
            return self.models[table]

        conn = self.get_connection()
        c = conn.cursor()
        try:
            result = c.execute(f'PRAGMA table_info({table})')
            rows = result.fetchall()
            table_fields = []
            for row in rows:
                table_field = TableField(cid=row[0],
                                         name=row[1],
                                         type_=row[2],
                                         notnull=row[3],
                                         dflt_value=row[4],
                                         pk=row[5])
                table_fields.append(table_field)

            table_info = TableInfo(table=table, table_fields=table_fields)
            self.models[table] = table_info.parse_model()
            return self.models[table]
        finally:
            c.close()
            conn.close()

    def get_executor(self):
        return self.executor

    def get_type(self):
        return 'sqlite'
