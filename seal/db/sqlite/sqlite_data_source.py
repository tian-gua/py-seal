import sqlite3
from abc import ABC
from typing import Any, Dict

from .table_info import TableField, TableInfo
from .executor import SqliteExecutor
from ..data_source import DataSource


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class SqliteDataSource(DataSource, ABC):
    def __init__(self, conf):
        self.src = conf['src']
        self.models: Dict[str, Any] = {}
        self.executor = SqliteExecutor(self)

    def get_connection(self):
        conn = sqlite3.connect(self.src)
        conn.row_factory = dict_factory
        return conn

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
                table_field = TableField(cid=row['cid'],
                                         name=row['name'],
                                         type_=row['type'],
                                         notnull=row['notnull'],
                                         dflt_value=row['dflt_value'],
                                         pk=row['pk'])
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
