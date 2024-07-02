from datetime import datetime
from ._sqlite_connector import SqliteConnector
from ..table_info import TableInfo
from loguru import logger


class Meta:

    @staticmethod
    def get_table_info(table: str) -> TableInfo:
        conn = SqliteConnector().get_connection()
        c = conn.cursor()
        try:
            result = c.execute(f'PRAGMA table_info({table})')
            rows = result.fetchall()
            table_info = TableInfo()
            for row in rows:
                table_info.columns.append((row[1], row[2]))
                if row[2] == 'INTEGER':
                    table_info.model_fields.append((row[1], int))
                elif row[2] == 'TEXT':
                    table_info.model_fields.append((row[1], str))
                elif row[2] == 'REAL':
                    table_info.model_fields.append((row[1], float))
                elif row[2] == 'BLOB':
                    table_info.model_fields.append((row[1], bytes))
                elif row[2] == 'DATETIME':
                    table_info.model_fields.append((row[1], datetime))
                elif row[2] == 'NULL':
                    table_info.model_fields.append((row[1], None))
                else:
                    table_info.model_fields.append((row[1], str))
            logger.debug(f'获取表{table}的结构信息：{table_info}')
            return table_info
        finally:
            c.close()
            conn.close()
