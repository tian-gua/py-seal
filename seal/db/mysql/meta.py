from datetime import datetime
from .mysql_connector import MysqlConnector
from ..table_info import TableInfo
from ...cache import Cache


class Meta:

    @staticmethod
    def get_table_info(table: str) -> TableInfo:
        table_info_cache = Cache().get(f'table_info_{table}')
        if table_info_cache is not None:
            return table_info_cache

        conn = MysqlConnector().get_connection()
        c = conn.cursor()
        try:
            c.execute(f'show columns from {table}')
            rows = c.fetchall()
            table_info = TableInfo()
            for row in rows:
                field_type: str = row['Type']
                field_name: str = row['Field']
                table_info.columns.append((field_name, field_type))
                if field_type.startswith('bigint') or field_type.startswith('int') or field_type.startswith('tinyint'):
                    table_info.model_fields.append((field_name, int))
                elif field_type.startswith('varchar') or field_type.startswith('text'):
                    table_info.model_fields.append((field_name, str))
                elif field_type.startswith('decimal') or field_type.startswith('float'):
                    table_info.model_fields.append((field_name, float))
                elif field_type.startswith('blob'):
                    table_info.model_fields.append((field_name, bytes))
                elif field_type.startswith('datetime'):
                    table_info.model_fields.append((field_name, datetime))
                else:
                    table_info.model_fields.append((field_name, str))
            Cache().set(f'table_info_{table}', table_info)
            return table_info
        finally:
            c.close()
            conn.close()