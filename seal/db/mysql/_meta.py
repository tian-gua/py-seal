from datetime import datetime
from ._mysql_connector import MysqlConnector
from ..table_info import TableInfo
from loguru import logger


class Meta:

    @staticmethod
    def get_table_info(table: str) -> TableInfo:
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
            logger.debug(f'获取表{table}的结构信息：{table_info}')
            return table_info
        finally:
            c.close()
            conn.close()
