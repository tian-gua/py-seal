from .data_source import DataSource
from loguru import logger


class Executor:

    def __init__(self, data_source: DataSource):
        self.data_source = data_source

    def raw(self, sql, args=()):
        cursor = self.data_source.get_connection().cursor()
        try:
            result = cursor.execute(sql, args)
            if result is None:
                return None

            return cursor.fetchall()
        except Exception as e:
            logger.exception(e)
            raise e
        finally:
            cursor.close()
