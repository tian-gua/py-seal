from loguru import logger
from ..data_source import DataSource
from ..executor import Executor


class MysqlExecutor(Executor):

    def __init__(self, data_source: DataSource):
        super().__init__(data_source)

    def find(self, sql, args, select_fields, result_type, **options):
        sql = sql.replace('?', '%s')
        connection = self.data_source.get_connection()
        connection.begin()
        cursor = connection.cursor()
        try:
            result = cursor.execute(sql, args)
            if result is None:
                return None

            row = cursor.fetchone()
            if row is None:
                return None

            if options.get('to_dict', False) is True:
                return row
            return result_type(**{field: row[field] for _, field in enumerate(select_fields)})
        except Exception as e:
            logger.exception(e)
            raise e
        finally:
            cursor.close()
            connection.commit()
            connection.close()

    def find_list(self, sql, args, select_fields, result_type, **options):
        sql = sql.replace('?', '%s')
        connection = self.data_source.get_connection()
        connection.begin()
        cursor = connection.cursor()
        try:
            result = cursor.execute(sql, args)
            if result is None:
                return None

            rows = cursor.fetchall()
            if rows is None:
                return None

            if options.get('to_dict', False) is True:
                return [row for row in rows]
            return [result_type(**{field: row[field] for _, field in enumerate(select_fields)}) for row in rows]
        except Exception as e:
            logger.exception(e)
            raise e
        finally:
            cursor.close()
            connection.commit()
            connection.close()

    def count(self, sql, args):
        sql = sql.replace('?', '%s')
        connection = self.data_source.get_connection()
        connection.begin()
        cursor = connection.cursor()
        try:
            result = cursor.execute(sql, args)
            if result is None:
                return None

            row = cursor.fetchone()
            if row is None:
                return None

            return row['COUNT(1)']
        except Exception as e:
            logger.exception(e)
            raise e
        finally:
            cursor.close()
            connection.commit()
            connection.close()

    def update(self, sql, args):
        sql = sql.replace('?', '%s')
        connection = self.data_source.get_connection()
        connection.begin()
        cursor = connection.cursor()
        try:
            result = cursor.execute(sql, args)
            if result is None:
                return None
            return result
        except Exception as e:
            logger.exception(e)
            raise e
        finally:
            cursor.close()
            connection.commit()
            connection.close()

    def insert(self, sql, args):
        sql = sql.replace('?', '%s')
        connection = self.data_source.get_connection()
        connection.begin()
        cursor = connection.cursor()
        try:
            result = cursor.execute(sql, args)
            if result is None:
                return None
            return result
        except Exception as e:
            logger.exception(e)
            raise e
        finally:
            cursor.close()
            connection.commit()
            connection.close()

    def insert_bulk(self, sql, args):
        sql = sql.replace('?', '%s')
        connection = self.data_source.get_connection()
        connection.begin()
        cursor = connection.cursor()
        try:
            for args in args:
                cursor.execute(sql, args)
        except Exception as e:
            logger.exception(e)
            raise e
        finally:
            cursor.close()
            connection.commit()
            connection.close()
