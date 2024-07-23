from loguru import logger
from ..data_source import DataSource
from ..executor import Executor


class SqliteExecutor(Executor):

    def __init__(self, data_source: DataSource):
        super().__init__(data_source)

    def find(self, sql, args, select_fields, result_type, **options):
        connection = self.data_source.get_connection()
        cursor = connection.cursor()
        try:
            result = cursor.execute(sql, args)
            if result is None:
                return None

            row = cursor.fetchone()
            if row is None:
                return None

            if options.get('to_dict', False) is True:
                return {field: row[i] for i, field in enumerate(select_fields)}
            return result_type(**{field: row[i] for i, field in enumerate(select_fields)})
        except Exception as e:
            logger.exception(e)
            raise e
        finally:
            cursor.close()
            connection.close()

    def find_list(self, sql, args, select_fields, result_type, **options):
        connection = self.data_source.get_connection()
        cursor = connection.cursor()
        try:
            result = cursor.execute(sql, args)
            if result is None:
                return None

            rows = cursor.fetchall()
            if rows is None:
                return None

            if options.get('to_dict', False) is True:
                return [{field: row[i] for i, field in enumerate(select_fields)} for row in rows]
            return [result_type(**{field: row[i] for i, field in enumerate(select_fields)}) for row in rows]
        except Exception as e:
            logger.exception(e)
            raise e
        finally:
            cursor.close()
            connection.close()

    def count(self, sql, args):
        connection = self.data_source.get_connection()
        cursor = connection.cursor()
        try:
            result = cursor.execute(sql, args)
            if result is None:
                return None

            row = cursor.fetchone()
            if row is None:
                return None
            return row[0]
        except Exception as e:
            logger.exception(e)
            raise e
        finally:
            cursor.close()
            connection.close()

    def update(self, sql, args):
        connection = self.data_source.get_connection()
        cursor = connection.cursor()
        try:
            result = cursor.execute(sql, args)
            connection.commit()
            if result is None:
                return None
            return cursor.rowcount
        except Exception as e:
            logger.exception(e)
            raise e
        finally:
            cursor.close()
            connection.close()

    def insert(self, sql, args):
        connection = self.data_source.get_connection()
        cursor = connection.cursor()
        try:
            result = cursor.execute(sql, args)
            connection.commit()
            if result is None:
                return None
            return cursor.lastrowid
        except Exception as e:
            logger.exception(e)
            raise e
        finally:
            cursor.close()
            connection.close()

    def insert_bulk(self, sql, args):
        connection = self.data_source.get_connection()
        cursor = connection.cursor()
        try:
            result = cursor.executemany(sql, args)
            connection.commit()
            if result is None:
                return None
            return cursor.rowcount
        except Exception as e:
            logger.exception(e)
            raise e
        finally:
            cursor.close()
            connection.close()

    def insert_interator(self, data_iterator):
        connection = self.data_source.get_connection()
        cursor = connection.cursor()
        try:
            data_iterator(lambda sql, args: cursor.execute(sql, args))
            connection.commit()
            return cursor.rowcount
        except Exception as e:
            logger.exception(e)
            raise e
        finally:
            cursor.close()
            connection.close()
