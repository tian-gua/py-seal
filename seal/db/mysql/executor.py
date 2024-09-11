from loguru import logger
from ..data_source import DataSource
from ..result import Result, Results
from ..executor import Executor


class MysqlExecutor(Executor):

    def __init__(self, data_source: DataSource):
        super().__init__(data_source)

    def find(self, sql, args, bean_type, **options) -> Result:
        logger.debug(f'#### sql: {sql}')
        logger.debug(f'#### args: {args}')

        sql = sql.replace('?', '%s')
        connection = self.data_source.get_connection()
        connection.begin()
        cursor = connection.cursor()
        try:
            result = cursor.execute(sql, args)
            if result is None:
                return Result.empty()

            row = cursor.fetchone()
            if row is None:
                return Result.empty()

            return Result(row=row, bean_type=bean_type)
        except Exception as e:
            logger.exception(e)
            raise e
        finally:
            cursor.close()
            connection.commit()
            connection.close()

    def find_list(self, sql, args, bean_type, **options) -> Results:
        logger.debug(f'#### sql: {sql}')
        logger.debug(f'#### args: {args}')

        sql = sql.replace('?', '%s')
        connection = self.data_source.get_connection()
        connection.begin()
        cursor = connection.cursor()
        try:
            result = cursor.execute(sql, args)
            if result is None:
                return Results.empty()

            rows = cursor.fetchall()
            if rows is None:
                return Results.empty()

            return Results(rows=rows, bean_type=bean_type)
        except Exception as e:
            logger.exception(e)
            raise e
        finally:
            cursor.close()
            connection.commit()
            connection.close()

    def count(self, sql, args):
        logger.debug(f'#### sql: {sql}')
        logger.debug(f'#### args: {args}')

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
        logger.debug(f'#### sql: {sql}')
        logger.debug(f'#### args: {args}')

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
        logger.debug(f'#### sql: {sql}')
        logger.debug(f'#### args: {args}')

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
        logger.debug(f'#### sql: {sql}')
        logger.debug(f'#### args: {args}')

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

    def raw(self, sql, args=()) -> Results:
        logger.debug(f'#### sql: {sql}')
        logger.debug(f'#### args: {args}')

        sql = sql.replace('?', '%s')
        connection = self.data_source.get_connection()
        connection.begin()
        cursor = connection.cursor()
        try:
            result = cursor.execute(sql, args)
            if result is None:
                return Results.empty()

            rows = cursor.fetchall()
            if rows is None:
                return Results.empty()

            return Results(rows=rows)
        except Exception as e:
            logger.exception(e)
            raise e
        finally:
            cursor.close()
            connection.commit()
            connection.close()
