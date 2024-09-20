from typing import Any, Tuple, List

from loguru import logger
from seal.model.result import Result, Results
from seal.protocol.data_source_protocol import DataSourceProtocol


class SqliteExecutor:

    def __init__(self, data_source: DataSourceProtocol):
        self.data_source = data_source

    def find(self, sql: str, args: Tuple[Any, ...], bean_type: Any) -> Result:
        logger.debug(f'#### sql: {sql}')
        logger.debug(f'#### args: {args}')

        connection = self.data_source.get_connection()
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
            connection.close()

    def find_list(self, sql: str, args: Tuple[Any, ...], bean_type: Any) -> Results:
        logger.debug(f'#### sql: {sql}')
        logger.debug(f'#### args: {args}')

        connection = self.data_source.get_connection()
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
            connection.close()

    def count(self, sql: str, args: Tuple[Any, ...]) -> int | None:
        logger.debug(f'#### sql: {sql}')
        logger.debug(f'#### args: {args}')

        connection = self.data_source.get_connection()
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
            connection.close()

    def update(self, sql: str, args: Tuple[Any, ...]) -> int | None:
        logger.debug(f'#### sql: {sql}')
        logger.debug(f'#### args: {args}')

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

    def insert(self, sql: str, args: Tuple[Any, ...]) -> int | None:
        logger.debug(f'#### sql: {sql}')
        logger.debug(f'#### args: {args}')

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

    def insert_bulk(self, sql: str, args: List[Tuple[Any, ...]]) -> int | None:
        logger.debug(f'#### sql: {sql}')
        logger.debug(f'#### args: {args}')

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

    # def insert_interator(self, data_iterator):
    #     connection = self.data_source.get_connection()
    #     cursor = connection.cursor()
    #     try:
    #         data_iterator(lambda sql, args: cursor.execute(sql, args))
    #         connection.commit()
    #         return cursor.rowcount
    #     except Exception as e:
    #         logger.exception(e)
    #         raise e
    #     finally:
    #         cursor.close()
    #         connection.close()

    def custom_query(self, sql: str, args: Tuple[Any, ...]) -> Results:
        logger.debug(f'#### sql: {sql}')
        logger.debug(f'#### args: {args}')

        connection = self.data_source.get_connection()
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
            connection.close()

    def custom_update(self, sql: str, args: Tuple[Any, ...]) -> int | None:
        logger.debug(f'#### sql: {sql}')
        logger.debug(f'#### args: {args}')

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
