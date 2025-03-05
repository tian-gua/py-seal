from typing import List

from loguru import logger

from seal.db.protocol.data_source_protocol import IDataSource
from seal.db.result import Result, Results


class SqliteExecutor:

    def __init__(self, data_source: IDataSource):
        self.data_source = data_source

    def find(self, sql: str, args: tuple[any, ...], model: object) -> Result | None:
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

            return Result(row=row, model=model)
        except Exception as e:
            logger.exception(e)
            raise e
        finally:
            cursor.close()
            connection.close()

    def find_list(self, sql: str, args: tuple[any, ...], model: object) -> Results | None:
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

            return Results(rows=rows, model=model)
        except Exception as e:
            logger.exception(e)
            raise e
        finally:
            cursor.close()
            connection.close()

    def count(self, sql: str, args: tuple[any, ...]) -> int | None:
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

    def update(self, sql: str, args: tuple[any, ...]) -> int | None:
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

    def insert(self, sql: str, args: tuple[any, ...]) -> int | None:
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

    def insert_bulk(self, sql: str, args: List[tuple[any, ...]]) -> int | None:
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

    def custom_query(self, sql: str, args: tuple[any, ...]) -> Results | None:
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

    def custom_update(self, sql: str, args: tuple[any, ...]) -> int | None:
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
