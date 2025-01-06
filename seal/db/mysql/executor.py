from typing import Tuple, Any, List

from loguru import logger

from seal.db.protocol import IDatabaseConnection
from seal.db.protocol.data_source_protocol import IDataSource
from seal.db.transaction import sql_context
from seal.model.result import Result, Results


class MysqlExecutor:

    def __init__(self, data_source: IDataSource):
        self.data_source = data_source

    def find(self, sql: str, args: Tuple[Any, ...], bean_type: Any) -> Result:
        self.debug(sql, args)

        connection: IDatabaseConnection = self.get_connection()
        cursor = connection.cursor()
        try:
            sql = sql.replace('?', '%s')
            result = cursor.execute(sql, args)
            if result is None:
                return Result.empty()

            row = cursor.fetchone()
            if row is None:
                return Result.empty()

            return Result(row=row, bean_type=bean_type)
        except Exception as e:
            raise e
        finally:
            cursor.close()
            self.close_connection(connection)

    def find_list(self, sql: str, args: Tuple[Any, ...], bean_type: Any) -> Results:
        self.debug(sql, args)

        connection: IDatabaseConnection = self.get_connection()
        cursor = connection.cursor()
        try:
            sql = sql.replace('?', '%s')
            result = cursor.execute(sql, args)
            if result is None:
                return Results.empty()

            rows = cursor.fetchall()
            if rows is None:
                return Results.empty()

            return Results(rows=rows, bean_type=bean_type)
        except Exception as e:
            raise e
        finally:
            cursor.close()
            self.close_connection(connection)

    def count(self, sql: str, args: Tuple[Any, ...]) -> int | None:
        self.debug(sql, args)

        connection: IDatabaseConnection = self.get_connection()
        cursor = connection.cursor()
        try:
            sql = sql.replace('?', '%s')
            result = cursor.execute(sql, args)
            if result is None:
                return None

            row = cursor.fetchone()
            if row is None:
                return None

            return row['COUNT(1)']
        except Exception as e:
            raise e
        finally:
            cursor.close()
            self.close_connection(connection)

    def update(self, sql: str, args: Tuple[Any, ...]) -> int | None:
        self.debug(sql, args)

        connection: IDatabaseConnection = self.get_connection()
        cursor = connection.cursor()
        try:
            sql = sql.replace('?', '%s')
            result = cursor.execute(sql, args)
            if result is None:
                return None
            return result
        except Exception as e:
            raise e
        finally:
            cursor.close()
            self.close_connection(connection)

    def insert(self, sql: str, args: Tuple[Any, ...]) -> int | None:
        self.debug(sql, args)

        connection: IDatabaseConnection = self.get_connection()
        cursor = connection.cursor()
        try:
            sql = sql.replace('?', '%s')
            result = cursor.execute(sql, args)
            if result is None:
                return None
            return cursor.lastrowid
        except Exception as e:
            raise e
        finally:
            cursor.close()
            self.close_connection(connection)

    def insert_bulk(self, sql: str, args: List[Tuple[Any, ...]]) -> int | None:
        logger.debug(f'#### sql: {sql}')

        connection: IDatabaseConnection = self.get_connection()
        cursor = connection.cursor()
        try:
            sql = sql.replace('?', '%s')
            row_affected = 0
            for row_args in args:
                logger.debug(f'#### args: {row_args}')
                row_affected += cursor.execute(sql, row_args) or 0
            logger.debug(f'#### row_affected: {row_affected}')
            connection.commit()
            return row_affected
        except Exception as e:
            connection.rollback()
            raise e
        finally:
            cursor.close()
            self.close_connection(connection)

    def custom_query(self, sql: str, args: Tuple[Any, ...]) -> Results:
        self.debug(sql, args)

        connection: IDatabaseConnection = self.get_connection()
        cursor = connection.cursor()
        try:
            sql = sql.replace('?', '%s')
            result = cursor.execute(sql, args)
            if result is None:
                return Results.empty()

            rows = cursor.fetchall()
            if rows is None:
                return Results.empty()

            return Results(rows=rows)
        except Exception as e:
            raise e
        finally:
            cursor.close()
            self.close_connection(connection)

    def custom_update(self, sql: str, args: Tuple[Any, ...]) -> int | None:
        self.debug(sql, args)

        connection: IDatabaseConnection = self.get_connection()
        cursor = connection.cursor()
        try:
            sql = sql.replace('?', '%s')
            result = cursor.execute(sql, args)
            if result is None:
                return None
            return result
        except Exception as e:
            raise e
        finally:
            cursor.close()
            self.close_connection(connection)

    def get_connection(self) -> IDatabaseConnection:
        ctx = sql_context.get()
        connection: IDatabaseConnection = ctx.tx() or self.data_source.get_connection()

        if ctx.tx() is None:
            connection.begin()

        return connection

    # noinspection PyMethodMayBeStatic
    def close_connection(self, connection: IDatabaseConnection):
        ctx = sql_context.get()
        if ctx.tx() is None:
            connection.commit()
            connection.close()

    # noinspection PyMethodMayBeStatic
    def debug(self, sql, args):
        logger.debug(f'#### sql: {sql}')
        logger.debug(f'#### args: {args}')
        # ctx = sql_context.get()
        # if ctx.tx() is not None:
        #     logger.debug(f'#### Transaction id: {ctx.tx_id()}')
