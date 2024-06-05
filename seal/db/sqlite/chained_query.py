from ..base_chained_query import BaseChainedQuery
from .sqlite_connector import SqliteConnector
from ...model import PageResult


class ChainedQuery(BaseChainedQuery):
    def __init__(self, clz, table: str = None, logic_delete_col: str = None):
        super().__init__(clz, '?', table, logic_delete_col)
        self.__conn = SqliteConnector().get_connection()

    def __get_cursor(self):
        return self.__conn.cursor()

    def count(self, reuse_conn: bool = False):
        c = self.__get_cursor()
        try:
            sql, args = self.count_statement()
            result = c.execute(sql, args)
            return result.fetchone()[0]
        except Exception as e:
            print(f'数据库操作异常: {e}')
        finally:
            c.close()
            if reuse_conn is False:
                self.__conn.close()

    def list(self, reuse_conn: bool = False):
        c = self.__get_cursor()
        try:
            sql, args = self.select_statement()
            result = c.execute(sql, args)
            if result is None:
                return []

            return self.fetchall(result)
        except Exception as e:
            print(f'数据库操作异常: {e}')
        finally:
            c.close()
            if reuse_conn is False:
                self.__conn.close()

    def page(self, page: int = 1, page_size: int = 10, reuse_conn=False) -> PageResult:
        c = self.__get_cursor()
        try:
            sql, args = self.page_statement(page, page_size)
            result = c.execute(sql, args)
            if result is None:
                return PageResult(page=page, page_size=page_size, total=0, data=[])
            entities = self.fetchall(result)
            total = self.count(reuse_conn=True)
            return PageResult(page=page, page_size=page_size, total=total, data=entities)
        except Exception as e:
            print(f'数据库操作异常: {e}')
        finally:
            c.close()
            if reuse_conn is False:
                self.__conn.close()

    def first(self, reuse_conn: bool = False):
        c = self.__get_cursor()
        try:
            sql, args = self.select_statement()
            result = c.execute(sql, args)
            if result is None:
                return None
            row = result.fetchone()
            if row is None:
                return None
            return self.clz(**{col: row[i] for i, col in enumerate(self.clz.columns())})
        except Exception as e:
            print(f'数据库操作异常: {e}')
        finally:
            c.close()
            if reuse_conn is False:
                self.__conn.close()

    def mapping(self, reuse_conn: bool = False):
        c = self.__get_cursor()
        try:
            sql, args = self.mapping_statement()
            result = c.execute(sql, args)
            if result is None:
                return []
            return self.fetchall(result)
        except Exception as e:
            print(f'数据库操作异常: {e}')
        finally:
            c.close()
            if reuse_conn is False:
                self.__conn.close()
