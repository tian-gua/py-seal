from ..base_chained_query import BaseChainedQuery
from .mysql_connector import MysqlConnector
from ...model import PageResult


class ChainedQuery(BaseChainedQuery):
    def __init__(self, clz=None, table: str = None, logic_delete_col: str = None):
        if clz is None and table is None:
            raise ValueError('clz和table不能同时为空')
        super().__init__(clz=clz, placeholder='?', table=table, logic_delete_col=logic_delete_col)
        self.__conn = MysqlConnector().get_connection()

    def __get_cursor(self):
        return self.__conn.cursor()

    def count(self, reuse_conn: bool = False):
        c = self.__get_cursor()
        try:
            sql, args = self.count_statement()
            c.execute(sql, args)
            return c.fetchone()[0]
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
            c.execute(sql, args)
            return self.fetchall(c)
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
            c.execute(sql, args)
            entities = self.fetchall(c)
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
            c.execute(sql, args)
            row = self.fetchone(c)
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
            c.execute(sql, args)
            return self.fetchall(c)
        except Exception as e:
            print(f'数据库操作异常: {e}')
        finally:
            c.close()
            if reuse_conn is False:
                self.__conn.close()
