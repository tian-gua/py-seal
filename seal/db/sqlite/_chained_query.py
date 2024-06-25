import traceback

from ._sqlite_connector import SqliteConnector
from ._meta import Meta
from .._base_chained_query import BaseChainedQuery
from ...model import PageResult
from loguru import logger


class ChainedQuery(BaseChainedQuery):

    def meta(self):
        return Meta

    def __init__(self, target, logic_delete_col: str = None):
        super().__init__(target, logic_delete_col=logic_delete_col, placeholder='?')
        self._conn = SqliteConnector().get_connection()

    def _get_cursor(self):
        return self._conn.cursor()

    def count(self, reuse_conn: bool = False):
        c = self._get_cursor()
        try:
            sql, args = self.count_statement()
            result = c.execute(sql, args)
            return result.fetchone()[0]
        except Exception as e:
            logger.error(f'数据库操作异常: {e}')
            logger.error(traceback.format_exc())
        finally:
            c.close()
            if reuse_conn is False:
                self._conn.close()

    def list(self, to_dict: bool = False, reuse_conn: bool = False):
        c = self._get_cursor()
        try:
            sql, args = self.select_statement()
            result = c.execute(sql, args)
            if result is None:
                return []

            return self.fetchall(result, to_dict=to_dict)
        except Exception as e:
            logger.error(f'数据库操作异常: {e}')
            logger.error(traceback.format_exc())
        finally:
            c.close()
            if reuse_conn is False:
                self._conn.close()

    def page(self, page: int = 1, page_size: int = 10, to_dict: bool = False, reuse_conn=False) -> PageResult:
        c = self._get_cursor()
        try:
            sql, args = self.page_statement(page, page_size)
            result = c.execute(sql, args)
            if result is None:
                return PageResult(page=page, page_size=page_size, total=0, data=[])
            entities = self.fetchall(result, to_dict=to_dict)
            total = self.count(reuse_conn=True)
            return PageResult(page=page, page_size=page_size, total=total, data=entities)
        except Exception as e:
            logger.error(f'数据库操作异常: {e}')
            logger.error(traceback.format_exc())
        finally:
            c.close()
            if reuse_conn is False:
                self._conn.close()

    def first(self, to_dict=False, reuse_conn: bool = False):
        c = self._get_cursor()
        try:
            sql, args = self.select_statement()
            result = c.execute(sql, args)
            if result is None:
                return None
            return self.fetchone(result, to_dict=to_dict)
        except Exception as e:
            logger.error(f'数据库操作异常: {e}')
            logger.error(traceback.format_exc())
        finally:
            c.close()
            if reuse_conn is False:
                self._conn.close()

    def mapping(self, to_dict: bool = False, reuse_conn: bool = False):
        c = self._get_cursor()
        try:
            sql, args = self.mapping_statement()
            result = c.execute(sql, args)
            if result is None:
                return []
            return self.fetchall(result, to_dict=to_dict)
        except Exception as e:
            logger.error(f'数据库操作异常: {e}')
            logger.error(traceback.format_exc())
        finally:
            c.close()
            if reuse_conn is False:
                self._conn.close()
