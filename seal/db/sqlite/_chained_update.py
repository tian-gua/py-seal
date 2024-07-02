import traceback

from ._meta import Meta
from ._sqlite_connector import SqliteConnector
from .._base_chained_update import BaseChainedUpdate
from ...context import WebContext
from ...model import BaseEntity
from datetime import datetime
from loguru import logger


class ChainedUpdate(BaseChainedUpdate):

    def meta(self):
        return Meta

    def __init__(self, target, logic_delete_col: str = None):
        super().__init__(target, logic_delete_col, '?')
        self._conn = SqliteConnector().get_connection()

    def _get_cursor(self):
        return self._conn.cursor()

    def logic_delete(self):
        self.sets['deleted'] = 1
        self.update()

    def insert(self, record, duplicated_ignore=False, reuse_conn: bool = False):
        c = self._get_cursor()
        try:
            sql, args = self.insert_statement(record, duplicated_ignore=duplicated_ignore)
            c.execute(sql, args)
            self._conn.commit()
        except Exception as e:
            logger.error(f'数据库操作异常: {e}')
            logger.error(traceback.format_exc())
        finally:
            c.close()
            if reuse_conn is False:
                self._conn.close()

    def insert_bulk(self, records: list[dict] = None, duplicated_ignore: bool = False, reuse_conn: bool = False):
        if records is None:
            raise ValueError('null data')

        c = self._get_cursor()
        try:
            sql = self.insert_bulk_statement(duplicated_ignore=duplicated_ignore)
            for record in records:
                now = datetime.now()

                insert_columns = self.columns(exclude=["id"])
                if isinstance(record, dict):
                    if 'deleted' in insert_columns:
                        record['deleted'] = 0
                    if 'create_by' in insert_columns:
                        record['create_by'] = WebContext().uid()
                    if 'create_at' in insert_columns:
                        record['create_at'] = now
                    args = [record.get(col, None) for col in insert_columns]
                else:
                    if isinstance(record, BaseEntity):
                        record.deleted = 0
                        record.create_by = WebContext().uid()
                        record.create_at = now
                    else:
                        if 'deleted' in insert_columns:
                            record.deleted = 0
                        if 'create_by' in insert_columns:
                            record.create_by = WebContext().uid()
                        if 'create_at' in insert_columns:
                            record.create_at = now
                    args = [getattr(record, col) for col in insert_columns]

                logger.info(f'#### args: {args}')
                c.execute(sql, tuple(args))
            self._conn.commit()
        except Exception as e:
            logger.error(f'数据库操作异常: {e}')
            logger.error(traceback.format_exc())
        finally:
            c.close()
            if reuse_conn is False:
                self._conn.close()

    def update(self, reuse_conn: bool = False):
        c = self._get_cursor()
        try:

            sql, args = self.update_statement()
            c.execute(sql, args)
            self._conn.commit()
        except Exception as e:
            logger.error(f'数据库操作异常: {e}')
            logger.error(traceback.format_exc())
        finally:
            c.close()
            if reuse_conn is False:
                self._conn.close()

    def delete(self, reuse_conn: bool = False):
        c = self._get_cursor()
        try:
            sql, args = self.delete_statement()
            c.execute(sql, args)
            self._conn.commit()
        except Exception as e:
            logger.error(f'数据库操作异常: {e}')
            logger.error(traceback.format_exc())
        finally:
            c.close()
            if reuse_conn is False:
                self._conn.close()

    def update_by_pk(self, record, reuse_conn: bool = False):
        c = self._get_cursor()
        try:
            sql, args = self.update_by_pk_statement(record)
            c.execute(sql, args)
            self._conn.commit()
        except Exception as e:
            logger.error(f'数据库操作异常: {e}')
            logger.error(traceback.format_exc())
        finally:
            c.close()
            if reuse_conn is False:
                self._conn.close()
