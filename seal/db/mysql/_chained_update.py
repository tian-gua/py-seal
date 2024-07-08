import traceback

from ._connection_pool import ConnectionPool
from ._meta import Meta
from .._base_chained_update import BaseChainedUpdate
from ...context import WebContext
from ...model import BaseEntity
from datetime import datetime
from loguru import logger


class ChainedUpdate(BaseChainedUpdate):

    def meta(self):
        return Meta

    def __init__(self, target, logic_delete_col: str = None):
        super().__init__(target, logic_delete_col, '%s')
        # self._conn = MysqlConnector().get_connection()
        self._conn = ConnectionPool().get_connection()

    def _get_cursor(self):
        return self._conn.cursor()

    def logic_delete(self):
        self.sets['deleted'] = 1
        self.update()

    def insert(self, record, duplicated_key_update=False,
               reuse_conn: bool = False):
        c = self._get_cursor()
        try:
            sql, args = self.insert_statement(record, duplicated_key_update=duplicated_key_update)
            affected = c.execute(sql, args)
            logger.debug(f'#### affected: {affected}')
            self._conn.commit()
        except Exception as e:
            logger.error(f'数据库操作异常: {e}')
            logger.error(traceback.format_exc())
        finally:
            c.close()
            if reuse_conn is False:
                self._conn.close()

    def insert_bulk(self, records, duplicated_key_update: bool = False, reuse_conn: bool = False):
        if records is None:
            raise ValueError('null data')

        c = self._get_cursor()
        try:
            sql = self.insert_bulk_statement(duplicated_key_update=duplicated_key_update)
            total_affected = 0
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

                if duplicated_key_update:
                    args += args
                logger.debug(f'#### args: {args}')
                affected = c.execute(sql, args)
                total_affected += affected
            logger.debug(f'#### affected: {total_affected}')
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
            affected = c.execute(sql, args)
            logger.debug(f'#### affected: {affected}')
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
            affected = c.execute(sql, args)
            logger.debug(f'#### affected: {affected}')
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
            affected = c.execute(sql, args)
            logger.debug(f'#### affected: {affected}')
            self._conn.commit()
        except Exception as e:
            logger.error(f'数据库操作异常: {e}')
            logger.error(traceback.format_exc())
        finally:
            c.close()
            if reuse_conn is False:
                self._conn.close()
