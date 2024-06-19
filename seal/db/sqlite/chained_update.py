import traceback

from .meta import Meta
from .sqlite_connector import SqliteConnector
from ..base_chained_update import BaseChainedUpdate
from ...context import WebContext
from ...model import BaseEntity
from datetime import datetime
from loguru import logger


class ChainedUpdate(BaseChainedUpdate):

    def meta(self):
        return Meta

    def __init__(self, clz=None, table: str = None, logic_delete_col: str = None):
        super().__init__(clz, '?', table, logic_delete_col)
        self.__conn = SqliteConnector().get_connection()

    def __get_cursor(self):
        return self.__conn.cursor()

    def logic_delete(self):
        self.sets['deleted'] = 1
        self.update()

    def insert(self, entity: BaseEntity = None, data: dict = None, duplicated_ignore=False, reuse_conn: bool = False):
        c = self.__get_cursor()
        try:
            if entity is not None:
                sql, args = self.insert_statement(entity=entity, duplicated_ignore=duplicated_ignore)
            elif data is not None:
                sql, args = self.insert_statement(data=data, duplicated_ignore=duplicated_ignore)
            else:
                raise ValueError('null data')
            c.execute(sql, args)
            self.__conn.commit()
        except Exception as e:
            logger.error(f'数据库操作异常: {e}')
            logger.error(traceback.format_exc())
        finally:
            c.close()
            if reuse_conn is False:
                self.__conn.close()

    def insert_bulk(self, entity_list: list[BaseEntity] = None, data_list: list[dict] = None,
                    duplicated_ignore: bool = False, reuse_conn: bool = False):
        if entity_list is None and data_list is None:
            raise ValueError('null data')

        c = self.__get_cursor()
        try:
            sql = self.insert_bulk_statement(duplicated_ignore=duplicated_ignore)
            if entity_list is not None:
                for entity in entity_list:
                    now = datetime.now()
                    entity.deleted = 0
                    entity.create_by = WebContext().uid()
                    entity.create_at = now
                    args = [getattr(entity, col) for col in self.columns(exclude=["id"])]
                    logger.info(f'#### args: {args}')
                    c.execute(sql, tuple(args))
            elif data_list is not None:
                for data in data_list:
                    now = datetime.now()
                    data['deleted'] = 0
                    data['create_by'] = WebContext().uid()
                    data['create_at'] = now
                    args = [data[col] for col in self.clz.columns(exclude=["id"])]
                    logger.info(f'#### args: {args}')
                    c.execute(sql, tuple(args))
            self.__conn.commit()
        except Exception as e:
            logger.error(f'数据库操作异常: {e}')
            logger.error(traceback.format_exc())
        finally:
            c.close()
            if reuse_conn is False:
                self.__conn.close()

    def update(self, reuse_conn: bool = False):
        c = self.__get_cursor()
        try:

            sql, args = self.update_statement()
            c.execute(sql, args)
            self.__conn.commit()
        except Exception as e:
            logger.error(f'数据库操作异常: {e}')
            logger.error(traceback.format_exc())
        finally:
            c.close()
            if reuse_conn is False:
                self.__conn.close()

    def delete(self, reuse_conn: bool = False):
        c = self.__get_cursor()
        try:
            sql, args = self.delete_statement()
            c.execute(sql, args)
            self.__conn.commit()
        except Exception as e:
            logger.error(f'数据库操作异常: {e}')
            logger.error(traceback.format_exc())
        finally:
            c.close()
            if reuse_conn is False:
                self.__conn.close()

    def update_by_pk(self, entity: BaseEntity = None, data: dict = None, reuse_conn: bool = False):
        c = self.__get_cursor()
        try:
            if entity is not None:
                sql, args = self.update_by_pk_statement(entity=entity)
            elif data is not None:
                sql, args = self.update_by_pk_statement(data=data)
            else:
                raise ValueError('null data')
            c.execute(sql, args)
            self.__conn.commit()
        except Exception as e:
            logger.error(f'数据库操作异常: {e}')
            logger.error(traceback.format_exc())
        finally:
            c.close()
            if reuse_conn is False:
                self.__conn.close()
