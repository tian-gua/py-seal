from .meta import Meta
from ..base_chained_update import BaseChainedUpdate
from .mysql_connector import MysqlConnector
from ...context import WebContext
from ...model import BaseEntity
from ...utils.tranform_utils import transform_nan
from datetime import datetime


class ChainedUpdate(BaseChainedUpdate):

    def meta(self):
        return Meta

    def __init__(self, clz=None, table: str = None, logic_delete_col: str = None):
        super().__init__(clz, '%s', table, logic_delete_col)
        self.__conn = MysqlConnector().get_connection()

    def __get_cursor(self):
        return self.__conn.cursor()

    def logic_delete(self):
        self.sets['deleted'] = 1
        self.update()

    def insert(self, entity: BaseEntity = None, data: dict = None, duplicated_key_update=False,
               reuse_conn: bool = False):
        c = self.__get_cursor()
        try:
            if entity is not None:
                sql, args = self.insert_statement(entity=entity, duplicated_key_update=duplicated_key_update)
            elif data is not None:
                sql, args = self.insert_statement(data=data, duplicated_key_update=duplicated_key_update)
            else:
                raise ValueError('null data')
            affected = c.execute(sql, args)
            print(f'#### affected: {affected}')
            self.__conn.commit()
        except Exception as e:
            print(f'数据库操作异常: {e}')
        finally:
            c.close()
            if reuse_conn is False:
                self.__conn.close()

    def insert_bulk(self, entity_list: list[BaseEntity] = None, data_list: list[dict] = None,
                    duplicated_key_update: bool = False, reuse_conn: bool = False):
        if entity_list is None and data_list is None:
            raise ValueError('null data')

        c = self.__get_cursor()
        try:
            sql = self.insert_bulk_statement(duplicated_key_update=duplicated_key_update)
            total_affected = 0
            if entity_list is not None:
                for entity in entity_list:
                    now = datetime.now()
                    entity.deleted = 0
                    entity.create_by = WebContext().uid()
                    entity.create_at = now
                    args = [getattr(entity, col) for col in self.columns(exclude=["id"])]
                    if duplicated_key_update:
                        args += args
                    print(f'#### args: {args}')
                    affected = c.execute(sql, args)
                    total_affected += affected
            elif data_list is not None:
                for data in data_list:
                    now = datetime.now()
                    data['deleted'] = 0
                    data['create_by'] = WebContext().uid()
                    data['create_at'] = now
                    args = [transform_nan(data[col]) for col in self.columns(exclude=["id"])]
                    if duplicated_key_update:
                        args += args
                    print(f'#### args: {args}')
                    affected = c.execute(sql, args)
                    total_affected += affected
            print(f'#### affected: {total_affected}')
            self.__conn.commit()
        except Exception as e:
            print(f'数据库操作异常: {e}')
        finally:
            c.close()
            if reuse_conn is False:
                self.__conn.close()

    def update(self, reuse_conn: bool = False):
        c = self.__get_cursor()
        try:
            sql, args = self.update_statement()
            affected = c.execute(sql, args)
            print(f'#### affected: {affected}')
            self.__conn.commit()
        except Exception as e:
            print(f'数据库操作异常: {e}')
        finally:
            c.close()
            if reuse_conn is False:
                self.__conn.close()

    def delete(self, reuse_conn: bool = False):
        c = self.__get_cursor()
        try:
            sql, args = self.delete_statement()
            affected = c.execute(sql, args)
            print(f'#### affected: {affected}')
            self.__conn.commit()
        except Exception as e:
            print(f'数据库操作异常: {e}')
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

            affected = c.execute(sql, args)
            print(f'#### affected: {affected}')
            self.__conn.commit()
        except Exception as e:
            print(f'数据库操作异常: {e}')
        finally:
            c.close()
            if reuse_conn is False:
                self.__conn.close()
