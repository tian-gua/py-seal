from ..base_chained_update import BaseChainedUpdate
from .mysql_connector import MysqlConnector
from ...context import WebContext
from ...model import BaseEntity
from datetime import datetime


class ChainedUpdate(BaseChainedUpdate):
    def __init__(self, clz, table: str = None, logic_delete_col: str = None):
        super().__init__(clz, '%s', table, logic_delete_col)
        self.__conn = MysqlConnector().get_connection()

    def __get_cursor(self):
        return self.__conn.cursor()

    def logic_delete(self):
        self.__sets['deleted'] = 1
        self.update()

    def insert(self, entity: BaseEntity, reuse_conn: bool = False):
        c = self.__get_cursor()
        try:
            sql, args = self.insert_entity_statement(entity)
            affected = c.execute(sql, args)
            print(f'#### affected: {affected}')
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

    def insert_bulk(self, entity_list: list[BaseEntity], duplicated_ignore: bool = False, reuse_conn: bool = False):
        c = self.__get_cursor()
        try:
            sql = self.insert_bulk_statement(duplicated_ignore)
            total_affected = 0
            for entity in entity_list:
                now = datetime.now()
                entity.deleted = 0
                entity.create_by = WebContext().uid()
                entity.create_at = now
                args = [getattr(entity, col) for col in self.clz.columns(exclude=["id"])]
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

    def update_entity(self, entity: BaseEntity, reuse_conn: bool = False):
        c = self.__get_cursor()
        try:
            sql, args = self.update_entity_statement(entity)
            affected = c.execute(sql, args)
            print(f'#### affected: {affected}')
            self.__conn.commit()
        except Exception as e:
            print(f'数据库操作异常: {e}')
        finally:
            c.close()
            if reuse_conn is False:
                self.__conn.close()
