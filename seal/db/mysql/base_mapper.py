import abc
from datetime import datetime
from builtins import list as _list
from ...context import WebContext
from ...model import BaseEntity, PageResult
from .mysql_connector import MysqlConnector


class BaseMapper(metaclass=abc.ABCMeta):

    def __init__(self, clz, table: str = None):
        self.clz = clz
        self.table = table if table is not None else self.clz.table_name()
        self.placeholder = '%s'

    def all(self):
        """
        Get all entities
        :return: list of entities
        """
        conn = MysqlConnector().get_connection()
        try:
            c = conn.cursor()
            cols = ', '.join(self.clz.columns())
            sql = f'SELECT {cols} FROM {self.table} WHERE deleted = 0'
            print(f'#### sql: {sql}')
            affected = c.execute(sql, ())
            print(f'#### affected rows: {affected}')
            rows = c.fetchall()
            entities = []
            for row in rows:
                entities.append(self.clz(**{col: row[i] for i, col in enumerate(self.clz.columns())}))
            return entities
        finally:
            conn.close()

    def first(self, **conditions):
        """
        Find entity by multiple columns
        Example: find([('name', 'test'), ('status', 1)])
                 find([('name', '%test%', 'like'), ('status', 1)])
        :param conditions: list of tuple of column and value
        :return: entity
        """
        conditions['deleted'] = 0
        cols = ', '.join(self.clz.columns())
        sql = f'SELECT {cols} FROM {self.table} where '
        sql += ' and '.join([f'{col} {"=" if len(val) == 1 else val[1]} {self.placeholder}' for col, *val in
                             conditions.items()])
        print(f'#### sql: {sql}')
        print(f'#### args: {tuple([val[0] for _, *val in conditions])}')

        conn = MysqlConnector().get_connection()
        try:
            c = conn.cursor()
            affected = c.execute(sql, tuple([val[0] for _, *val in conditions]))
            print(f'#### affected rows: {affected}')
            row = c.fetchone()
            if row is None:
                return None
            return self.clz(**{col: row[i] for i, col in enumerate(self.clz.columns())})
        finally:
            conn.close()

    def list(self, **conditions):
        """
        List entities by multiple columns
        Example: list([('name', 'test'), ('status', 1)])
                 list([('name', '%test%', 'like'), ('status', 1)])
        :param conditions: list of tuple of column and value
        :return: list of entities
        """
        conditions['deleted'] = 0
        cols = ', '.join(self.clz.columns())
        sql = f'SELECT {cols} FROM {self.table} where '
        sql += ' and '.join([f'{col} {"=" if len(val) == 1 else val[1]} {self.placeholder}' for col, *val in
                             conditions.items()])
        print(f'#### sql: {sql}')
        print(f'#### args: {tuple([val[0] for _, *val in conditions])}')

        conn = MysqlConnector().get_connection()
        try:
            c = conn.cursor()
            affected = c.execute(sql, tuple([val[0] for _, *val in conditions]))
            print(f'#### affected rows: {affected}')
            rows = c.fetchall()
            entities = []
            for row in rows:
                entities.append(self.clz(**{col: row[i] for i, col in enumerate(self.clz.columns())}))
            return entities
        finally:
            conn.close()

    def page(self, page: int, page_size: int, **conditions) -> PageResult:
        """
        Page entities by multiple columns
        Example: page(1, 10, ('name', 'test'), ('status', 1))
                 page(1, 10, ('name', '%test%', 'like'), ('status', 1))
        :param page: page number
        :param page_size: page size
        :param conditions: list of tuple of column and value
        :return: page result
        """
        conditions['deleted'] = 0
        cols = ', '.join(self.clz.columns())
        sql = f'SELECT {cols} FROM {self.table} where '
        sql += ' and '.join([f'{col} {"=" if len(val) == 1 else val[1]} {self.placeholder}' for col, *val in
                             conditions.items()])
        sql += f' limit {page_size} offset {(page - 1) * page_size}'
        print(f'#### sql: {sql}')
        print(f'#### args: {tuple([val[0] for _, *val in conditions])}')

        conn = MysqlConnector().get_connection()
        try:
            c = conn.cursor()
            affected = c.execute(sql, tuple([val[0] for _, *val in conditions]))
            print(f'#### affected rows: {affected}')
            rows = c.fetchall()
            entities = []
            for row in rows:
                entities.append(self.clz(**{col: row[i] for i, col in enumerate(self.clz.columns())}))
            return PageResult(page, page_size, self.count(**conditions), entities)
        finally:
            conn.close()

    def count(self, **conditions) -> int:
        """
        Count entities by multiple columns
        Example: count(('name', 'test'), ('status', 1))
                 count(('name', '%test%', 'like'), ('status', 1))
        :param conditions: list of tuple of column and value
        :return: count of entities
        """
        conditions['deleted'] = 0
        sql = f'SELECT count(*) FROM {self.table} where '
        sql += ' and '.join([f'{col} {"=" if len(val) == 1 else val[1]} {self.placeholder}' for col, *val in
                             conditions.items()])
        print(f'#### sql: {sql}')
        print(f'#### args: {tuple([val[0] for _, *val in conditions])}')

        conn = MysqlConnector().get_connection()
        try:
            c = conn.cursor()
            affected = c.execute(sql, tuple([val[0] for _, *val in conditions]))
            print(f'#### affected rows: {affected}')
            return c.fetchone()[0]
        finally:
            conn.close()

    def insert(self, entity: BaseEntity):
        """
        Insert entity
        :param entity: entity object
        :return: no return
        """
        now = datetime.now()
        entity.deleted = 0
        entity.create_by = WebContext().uid()
        entity.create_at = now

        conn = MysqlConnector().get_connection()
        try:
            c = conn.cursor()
            cols = ', '.join(self.clz.columns(exclude=["id"]))
            placeholders = ', '.join([self.placeholder for _ in self.clz.columns(exclude=["id"])])
            sql = f'INSERT INTO {self.table} ({cols}) VALUES ({placeholders})'
            print(f'#### sql: {sql}')
            affected = c.execute(sql, tuple([getattr(entity, col) for col in self.clz.columns(exclude=["id"])]))
            print(f'#### affected rows: {affected}')
            conn.commit()
        finally:
            conn.close()

    def update_by_id(self, entity: BaseEntity):
        """
        Update entity by id
        :param entity: entity object
        :return: no return
        """
        entity.gmt_modified = datetime.now()
        entity.update_by = WebContext().uid()

        conn = MysqlConnector().get_connection()
        try:
            c = conn.cursor()
            cols = [f'{col} = {self.placeholder}' for col in self.clz.columns(exclude=["id"])]
            sql = f'UPDATE {self.table} SET {", ".join(cols)} WHERE id = {self.placeholder}'
            print(f'#### sql: {sql}')
            affected = c.execute(sql,
                                 tuple([getattr(entity, col) for col in self.clz.columns(exclude=["id"])] + [
                                     entity.id]))
            print(f'#### affected rows: {affected}')
            conn.commit()
        finally:
            conn.close()

    def update(self, sets: _list[tuple], conditions: _list[tuple]):
        """
        Update entity by multiple columns
        Example: update([('name', 'test'), ('status', 1)], [('id', 1)])
                 update([('name', 'test'), ('status', 1)], [('name', '%test%', 'like'), ('status', 0)])
        :param sets: list of tuple of column and value to be updated
        :param conditions: list of tuple of column and value for condition
        :return: no return
        """
        sets += [('update_at', datetime.now()), ('update_by', WebContext().uid())]
        conditions += [('deleted', 0)]
        sql = f'UPDATE {self.table} SET '
        sql += ', '.join([f'{col} = {self.placeholder}' for col, val in sets])
        sql += ' WHERE '
        sql += ' and '.join([f'{col} {"=" if len(val) == 1 else val[1]} {self.placeholder}' for col, *val in
                             conditions])
        print(f'#### sql: {sql}')
        print(f'#### args: {tuple([val[0] for _, *val in sets] + [val[0] for _, *val in conditions])}')

        conn = MysqlConnector().get_connection()
        try:
            c = conn.cursor()
            affected = c.execute(sql, tuple([val[0] for _, *val in sets] + [val[0] for _, *val in conditions]))
            print(f'#### affected rows: {affected}')
            conn.commit()
        finally:
            conn.close()

    def delete(self, **conditions):
        """
        Delete entity by multiple columns
        Example: delete(name='test', status=1)
                 delete(name=('%test%', 'like'), status=1)
        :param conditions: list of tuple of column and value
        :return: no return
        """
        conditions['deleted'] = 0
        sql = f'DELETE FROM {self.table} where '
        sql += ' and '.join([f'{col} {"=" if len(val) == 1 else val[1]} {self.placeholder}' for col, val in
                             conditions.items()])
        print(f'#### sql: {sql}')
        print(f'#### args: {tuple([val[0] for _, *val in conditions])}')

        conn = MysqlConnector().get_connection()
        try:
            c = conn.cursor()
            affected = c.execute(sql, tuple([val[0] for _, *val in conditions]))
            print(f'#### affected rows: {affected}')
            conn.commit()
        finally:
            conn.close()
