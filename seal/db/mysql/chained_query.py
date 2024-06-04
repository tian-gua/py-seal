from datetime import datetime
from ...context import WebContext
from .mysql_connector import MysqlConnector
from ...model import PageResult, BaseEntity


class ChainedQuery:
    def __init__(self, clz, table: str = None, logic_delete_col: str = None):
        self.clz = clz
        self.table = table if table is not None else self.clz.table_name()
        self.__conn = MysqlConnector().get_connection()
        self.__conditions = [(logic_delete_col if logic_delete_col is not None else 'deleted', 0, '=')]
        self.__select_cols = ()
        self.__sets = {}
        self.__raw = ""
        self.placeholder = '%s'

    def __get_cursor(self):
        return self.__conn.cursor()

    def __cols(self):
        if len(self.__select_cols) == 0:
            return ', '.join(self.clz.columns())
        return ', '.join(self.__select_cols)

    def __where(self):
        if len(self.__conditions) == 0:
            return ''
        return 'where ' + ' and '.join([f'{cond[0]} {cond[2]} {self.placeholder}' for cond in self.__conditions])

    def __args(self):
        if len(self.__conditions) == 0:
            return ()
        return tuple([cond[1] for cond in self.__conditions])

    def raw(self, raw_sql: str):
        self.__raw = raw_sql
        return self

    def select(self, *cols):
        self.__select_cols = cols
        return self

    def set(self, **sets):
        self.__sets = sets
        return self

    def eq(self, col, value):
        self.__conditions.append((col, value, '='))
        return self

    def ne(self, col, value):
        self.__conditions.append((col, value, '!='))
        return self

    def gt(self, col, value):
        self.__conditions.append((col, value, '>'))
        return self

    def ge(self, col, value):
        self.__conditions.append((col, value, '>='))
        return self

    def lt(self, col, value):
        self.__conditions.append((col, value, '<'))
        return self

    def le(self, col, value):
        self.__conditions.append((col, value, '<='))
        return self

    def in_(self, col, value):
        self.__conditions.append((col, value, 'in'))
        return self

    def like(self, col, value):
        self.__conditions.append((col, value, 'like'))
        return self

    def count(self):
        c = self.__get_cursor()
        try:
            sql = f'SELECT count(1) FROM {self.table} {self.__where()}'
            print(f'#### sql: {sql}')
            print(f'#### args: {self.__args()}')
            affected = c.execute(sql, self.__args())
            print(f'#### affected rows: {affected}')
            return c.fetchone()[0]
        finally:
            c.close()
            self.__conn.close()

    def list(self):
        c = self.__get_cursor()
        try:
            sql = f'SELECT {self.__cols()} FROM {self.table} {self.__where()}'
            print(f'#### sql: {sql}')
            print(f'#### args: {self.__args()}')
            affected = c.execute(sql, self.__args())
            print(f'#### affected rows: {affected}')
            rows = c.fetchall()
            entities = []
            for row in rows:
                entities.append(self.clz(**{col: row[i] for i, col in enumerate(
                    self.clz.columns() if len(self.__select_cols) == 0 else self.__select_cols)}))
            return entities
        finally:
            self.__conn.close()

    def page(self, page: int = 1, page_size: int = 10):
        c = self.__get_cursor()
        try:
            limit = f'limit {page_size} offset {(page - 1) * page_size}'
            sql = f'SELECT {self.__cols()} FROM {self.table} {self.__where()} {limit}'
            print(f'#### sql: {sql}')
            print(f'#### args: {self.__args()}')
            affected = c.execute(sql, self.__args())
            print(f'#### affected rows: {affected}')
            rows = c.fetchall()
            entities = []
            for row in rows:
                entities.append(self.clz(**{col: row[i] for i, col in enumerate(
                    self.clz.columns() if len(self.__select_cols) == 0 else self.__select_cols)}))
            page_result = PageResult(page, page_size, self.count(), entities)
            return page_result
        finally:
            self.__conn.close()

    def first(self):
        c = self.__get_cursor()
        try:
            sql = f'SELECT {self.__cols()} FROM {self.table} {self.__where()}'
            print(f'#### sql: {sql}')
            print(f'#### args: {self.__args()}')
            affected = c.execute(sql, self.__args())
            print(f'#### affected rows: {affected}')
            row = c.fetchone()
            if row is None:
                return None
            return self.clz(**{col: row[col] for i, col in enumerate(self.clz.columns())})
        finally:
            self.__conn.close()

    def update(self):
        self.__sets['update_by'] = WebContext().uid()
        self.__sets['update_at'] = datetime.now()
        c = self.__get_cursor()
        try:
            if self.__sets is None or len(self.__sets.keys()) == 0:
                raise Exception('update set is required')
            if self.__conditions is None or self.__where() == '':
                raise Exception('update condition is required')

            sql = f'UPDATE {self.table} SET {", ".join([f"{col} = {self.placeholder}" for col in self.__sets.keys()])} {self.__where()}'
            print(f'#### sql: {sql}')
            print(f'#### args: {tuple(self.__sets.values()) + self.__args()}')
            affected = c.execute(sql, tuple(self.__sets.values()) + self.__args())
            print(f'#### affected rows: {affected}')
            self.__conn.commit()
        finally:
            self.__conn.close()

    def update_entity(self, entity: BaseEntity):
        entity.update_by = WebContext().uid()
        entity.update_at = datetime.now()
        c = self.__get_cursor()
        try:

            update_cols = [f'{col} = {self.placeholder}' for col in self.clz.columns(exclude=["id"])]
            sql = f'UPDATE {self.table} SET {", ".join(update_cols)} where id = {self.placeholder}'
            print(f'#### sql: {sql}')
            args = tuple([getattr(entity, col) for col in self.clz.columns(exclude=["id"])] + [entity.id])
            print(f'#### args: {args}')
            affected = c.execute(sql, args)
            print(f'#### affected rows: {affected}')
            self.__conn.commit()
        finally:
            self.__conn.close()

    def delete(self):
        c = self.__get_cursor()
        try:
            if self.__conditions is None or self.__where() == '':
                raise Exception('delete condition is required')

            sql = f'DELETE FROM {self.table} {self.__where()}'
            print(f'#### sql: {sql}')
            print(f'#### args: {self.__args()}')
            affected = c.execute(sql, self.__args())
            print(f'#### affected rows: {affected}')
            self.__conn.commit()
        finally:
            self.__conn.close()

    def logic_delete(self):
        self.__sets['deleted'] = 1
        self.update()

    def mapping(self):
        c = self.__get_cursor()
        try:
            sql = f'{self.__raw} {self.__where()}'
            print(f'#### sql: {sql}')
            print(f'#### args: {self.__args()}')
            affected = c.execute(sql, self.__args())
            print(f'#### affected rows: {affected}')
            rows = c.fetchall()
            entities = []
            for row in rows:
                entities.append(self.clz(**{col: row[i] for i, col in enumerate(self.clz.columns())}))
            return entities
        finally:
            self.__conn.close()
