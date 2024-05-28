from datetime import datetime
from seal.context.context import PageContext, WebContext
from seal.config import config
from seal.db.mysql.mysql_connector import MysqlConnector


class ChainedQuery:
    def __init__(self, entity_clz, logic_delete_col: str = None):
        self.clz = entity_clz
        self.__conditions = [(logic_delete_col if logic_delete_col is not None else 'deleted', 0, '=')]
        self.__select_cols = ()
        self.__sets = {}
        self.__page = ()
        self.__conn = MysqlConnector().get_connection()
        self.__raw = ""
        self.placeholder = '?' if config['data_source'] == 'sqlite' else '%s'

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

    def __limit(self):
        if len(self.__page) == 0:
            return ''
        return f'limit {self.__page[1]} offset {(self.__page[0] - 1) * self.__page[1]}'

    def raw(self, raw_sql: str):
        self.__raw = raw_sql
        return self

    def select(self, *cols):
        self.__select_cols = cols
        return self

    def set(self, **sets):
        self.__sets = sets
        return self

    def page(self, page, page_size):
        self.__page = (page, page_size)
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
        c = self.__conn.cursor()
        try:
            sql = f'SELECT count(1) FROM {self.clz.table_name()} {self.__where()}'
            print(f'#### sql: {sql}')
            print(f'#### args: {self.__args()}')
            affected = c.execute(sql, self.__args())
            print(f'#### affected rows: {affected}')
            return c.fetchone()[0]
        finally:
            c.close()
            self.__conn.close()

    def list(self):
        c = self.__conn.cursor()
        try:
            sql = f'SELECT {self.__cols()} FROM {self.clz.table_name()} {self.__where()} {self.__limit()}'
            print(f'#### sql: {sql}')
            print(f'#### args: {self.__args()}')
            if len(self.__page) > 0:
                print(f'#### page: {self.__page[0]}, {self.__page[1]}')
                PageContext().set(self.__page[0], self.__page[1], self.count())
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

    def first(self):
        c = self.__conn.cursor()
        try:
            sql = f'SELECT {self.__cols()} FROM {self.clz.table_name()} {self.__where()}'
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
        self.__sets['gmt_modified'] = datetime.now()
        self.__sets['update_by'] = WebContext().uid()
        c = self.__conn.cursor()
        try:
            if self.__sets is None or len(self.__sets.keys()) == 0:
                raise Exception('update set is required')
            if self.__conditions is None or self.__where() == '':
                raise Exception('update condition is required')

            sql = f'UPDATE {self.clz.table_name()} SET {", ".join([f"{col} = {self.placeholder}" for col in self.__sets.keys()])} {self.__where()}'
            print(f'#### sql: {sql}')
            print(f'#### args: {tuple(self.__sets.values()) + self.__args()}')
            affected = c.execute(sql, tuple(self.__sets.values()) + self.__args())
            print(f'#### affected rows: {affected}')
            self.__conn.commit()
        finally:
            self.__conn.close()

    def delete(self):
        c = self.__conn.cursor()
        try:
            if self.__conditions is None or self.__where() == '':
                raise Exception('delete condition is required')

            sql = f'DELETE FROM {self.clz.table_name()} {self.__where()}'
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
        c = self.__conn.cursor()
        try:
            sql = f'{self.__raw} {self.__where()} {self.__limit()}'
            print(f'#### sql: {sql}')
            print(f'#### args: {self.__args()}')
            if len(self.__page) > 0:
                print(f'#### page: {self.__page[0]}, {self.__page[1]}')
            affected = c.execute(sql, self.__args())
            print(f'#### affected rows: {affected}')
            rows = c.fetchall()
            entities = []
            for row in rows:
                entities.append(self.clz(**{col: row[i] for i, col in enumerate(self.clz.columns())}))
            return entities
        finally:
            self.__conn.close()
