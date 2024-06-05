import abc


class BaseChainedQuery(metaclass=abc.ABCMeta):

    def __init__(self, clz, placeholder, table: str = None, logic_delete_col: str = None):
        self.clz = clz
        self.table = table if table is not None else self.clz.table_name()
        self.__conditions: list[tuple] = [(logic_delete_col if logic_delete_col is not None else 'deleted', 0, '=')]
        self.__select_cols = ()
        self.__order_by = ()
        self.__raw = ""
        self.placeholder = placeholder

    def __args(self):
        if len(self.__conditions) == 0:
            return ()
        return tuple([cond[1] for cond in self.__conditions])

    def __cols(self):
        if len(self.__select_cols) == 0:
            return ', '.join(self.clz.columns())
        return ', '.join(self.__select_cols)

    def __where(self):
        if len(self.__conditions) == 0:
            return ''
        return 'where ' + ' and '.join([f'{cond[0]} {cond[2]} {self.placeholder}' for cond in self.__conditions])

    def raw(self, raw_sql: str):
        self.__raw = raw_sql
        return self

    def select(self, *cols):
        self.__select_cols = cols
        return self

    def ignore(self, *cols):
        self.__select_cols = tuple(set(self.clz.columns()) - set(cols))
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

    def l_like(self, col, value):
        self.__conditions.append((col, f'%{value}', 'like'))
        return self

    def r_like(self, col, value):
        self.__conditions.append((col, f'{value}%', 'like'))
        return self

    def like(self, col, value):
        self.__conditions.append((col, f'%{value}%', 'like'))
        return self

    def order_by(self, *sorts):
        self.__order_by = sorts
        return self

    def count_statement(self) -> tuple[str, tuple]:
        sql = f'SELECT count(1) FROM {self.table} {self.__where()}'
        args = self.__args()
        print(f'#### sql: {sql}')
        print(f'#### args: {args}')
        return sql, args

    def select_statement(self) -> tuple[str, tuple]:
        order_by = ', '.join(f'{sort[0]} {sort[1]}' for sort in self.__order_by)
        order_by = '' if len(self.__order_by) == 0 else f'order by {order_by}'
        sql = f'SELECT {self.__cols()} FROM {self.table} {self.__where()} {order_by}'
        args = self.__args()
        print(f'#### sql: {sql}')
        print(f'#### args: {args}')
        return sql, args

    def page_statement(self, page: int, page_size: int) -> tuple[str, tuple]:
        limit = f'limit {page_size} offset {(page - 1) * page_size}'
        order_by = ', '.join(f'{sort[0]} {sort[1]}' for sort in self.__order_by)
        order_by = '' if len(self.__order_by) == 0 else f'order by {order_by}'
        sql = f'SELECT {self.__cols()} FROM {self.table} {self.__where()} {order_by} {limit}'
        args = self.__args()
        print(f'#### sql: {sql}')
        print(f'#### args: {args}')
        return sql, args

    def mapping_statement(self) -> tuple[str, tuple]:
        sql = f'{self.__raw} {self.__where()}'
        args = self.__args()
        print(f'#### sql: {sql}')
        print(f'#### args: {args}')
        return sql, args

    def fetchall(self, cursor) -> list:
        rows = cursor.fetchall()
        entities = []
        for row in rows:
            entities.append(self.clz(**{col: row[i] for i, col in enumerate(
                self.clz.columns() if len(self.__select_cols) == 0 else self.__select_cols)}))
        return entities
