import abc
from loguru import logger


class BaseChainedQuery(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def meta(self):
        ...

    def __init__(self, clz=None, placeholder=None, table: str = None, logic_delete_col: str = None):
        self.clz = clz
        self.table = table if table is not None else self.clz.table_name()
        self._where: list[tuple] = [(logic_delete_col if logic_delete_col is not None else 'deleted', 0, '=')]
        self._columns = ()
        self._sorts = ()
        self._limit = None
        self._raw = None
        self._ignore_columns = ()
        self.placeholder = placeholder

        if clz is None:
            self.is_dynamic = True
            self.table_info = self.meta().get_table_info(self.table)
        else:
            if clz.dynamic:
                self.is_dynamic = True
                self.table_info = self.meta().get_table_info(self.table)
            else:
                self.is_dynamic = False

    def columns(self):
        if len(self._columns) != 0:
            return self._columns
        if self.is_dynamic:
            return [key for key, db_type in self.table_info.columns if key not in self._ignore_columns]
        return [col for col in self.clz.columns() if col not in self._ignore_columns]

    def build_select(self):
        return ', '.join(self.columns())

    def build_args(self):
        if len(self._where) == 0:
            return ()
        return tuple([cond[1] for cond in self._where])

    def build_where(self):
        if len(self._where) == 0:
            return ''
        return 'where ' + ' and '.join([f'{cond[0]} {cond[2]} {self.placeholder}' for cond in self._where])

    def build_order_by(self):
        order_by = ', '.join(f'{sort[0]} {sort[1]}' for sort in self._sorts)
        order_by = '' if len(self._sorts) == 0 else f'order by {order_by}'
        return order_by

    def raw(self, raw_sql: str):
        self._raw = raw_sql
        return self

    def select(self, *cols):
        self._columns = cols
        return self

    def ignore(self, *cols):
        self._ignore_columns = cols
        return self

    def eq(self, col, value):
        self._where.append((col, value, '='))
        return self

    def ne(self, col, value):
        self._where.append((col, value, '!='))
        return self

    def gt(self, col, value):
        self._where.append((col, value, '>'))
        return self

    def ge(self, col, value):
        self._where.append((col, value, '>='))
        return self

    def lt(self, col, value):
        self._where.append((col, value, '<'))
        return self

    def le(self, col, value):
        self._where.append((col, value, '<='))
        return self

    def in_(self, col, value):
        self._where.append((col, value, 'in'))
        return self

    def l_like(self, col, value):
        self._where.append((col, f'%{value}', 'like'))
        return self

    def r_like(self, col, value):
        self._where.append((col, f'{value}%', 'like'))
        return self

    def like(self, col, value):
        self._where.append((col, f'%{value}%', 'like'))
        return self

    def limit(self, limit):
        self._limit = limit
        return self

    def sort(self, *sorts):
        self._sorts = sorts
        return self

    def count_statement(self) -> tuple[str, tuple]:
        sql = f'SELECT count(1) FROM {self.table} {self.build_where()}'
        args = self.build_args()
        logger.info(f'#### sql: {sql}')
        logger.info(f'#### args: {args}')
        return sql, args

    def select_statement(self) -> tuple[str, tuple]:
        limit = '' if self._limit is None else f'limit {self._limit}'
        sql = f'SELECT {self.build_select()} FROM {self.table} {self.build_where()} {self.build_order_by()} {limit}'
        args = self.build_args()
        logger.info(f'#### sql: {sql}')
        logger.info(f'#### args: {args}')
        return sql, args

    def page_statement(self, page: int, page_size: int) -> tuple[str, tuple]:
        limit = f'limit {page_size} offset {(page - 1) * page_size}'
        sql = f'SELECT {self.build_select()} FROM {self.table} {self.build_where()} {self.build_order_by()} {limit}'
        args = self.build_args()
        logger.info(f'#### sql: {sql}')
        logger.info(f'#### args: {args}')
        return sql, args

    def mapping_statement(self) -> tuple[str, tuple]:
        sql = f'{self._raw} {self.build_where()}'
        args = self.build_args()
        logger.info(f'#### sql: {sql}')
        logger.info(f'#### args: {args}')
        return sql, args

    def fetchall(self, cursor) -> list:
        rows = cursor.fetchall()
        entities = []
        for row in rows:
            if self.is_dynamic:
                if type(row) is dict:
                    entities.append(row)
                else:
                    entities.append({field: row[i] for i, field in enumerate(self.columns())})
            else:
                if type(row) is dict:
                    entities.append(self.clz(**row))
                else:
                    entities.append(self.clz(**{col: row[i] for i, col in enumerate(self.columns())}))
        return entities

    def fetchone(self, cursor):
        row = cursor.fetchone()
        if row is None:
            return None
        if self.is_dynamic:
            if type(row) is dict:
                return row
            else:
                return {field: row[i] for i, field in enumerate(self.columns())}
        else:
            if type(row) is dict:
                return self.clz(**row)
            else:
                return self.clz(**{col: row[i] for i, col in enumerate(self.columns())})
