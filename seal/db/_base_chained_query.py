import abc
from collections import namedtuple
from loguru import logger

from .table_info import TableInfo
from ..model import dynamic_models, BaseEntity


class BaseChainedQuery(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def meta(self):
        ...

    def __init__(self, target, logic_delete_col: str = None, placeholder=None):
        if type(target) is not str:
            self.clz = target
            self.table = target.table_name()
        else:
            self.table = target
            if not dynamic_models.has(target):
                table_info: TableInfo = self.meta().get_table_info(self.table)
                dynamic_model = namedtuple(target,
                                           [col[0] for col in table_info.columns],
                                           defaults=(None,) * len(table_info.columns))
                dynamic_models.register(target, dynamic_model)
                self.clz = dynamic_model
            else:
                self.clz = dynamic_models.get(target)

        self._where: list[tuple] = [(logic_delete_col if logic_delete_col is not None else 'deleted', 0, '=')]
        self._columns = ()
        self._sorts = ()
        self._limit: int | None = None
        self._group_by = ()
        self._raw = None
        self._ignore_columns = ()
        self.placeholder = placeholder

    def columns(self):
        if len(self._columns) != 0:
            return self._columns
        if issubclass(self.clz, BaseEntity):
            return [col for col in self.clz.columns() if col not in self._ignore_columns]
        return [key for key in self.clz._fields if key not in self._ignore_columns]

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

    def build_group_by(self):
        group_by = ', '.join(self._group_by)
        return '' if len(self._group_by) == 0 else f'group by {group_by}'

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

    def limit(self, limit: int):
        self._limit = limit
        return self

    def sort(self, col: str, order='asc'):
        self._sorts = [(col, order)]
        return self

    def sorts(self, *sorts):
        self._sorts = sorts
        return self

    def group_by(self, col: str):
        self._group_by = col
        return self

    def count_statement(self) -> tuple[str, tuple]:
        sql = f'SELECT count(1) FROM {self.table} {self.build_where()} {self.build_group_by()}'
        args = self.build_args()
        logger.info(f'#### sql: {sql}')
        logger.info(f'#### args: {args}')
        return sql, args

    def select_statement(self) -> tuple[str, tuple]:
        limit = '' if self._limit is None else f'limit {self._limit}'
        sql = f'SELECT {self.build_select()} FROM {self.table} {self.build_where()} {self.build_group_by()} {self.build_order_by()} {limit}'
        args = self.build_args()
        logger.info(f'#### sql: {sql}')
        logger.info(f'#### args: {args}')
        return sql, args

    def page_statement(self, page: int, page_size: int) -> tuple[str, tuple]:
        limit = f'limit {page_size} offset {(page - 1) * page_size}'
        sql = f'SELECT {self.build_select()} FROM {self.table} {self.build_where()} {self.build_group_by()} {self.build_order_by()} {limit}'
        args = self.build_args()
        logger.info(f'#### sql: {sql}')
        logger.info(f'#### args: {args}')
        return sql, args

    def mapping_statement(self) -> tuple[str, tuple]:
        sql = f'{self._raw} {self.build_where()} {self.build_group_by()} {self.build_order_by()}'
        args = self.build_args()
        logger.info(f'#### sql: {sql}')
        logger.info(f'#### args: {args}')
        return sql, args

    def fetchall(self, cursor, to_dict: bool = False) -> list:
        rows = cursor.fetchall()
        entities = []
        for row in rows:
            if type(row) is dict:
                if to_dict:
                    entities.append(row)
                else:
                    entities.append(self.clz(**row))
            else:
                if to_dict:
                    entities.append({col: row[i] for i, col in enumerate(self.columns())})
                else:
                    entities.append(self.clz(**{col: row[i] for i, col in enumerate(self.columns())}))
        return entities

    def fetchone(self, cursor, to_dict: bool = False):
        row = cursor.fetchone()
        if row is None:
            return None
        if type(row) is dict:
            if to_dict:
                return row
            return self.clz(**row)
        else:
            if to_dict:
                return {col: row[i] for i, col in enumerate(self.columns())}
            return self.clz(**{col: row[i] for i, col in enumerate(self.columns())})
