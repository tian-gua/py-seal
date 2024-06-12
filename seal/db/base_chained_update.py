import abc
from datetime import datetime
from ..model import BaseEntity
from ..context import WebContext


class BaseChainedUpdate(metaclass=abc.ABCMeta):

    def __init__(self, clz, placeholder, table: str = None, logic_delete_col: str = None):
        self.clz = clz
        self.table = table if table is not None else self.clz.table_name()
        self.__conditions: list[tuple] = [(logic_delete_col if logic_delete_col is not None else 'deleted', 0, '=')]
        self.__sets = {}
        self.placeholder = placeholder

    def __args(self):
        if len(self.__conditions) == 0:
            return ()
        return tuple([cond[1] for cond in self.__conditions])

    def __where(self):
        if len(self.__conditions) == 0:
            return ''
        return 'where ' + ' and '.join([f'{cond[0]} {cond[2]} {self.placeholder}' for cond in self.__conditions])

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

    def set(self, **sets):
        self.__sets = sets
        return self

    def delete_statement(self) -> tuple[str, tuple]:
        if self.__conditions is None or self.__where() == '':
            raise Exception('conditions is required')

        sql = f'DELETE FROM {self.table} {self.__where()}'
        args = self.__args()
        print(f'#### sql: {sql}')
        print(f'#### args: {args}')
        return sql, args

    def update_statement(self) -> tuple[str, tuple]:
        if self.__sets is None or len(self.__sets.keys()) == 0:
            raise Exception('update set is required')
        if self.__conditions is None or self.__where() == '':
            raise Exception('update condition is required')

        update_by = WebContext().uid()
        if update_by is not None:
            self.__sets['update_by'] = WebContext().uid()
        self.__sets['update_at'] = datetime.now()
        sql = f'UPDATE {self.table} SET {", ".join([f"{col} = {self.placeholder}" for col in self.__sets.keys()])} {self.__where()}'
        args = tuple(self.__sets.values()) + self.__args()
        print(f'#### sql: {sql}')
        print(f'#### args: {args}')
        return sql, args

    def update_entity_statement(self, entity: BaseEntity) -> tuple[str, tuple]:
        entity.update_by = WebContext().uid()
        entity.update_at = datetime.now()
        update_cols = [f'{col} = {self.placeholder}' for col in self.clz.columns(exclude=["id"])]
        sql = f'UPDATE {self.table} SET {", ".join(update_cols)} where id = {self.placeholder}'
        args = tuple([getattr(entity, col) for col in self.clz.columns(exclude=["id"])] + [entity.id])
        print(f'#### sql: {sql}')
        print(f'#### args: {args}')
        return sql, args

    def insert_entity_statement(self, entity: BaseEntity) -> tuple[str, tuple]:
        now = datetime.now()
        entity.deleted = 0
        entity.create_by = WebContext().uid()
        entity.create_at = now

        cols = ', '.join(self.clz.columns(exclude=["id"]))
        placeholders = ', '.join([self.placeholder for _ in self.clz.columns(exclude=["id"])])
        sql = f'INSERT INTO {self.table} ({cols}) VALUES ({placeholders})'
        args = tuple([getattr(entity, col) for col in self.clz.columns(exclude=["id"])])
        print(f'#### sql: {sql}')
        print(f'#### args: {args}')
        return sql, args

    def insert_bulk_statement(self, duplicated_ignore=False) -> str:
        cols = ', '.join(self.clz.columns(exclude=["id"]))
        placeholders = ', '.join([self.placeholder for _ in self.clz.columns(exclude=["id"])])
        sql = f'INSERT {"OR IGNORE" if duplicated_ignore else ""} INTO {self.table} ({cols}) VALUES ({placeholders})'
        print(f'#### sql: {sql}')
        return sql
