import abc
from datetime import datetime
from ..model import BaseEntity
from ..context import WebContext
from loguru import logger


class BaseChainedUpdate(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def meta(self):
        ...

    def __init__(self, clz, placeholder, table: str = None, logic_delete_col: str = None):
        self.clz = clz
        self.table = table if table is not None else self.clz.table_name()
        self.__conditions: list[tuple] = [(logic_delete_col if logic_delete_col is not None else 'deleted', 0, '=')]
        self.sets = {}
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

    def columns(self, exclude=None):
        if self.is_dynamic:
            return [key for key, db_type in self.table_info.columns if key not in exclude]
        return [col for col in self.clz.columns() if col not in exclude]

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
        self.sets = sets
        return self

    def delete_statement(self) -> tuple[str, tuple]:
        if self.__conditions is None or self.__where() == '':
            raise Exception('conditions is required')

        sql = f'DELETE FROM {self.table} {self.__where()}'
        args = self.__args()
        logger.info(f'#### sql: {sql}')
        logger.info(f'#### args: {args}')
        return sql, args

    def update_statement(self) -> tuple[str, tuple]:
        if self.sets is None or len(self.sets.keys()) == 0:
            raise Exception('update set is required')
        if self.__conditions is None or self.__where() == '':
            raise Exception('update condition is required')

        update_by = WebContext().uid()
        if update_by is not None:
            self.sets['update_by'] = WebContext().uid()
        self.sets['update_at'] = datetime.now()
        sql = f'UPDATE {self.table} SET {", ".join([f"{col} = {self.placeholder}" for col in self.sets.keys()])} {self.__where()}'
        args = tuple(self.sets.values()) + self.__args()
        logger.info(f'#### sql: {sql}')
        logger.info(f'#### args: {args}')
        return sql, args

    def update_by_pk_statement(self, entity: BaseEntity = None, data: dict = None) -> tuple[str, tuple]:
        if entity is None and data is None:
            raise ValueError('null data')

        if entity is not None:
            entity.update_by = WebContext().uid()
            entity.update_at = datetime.now()
        else:
            data['update_by'] = WebContext().uid()
            data['update_at'] = datetime.now()

        update_cols = [f'{col} = {self.placeholder}' for col in self.columns(exclude=["id"])]
        sql = f'UPDATE {self.table} SET {", ".join(update_cols)} where id = {self.placeholder}'

        if entity is not None:
            args = tuple([getattr(entity, col) for col in self.columns(exclude=["id"])] + [entity.id])
        else:
            args = tuple([data[col] for col in self.columns(exclude=["id"])] + [data['id']])
        logger.info(f'#### sql: {sql}')
        logger.info(f'#### args: {args}')
        return sql, args

    def insert_statement(self, entity: BaseEntity = None, data: dict = None, duplicated_ignore=False,
                         duplicated_key_update=False) -> tuple[str, tuple]:
        if entity is None and data is None:
            raise ValueError('null data')

        if entity is not None:
            now = datetime.now()
            entity.deleted = 0
            entity.create_by = WebContext().uid()
            entity.create_at = now
        else:
            now = datetime.now()
            data['deleted'] = 0
            data['create_by'] = WebContext().uid()
            data['create_at'] = now

        cols = ', '.join(self.columns(exclude=["id"]))
        placeholders = ', '.join([self.placeholder for col in self.columns(exclude=["id"])])

        sql = f'INSERT {"OR IGNORE" if duplicated_ignore else ""} INTO {self.table} ({cols}) VALUES ({placeholders})'
        if duplicated_key_update:
            sql += f'{" ON DUPLICATE KEY UPDATE" if duplicated_key_update else ""}'
            sql += ', '.join([f'{col} = {self.placeholder}' for col in self.columns(exclude=["id"])])

        if entity is not None:
            args = tuple([getattr(entity, col) for col in self.columns(exclude=["id"])])
            if duplicated_key_update:
                args += args
        else:
            args = tuple([data[col] for col in self.columns(exclude=["id"])])
            if duplicated_key_update:
                args += args
        logger.info(f'#### sql: {sql}')
        logger.info(f'#### args: {args}')
        return sql, args

    def insert_bulk_statement(self, duplicated_ignore=False, duplicated_key_update=False) -> str:
        cols = ', '.join(self.columns(exclude=["id"]))
        placeholders = ', '.join([self.placeholder for _ in self.columns(exclude=["id"])])
        sql = f'INSERT {"OR IGNORE" if duplicated_ignore else ""} INTO {self.table} ({cols}) VALUES ({placeholders})'
        if duplicated_key_update:
            sql += f'{" ON DUPLICATE KEY UPDATE " if duplicated_key_update else ""}'
            sql += ', '.join([f'{col} = {self.placeholder}' for col in self.columns(exclude=["id"])])
        logger.info(f'#### sql: {sql}')
        return sql
