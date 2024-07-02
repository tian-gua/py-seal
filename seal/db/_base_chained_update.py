import abc
from collections import namedtuple
from datetime import datetime
from .table_info import TableInfo
from ..model import BaseEntity, dynamic_models
from ..context import WebContext
from loguru import logger


class BaseChainedUpdate(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def meta(self):
        ...

    def __init__(self, target, logic_delete_col: str = None, placeholder: str = None):
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
        self.sets = {}
        self.placeholder = placeholder

    def columns(self, exclude=None):
        if issubclass(self.clz, BaseEntity):
            return [col for col in self.clz.columns() if col not in exclude]
        return [key for key in self.clz._fields if key not in exclude]

    def build_args(self):
        if len(self._where) == 0:
            return ()
        return tuple([cond[1] for cond in self._where])

    def build_where(self):
        if len(self._where) == 0:
            return ''
        return 'where ' + ' and '.join([f'{cond[0]} {cond[2]} {self.placeholder}' for cond in self._where])

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

    def set(self, **sets):
        self.sets = sets
        return self

    def delete_statement(self) -> tuple[str, tuple]:
        if self._where is None or self.build_where() == '':
            raise Exception('conditions is required')

        sql = f'DELETE FROM {self.table} {self.build_where()}'
        args = self.build_args()
        logger.info(f'#### sql: {sql}')
        logger.info(f'#### args: {args}')
        return sql, args

    def update_statement(self) -> tuple[str, tuple]:
        if self.sets is None or len(self.sets.keys()) == 0:
            raise Exception('update set is required')
        if self._where is None or self.build_where() == '':
            raise Exception('update condition is required')

        update_by = WebContext().uid()
        if update_by is not None:
            self.sets['update_by'] = WebContext().uid()
        self.sets['update_at'] = datetime.now()
        sql = f'UPDATE {self.table} SET {", ".join([f"{col} = {self.placeholder}" for col in self.sets.keys()])} {self.build_where()}'
        args = tuple(self.sets.values()) + self.build_args()
        logger.info(f'#### sql: {sql}')
        logger.info(f'#### args: {args}')
        return sql, args

    def update_by_pk_statement(self, record) -> tuple[str, tuple]:
        if record is None:
            raise ValueError('null data')

        now = datetime.now()
        update_columns = self.columns(exclude=["id"])
        if isinstance(record, dict):
            if 'update_by' in update_columns:
                record['update_by'] = WebContext().uid()
            if 'update_at' in update_columns:
                record['update_at'] = now
            valid_columns = [col for col in update_columns if record.__contains__(col)]
            id_ = record['id']
            args = tuple([record[col] for col in valid_columns] + [id_])

        else:
            if isinstance(record, BaseEntity):
                record.update_by = WebContext().uid()
                record.update_at = now
            else:
                if 'update_by' in update_columns:
                    record.update_by = WebContext().uid()
                if 'update_at' in update_columns:
                    record.update_at = now
            valid_columns = [col for col in update_columns if getattr(record, col) is not None]
            id_ = record.id
            args = tuple([getattr(record, col) for col in valid_columns] + [id_])

        sets = [f'{col} = {self.placeholder}' for col in valid_columns]
        sql = f'UPDATE {self.table} SET {", ".join(sets)} where id = {self.placeholder}'
        logger.info(f'#### sql: {sql}')
        logger.info(f'#### args: {args}')
        return sql, args

    def insert_statement(self, record, duplicated_ignore=False, duplicated_key_update=False) -> tuple[str, tuple]:
        if record is None:
            raise ValueError('null data')

        now = datetime.now()
        insert_columns = self.columns(exclude=["id"])

        if isinstance(record, dict):
            if 'deleted' in insert_columns:
                record['deleted'] = 0
            if 'create_by' in insert_columns:
                record['create_by'] = WebContext().uid()
            if 'create_at' in insert_columns:
                record['create_at'] = now
            args = tuple([record.get(col, None) for col in insert_columns])
        else:
            if isinstance(record, BaseEntity):
                record.deleted = 0
                record.create_by = WebContext().uid()
                record.create_at = now
            else:
                if 'deleted' in insert_columns:
                    record.deleted = 0
                if 'create_by' in insert_columns:
                    record.create_by = WebContext().uid()
                if 'create_at' in insert_columns:
                    record.create_at = now
            args = tuple([getattr(record, col) for col in insert_columns])

        placeholders = ', '.join([self.placeholder for _ in insert_columns])
        columns_str = ', '.join(insert_columns)

        sql = f'INSERT {"OR IGNORE" if duplicated_ignore else ""} INTO {self.table} ({columns_str}) VALUES ({placeholders})'
        if duplicated_key_update:
            sql += f'{" ON DUPLICATE KEY UPDATE" if duplicated_key_update else ""}'
            sql += ', '.join([f'{col} = {self.placeholder}' for col in self.columns(exclude=["id"])])
        if duplicated_key_update:
            args += args
        logger.info(f'#### sql: {sql}')
        logger.info(f'#### args: {args}')
        return sql, args

    def insert_bulk_statement(self, duplicated_ignore=False, duplicated_key_update=False) -> str:
        columns = self.columns(exclude=["id"])
        columns_str = ', '.join(columns)
        placeholders = ', '.join([self.placeholder for _ in columns])
        sql = f'INSERT {"OR IGNORE" if duplicated_ignore else ""} INTO {self.table} ({columns_str}) VALUES ({placeholders})'
        if duplicated_key_update:
            sql += f'{" ON DUPLICATE KEY UPDATE " if duplicated_key_update else ""}'
            sql += ', '.join([f'{col} = {self.placeholder}' for col in columns])
        logger.info(f'#### sql: {sql}')
        return sql
