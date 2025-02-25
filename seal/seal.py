import threading
from datetime import datetime, timedelta
from typing import Any, Dict

import jwt
from loguru import logger

from seal.db.protocol import IDataSource
from seal.model.result import Results
from .cache import LRUCache, Cache
from .config import configurator
from .context import web_context, WebContext
from .db import Wrapper, sql_context, InsertWrapper, QueryWrapper, UpdateWrapper, structures
from .router import get, post, put, delete


class Seal:

    def __init__(self):
        self._initialized = False
        self.data_source_dict: Dict[str, IDataSource] = {}
        self._lru_cache: LRUCache = LRUCache(102400)
        self._cache = Cache()

        self.get = get
        self.post = post
        self.put = put
        self.delete = delete

        self._web_context = web_context

    def init(self, config_path: str, init_loguru: bool = True, init_database: bool = True):
        configurator.load(config_path)
        self._initialized = True

        if init_loguru:
            logger.add(self.get_config('seal', 'loguru', 'path'),
                       rotation=self.get_config('seal', 'loguru', 'rotation'),
                       retention=self.get_config('seal', 'loguru', 'retention'),
                       level=self.get_config('seal', 'loguru', 'level'))

        if init_database:
            data_source_config = self.get_config('seal', 'data_source')
            for data_source_name, data_source_conf in data_source_config.items():
                data_source = self.get_config('seal', 'data_source', data_source_name)
                if 'mysql' == data_source.get('dialect'):
                    from .db.mysql.mysql_data_source import MysqlDataSource
                    mysql_ds = MysqlDataSource(name=data_source_name, conf=data_source_conf)
                    self.data_source_dict[data_source_name] = mysql_ds

                    # a new thread for keeping connections active
                    threading.Thread(target=mysql_ds.ping, args=[data_source.get('ping')], daemon=True).start()

                elif 'sqlite' == data_source.get('dialect'):
                    from .db.sqlite.sqlite_data_source import SqliteDataSource
                    self.data_source_dict[data_source_name] = SqliteDataSource(name=data_source_name, conf=data_source_conf)
                else:
                    raise ValueError(f'不支持的数据源类型: {data_source.get("dialect")}')

            if 'default' not in self.data_source_dict:
                raise ValueError('unknown default data source')

        logger.info(f'init seal with config: {config_path}')
        return self

    def data_source(self, data_source_name):
        if not self._initialized:
            raise ValueError('uninitialized seal')
        return self.data_source_dict.get(data_source_name)

    def get_config(self, *keys):
        if not self._initialized:
            raise ValueError('uninitialized seal')
        return configurator.get_config(*keys)

    def get_config_default(self, *keys, default=None):
        if not self._initialized:
            raise ValueError('uninitialized seal')
        return configurator.get_conf_default(*keys, default=default)

    def query_wrapper(self, table: str, database: str | None = None, data_source: str = 'default', disable_logical_deleted=False) -> QueryWrapper:
        if database is None:
            database = self.data_source_dict[data_source].get_default_database()

        logical_deleted_field = self.get_config_default('seal', 'orm', 'logical_deleted_field')
        if disable_logical_deleted:
            logical_deleted_field = None
        return QueryWrapper(table=table,
                            database=database,
                            data_source=self.data_source_dict[data_source],
                            tenant_field=self.get_config_default('seal', 'orm', 'tenant_field'),
                            tenant_value=self.get_config_default('seal', 'orm', 'tenant_value'),
                            logical_deleted_field=logical_deleted_field,
                            logical_deleted_value_true=self.get_config_default('seal', 'orm', 'logical_deleted_value_true'),
                            logical_deleted_value_false=self.get_config_default('seal', 'orm', 'logical_deleted_value_false'), )

    def update_wrapper(self, table: str, database: str | None = None, data_source: str = 'default', disable_logical_deleted=False) -> UpdateWrapper:
        if database is None:
            database = self.data_source_dict[data_source].get_default_database()

        logical_deleted_field = self.get_config_default('seal', 'orm', 'logical_deleted_field')
        if disable_logical_deleted:
            logical_deleted_field = None
        return UpdateWrapper(table,
                             database=database,
                             data_source=self.data_source_dict[data_source],
                             tenant_field=self.get_config_default('seal', 'orm', 'tenant_field'),
                             tenant_value=self.get_config_default('seal', 'orm', 'tenant_value'),
                             updated_by_field=self.get_config_default('seal', 'orm', 'updated_by_field'),
                             updated_at_field=self.get_config_default('seal', 'orm', 'updated_at_field'),
                             logical_deleted_field=logical_deleted_field,
                             logical_deleted_value_true=self.get_config_default('seal', 'orm', 'logical_deleted_value_true'),
                             logical_deleted_value_false=self.get_config_default('seal', 'orm', 'logical_deleted_value_false'), )

    def insert_wrapper(self, table: str, database: str | None = None, data_source: str = 'default', disable_logical_deleted=False) -> InsertWrapper:
        if database is None:
            database = self.data_source_dict[data_source].get_default_database()

        logical_deleted_field = self.get_config_default('seal', 'orm', 'logical_deleted_field')
        if disable_logical_deleted:
            logical_deleted_field = None
        return InsertWrapper(table,
                             database=database,
                             data_source=self.data_source_dict[data_source],
                             tenant_field=self.get_config_default('seal', 'orm', 'tenant_field'),
                             tenant_value=self.get_config_default('seal', 'orm', 'tenant_value'),
                             logical_deleted_field=logical_deleted_field,
                             logical_deleted_value_true=self.get_config_default('seal', 'orm', 'logical_deleted_value_true'),
                             logical_deleted_value_false=self.get_config_default('seal', 'orm', 'logical_deleted_value_false'), )

    # noinspection PyMethodMayBeStatic
    def conditions_wrapper(self) -> Wrapper:
        return Wrapper()

    def custom_query(self, data_source: str, sql: str, args=tuple[Any, ...]) -> Results:
        if data_source not in self.data_source_dict:
            raise ValueError(f'unknown data source: {data_source}')
        if sql is None:
            raise ValueError('sql is required')
        return self.data_source_dict[data_source].get_executor().custom_query(sql, args)

    def custom_update(self, data_source: str, sql: str, args=tuple[Any, ...]) -> int | None:
        if data_source not in self.data_source_dict:
            raise ValueError(f'unknown data source: {data_source}')
        if sql is None:
            raise ValueError('sql is required')
        return self.data_source_dict['default'].get_executor().custom_update(sql, args)

    # noinspection PyMethodMayBeStatic
    def get_structure(self, name: str = None, data_source: str = 'default', database: str | None = None) -> Any:
        if database is None:
            database = self.data_source_dict[data_source].get_default_database()
        if name is None:
            raise ValueError('name is required')
        structure = structures.get(data_source, database, name)
        if structure is None:
            structure = self.data_source_dict[data_source].load_structure(database, name)
            structures.register(data_source, database, name, structure)
        return structure

    def lru_cache(self) -> LRUCache:
        return self._lru_cache

    def memory_cache(self) -> Cache:
        return self._cache

    # noinspection PyMethodMayBeStatic
    def generate_token(self, **payloads):
        try:
            payloads["exp"] = datetime.now() + timedelta(seconds=configurator.get_config('seal', 'authorization', 'expire'))
            token = jwt.encode(payloads, configurator.get_config('seal', 'authorization', 'jwt_key'), algorithm="HS256")
        except Exception as e:
            raise Exception(f"Token generation failed: {e}")
        return token

    def web_context(self) -> WebContext:
        return self._web_context.get()

    # noinspection PyMethodMayBeStatic
    def begin_tx(self, data_source: str = 'default'):
        sql_context.get().begin(self.data_source_dict[data_source])

    # noinspection PyMethodMayBeStatic
    def commit_tx(self):
        sql_context.get().commit()

    # noinspection PyMethodMayBeStatic
    def rollback_tx(self):
        sql_context.get().rollback()
