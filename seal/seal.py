from .config import Configuration
from .db.query_wrapper import QueryWrapper
from .db.update_wrapper import UpdateWrapper
from .db.insert_wrapper import InsertWrapper
from .db.wrapper import Wrapper
from .db.result import Results
from .cache import LRUCache
from loguru import logger


class Seal:

    def __init__(self):
        self._configuration = Configuration()
        self._initialized = False
        self._data_sources = {}
        self._default_data_source = None
        self._lru_cache: LRUCache = LRUCache(1024)

    def init(self, config_path):
        self._configuration.load(config_path)
        self._initialized = True

        logger.add(self.get_config('seal', 'loguru', 'path'),
                   rotation=self.get_config('seal', 'loguru', 'rotation'),
                   retention=self.get_config('seal', 'loguru', 'retention'),
                   level=self.get_config('seal', 'loguru', 'level'))

        data_source_config = self.get_config('seal', 'data_source')
        for data_source_name, data_source_conf in data_source_config.items():
            data_source = self.get_config('seal', 'data_source', data_source_name)
            if 'mysql' == data_source.get('type'):
                from .db.mysql.mysql_data_source import MysqlDataSource
                self._data_sources[data_source_name] = MysqlDataSource(data_source_conf)
            elif 'sqlite' == data_source.get('type'):
                from .db.sqlite.sqlite_data_source import SqliteDataSource
                self._data_sources[data_source_name] = SqliteDataSource(data_source_conf)
            else:
                raise ValueError(f'不支持的数据源类型: {data_source.get("type")}')

            if data_source_conf.get('default', False):
                self._default_data_source = self._data_sources[data_source_name]

        if len(self._data_sources.values()) == 1 and self._default_data_source is None:
            self._default_data_source = list(self._data_sources.values())[0]
        if self._default_data_source is None:
            raise ValueError('未配置默认数据源')
        logger.info(f'初始化 seal({self}) 成功')
        return self

    def data_source(self, data_source_name):
        if not self._initialized:
            raise ValueError('Seal 未初始化')
        return self._data_sources.get(data_source_name)

    def get_config(self, *keys):
        if not self._initialized:
            raise ValueError('Seal 未初始化')
        return self._configuration.get_conf(*keys)

    def query(self, table) -> QueryWrapper:
        return QueryWrapper(table=table,
                            data_source=self._default_data_source,
                            tenant_field=self.get_config('seal', 'orm', 'tenant_field'),
                            tenant_value=self.get_config('seal', 'orm', 'tenant_value'),
                            logic_delete_field=self.get_config('seal', 'orm', 'logic_delete_field'),
                            logic_delete_true=self.get_config('seal', 'orm', 'logic_delete_true'),
                            logic_delete_false=self.get_config('seal', 'orm', 'logic_delete_false'),
                            )

    def update(self, table) -> UpdateWrapper:
        return UpdateWrapper(table,
                             self._default_data_source,
                             tenant_field=self.get_config('seal', 'orm', 'tenant_field'),
                             tenant_value=self.get_config('seal', 'orm', 'tenant_value'),
                             update_by_field=self.get_config('seal', 'orm', 'update_by_field'),
                             update_at_field=self.get_config('seal', 'orm', 'update_at_field'),
                             logic_delete_field=self.get_config('seal', 'orm', 'logic_delete_field'),
                             logic_delete_true=self.get_config('seal', 'orm', 'logic_delete_true'),
                             logic_delete_false=self.get_config('seal', 'orm', 'logic_delete_false'),
                             )

    def insert(self, table) -> InsertWrapper:
        return InsertWrapper(table,
                             self._default_data_source,
                             tenant_field=self.get_config('seal', 'orm', 'tenant_field'),
                             tenant_value=self.get_config('seal', 'orm', 'tenant_value'),
                             logic_delete_field=self.get_config('seal', 'orm', 'logic_delete_field'),
                             logic_delete_true=self.get_config('seal', 'orm', 'logic_delete_true'),
                             logic_delete_false=self.get_config('seal', 'orm', 'logic_delete_false'),
                             )

    def wrapper(self) -> Wrapper:
        return Wrapper()

    def raw(self, sql, args=()) -> Results:
        return self._default_data_source.get_executor().raw(sql, args)

    def enable_logical_delete(self, logical_delete):
        self._logical_delete = logical_delete
        return self

    def model(self, model_name):
        return self._default_data_source.get_data_structure(model_name)

    def lru_cache(self) -> LRUCache:
        return self._lru_cache
