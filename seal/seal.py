from .config import Configuration
from .db.query_wrapper import QueryWrapper
from .db.update_wrapper import UpdateWrapper
from .db.insert_wrapper import InsertWrapper
from .db.wrapper import Wrapper
from .cache import LRUCache
from loguru import logger


class Seal:

    def __init__(self):
        self._configuration = Configuration()
        self._initialized = False
        self._data_sources = {}
        self._default_data_source = None
        self._logical_delete = None
        self._default_cache: LRUCache = LRUCache(512)

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

    def q(self, table):
        return QueryWrapper(table, self._default_data_source, self._logical_delete)

    def query_wrapper(self, table):
        return QueryWrapper(table, self._default_data_source, self._logical_delete)

    def u(self, table):
        return UpdateWrapper(table, self._default_data_source, self._logical_delete)

    def update_wrapper(self, table):
        return UpdateWrapper(table, self._default_data_source, self._logical_delete)

    def i(self, table):
        return InsertWrapper(table, self._default_data_source)

    def insert_wrapper(self, table):
        return InsertWrapper(table, self._default_data_source)

    def wrapper(self):
        return Wrapper(self._logical_delete)

    def raw(self, sql, args=()):
        return self._default_data_source.get_executor().raw(sql, args)

    def enable_logical_delete(self, logical_delete):
        self._logical_delete = logical_delete
        return self

    def model(self, model_name):
        return self._default_data_source.get_data_structure(model_name)

    def default_cache(self) -> LRUCache:
        return self._default_cache
