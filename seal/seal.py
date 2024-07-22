from .config import Configuration
from .db.query_wrapper import QueryWrapper
from .db.update_wrapper import UpdateWrapper
from .db.insert_wrapper import InsertWrapper
from loguru import logger


class Seal:

    def __init__(self):
        self.configuration = Configuration()
        self.initialized = False
        self.data_sources = {}
        self.default_data_source = None
        self.logical_delete = None

    def init(self, config_path):
        self.configuration.load(config_path)
        self.initialized = True

        logger.add(self.get_config('seal', 'loguru', 'path'),
                   rotation=self.get_config('seal', 'loguru', 'rotation'),
                   retention=self.get_config('seal', 'loguru', 'retention'),
                   level=self.get_config('seal', 'loguru', 'level'))

        data_source_config = self.get_config('seal', 'data_source')
        for data_source_name, data_source_conf in data_source_config.items():
            data_source = self.get_config('seal', 'data_source', data_source_name)
            if 'mysql' == data_source.get('type'):
                from .db.mysql.mysql_data_source import MysqlDataSource
                self.data_sources[data_source_name] = MysqlDataSource(data_source_conf)
            elif 'sqlite' == data_source.get('type'):
                from .db.sqlite.sqlite_data_source import SqliteDataSource
                self.data_sources[data_source_name] = SqliteDataSource(data_source_conf)
            else:
                raise ValueError(f'不支持的数据源类型: {data_source.get("type")}')

            if data_source_conf.get('default', False):
                self.default_data_source = self.data_sources[data_source_name]

        if len(self.data_sources.values()) == 1 and self.default_data_source is None:
            self.default_data_source = list(self.data_sources.values())[0]
        if self.default_data_source is None:
            raise ValueError('未配置默认数据源')
        logger.info(f'初始化 seal({self}) 成功')
        return self

    def data_source(self, data_source_name):
        if not self.initialized:
            raise ValueError('Seal 未初始化')
        return self.data_sources.get(data_source_name)

    def get_config(self, *keys):
        if not self.initialized:
            raise ValueError('Seal 未初始化')
        return self.configuration.get_conf(*keys)

    def q(self, table):
        return QueryWrapper(table, self.default_data_source, self.logical_delete)

    def query_wrapper(self, table):
        return QueryWrapper(table, self.default_data_source, self.logical_delete)

    def u(self, table):
        return UpdateWrapper(table, self.default_data_source, self.logical_delete)

    def update_wrapper(self, table):
        return UpdateWrapper(table, self.default_data_source, self.logical_delete)

    def i(self, table):
        return InsertWrapper(table, self.default_data_source)

    def insert_wrapper(self, table):
        return InsertWrapper(table, self.default_data_source)

    def raw(self, sql, args=()):
        return self.default_data_source.get_executor().raw(sql, args)

    def enable_logical_delete(self, logical_delete):
        self.logical_delete = logical_delete
        return self

    def model(self, model_name):
        return self.default_data_source.get_data_structure(model_name)
