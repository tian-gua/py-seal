import abc


class DataSource(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_connection(self):
        pass

    @abc.abstractmethod
    def get_data_structure(self, table):
        pass

    @abc.abstractmethod
    def get_executor(self):
        pass

    @abc.abstractmethod
    def get_type(self):
        pass


class DataSourceManager:
    data_sources = {}

    @staticmethod
    def register(name, data_source: DataSource):
        DataSourceManager.data_sources[name] = data_source

    @staticmethod
    def get(name):
        return DataSourceManager.data_sources[name]
