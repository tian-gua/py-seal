from .data_source import DataSource


class Executor:

    def __init__(self, data_source: DataSource):
        self.data_source = data_source
