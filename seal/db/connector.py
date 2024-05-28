import abc


class DBConnector(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_connection(self):
        pass
