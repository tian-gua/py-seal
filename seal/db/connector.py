import abc


class Connector(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_connection(self):
        pass
