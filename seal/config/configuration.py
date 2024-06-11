import yaml

from ..wrapper import singleton


@singleton
class Configuration:

    def __init__(self):
        self.__config = {}

    def load(self, path: str):
        with open(path, 'r') as f:
            self.__config = yaml.load(f, Loader=yaml.FullLoader)

    def get_config(self, key: str):
        return self.__config.__getitem__('seal').__getitem__(key)
