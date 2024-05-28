import yaml

from seal.wrapper.decorator import singleton


@singleton
class Configuration:

    def __init__(self):
        self.config = {}

    def load(self, path: str):
        with open(path, 'r') as f:
            self.config = yaml.load(f, Loader=yaml.FullLoader)
