from .config import Configuration
from .wrapper import singleton


@singleton
class Seal:

    def __init__(self):
        self.config = Configuration()

    def init(self, config_path):
        self.config.load(config_path)
        print(f'初始化 seal({self}) 成功')
        return self

    def get_config(self, key: str):
        return self.config.get_config(key)


def init_seal(config_path):
    global seal
    seal = Seal().init(config_path)


def get_seal():
    return seal


seal: Seal | None = None
