from .config import Configuration
from .wrapper import singleton

__author__ = 'melon'
__email__ = '77729052@qq.com'
__version__ = '0.1.4'


@singleton
class Seal:

    def __init__(self):
        self.configuration = Configuration()

    def init(self, config_path):
        self.configuration.load(config_path)
        print(f'初始化 seal({self}) 成功')
        return self

    def get_config(self, *keys):
        return self.configuration.get_conf(*keys)


def init_seal(config_path):
    global seal
    seal = Seal().init(config_path)


def get_seal():
    return seal


def get_config(*keys):
    return seal.get_config(*keys)


seal: Seal | None = None
