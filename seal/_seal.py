from .config import Configuration
from loguru import logger


class Seal:

    def __init__(self):
        self.configuration = Configuration()
        self.initialized = False

    def init(self, config_path):
        self.configuration.load(config_path)
        self.initialized = True

        logger.add(self.get_config('seal', 'loguru', 'path'),
                   rotation=self.get_config('seal', 'loguru', 'rotation'),
                   retention=self.get_config('seal', 'loguru', 'retention'),
                   level=self.get_config('seal', 'loguru', 'level'))
        logger.info(f'初始化 seal({self}) 成功')
        return self

    def get_config(self, *keys):
        if not self.initialized:
            raise ValueError('Seal 未初始化')
        return self.configuration.get_conf(*keys)
