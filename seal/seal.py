from .config import Configuration
from loguru import logger


class Seal:

    def __init__(self):
        self.configuration = Configuration()
        self.initialized = False

    def init(self, config_path):
        self.configuration.load(config_path)

        logger.add('seal.log', rotation='1 day', retention='7 days', level='DEBUG')
        logger.info(f'初始化 seal({self}) 成功')

        self.initialized = True
        return self

    def get_config(self, *keys):
        if not self.initialized:
            raise ValueError('Seal 未初始化')
        return self.configuration.get_conf(*keys)
