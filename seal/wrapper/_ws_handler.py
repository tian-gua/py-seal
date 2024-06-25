from ..ws import dispatcher
from loguru import logger


def handler(path: str):
    def wrapper(func):
        dispatcher.register(path, func)
        logger.info(f'注册 websocket 路由: {path}')
        return func

    return wrapper
