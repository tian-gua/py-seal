from seal.ws import dispatcher
from loguru import logger


def handler(path: str):
    def wrapper(func):
        dispatcher.register(path, func)
        logger.info(f'Registered websocket route: {path}')
        return func

    return wrapper
