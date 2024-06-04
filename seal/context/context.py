from contextvars import ContextVar
from ..wrapper.singleton import singleton

web_ctx = ContextVar('web')


@singleton
class WebContext:
    def __init__(self):
        self.__ctx = web_ctx

    def get(self):
        return self.__ctx.get()

    def set(self, value: dict):
        self.__ctx.set(value)

    def uid(self):
        return self.get()['uid']
