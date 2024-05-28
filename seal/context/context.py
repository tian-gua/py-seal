from contextvars import ContextVar

from seal.wrapper.decorator import singleton

page_ctx = ContextVar('page')


@singleton
class PageContext:

    def __init__(self):
        self.__ctx = page_ctx

    def get(self):
        return self.__ctx.get()

    def set(self, page, page_size, total):
        self.__ctx.set((page, page_size, total))


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
