from contextvars import ContextVar
from ..wrapper import singleton

web_ctx = ContextVar('web', default={})


@singleton
class WebContext:
    def __init__(self):
        self.ctx = web_ctx

    def get(self):
        return self.ctx.get()

    def set(self, value: dict):
        self.ctx.set(value)

    def uid(self):
        var = self.get()
        if 'uid' not in var:
            return None
        return var['uid']
