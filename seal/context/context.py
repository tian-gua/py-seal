from contextvars import ContextVar


class WebContext:
    def __init__(self, uid=None):
        self.uid = uid

    def set_uid(self, uid):
        self.uid = uid

    def get_uid(self):
        return self.uid

    def set(self, key, value):
        setattr(self, key, value)


web_context = ContextVar('web', default=WebContext())
