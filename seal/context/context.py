from contextvars import ContextVar

web_ctx = ContextVar('web', default={})


def ctx_get():
    return web_ctx.get()


def ctx_set(value: dict):
    web_ctx.set(value)


def ctx_uid():
    var = ctx_get()
    if 'uid' not in var:
        return None
    return var['uid']
