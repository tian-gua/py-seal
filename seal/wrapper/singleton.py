from functools import wraps


def singleton(orig_cls):
    orig_new = orig_cls.__new__
    orig_init = orig_cls.__init__
    instance = None

    def singleton_init(self, *args, **kwargs):
        pass

    @wraps(orig_cls.__new__)
    def __new__(cls, *args, **kwargs):
        nonlocal instance
        if instance is None:
            instance = orig_new(cls, *args, **kwargs)
            orig_init(instance, *args, **kwargs)

            if not hasattr(cls, 'singleton_init'):
                cls.singleton_init = singleton_init
        return instance

    @wraps(orig_cls.__init__)
    def __init__(obj, *args, **kwargs):
        orig_cls.singleton_init(obj, *args, **kwargs)

    orig_cls.__new__ = __new__
    orig_cls.__init__ = __init__
    return orig_cls
