from functools import wraps
from boltons import strutils


def parametrized(decorator):
    def layer(*args, **kwargs):
        def repl(cls):
            return decorator(cls, *args, **kwargs)

        return repl

    return layer


@parametrized
def entity(cls, table: str = None, ignore: list[str] = None):
    def columns(exclude: list[str] = None):
        cs = [key for key in cls.model_fields if
              (exclude is None or key not in exclude) and (ignore is None or key not in ignore)]
        # print(cs)
        return cs

    def table_name():
        if table is not None:
            return table
        return strutils.camel2under(cls.__name__)

    setattr(cls, 'columns', columns)
    setattr(cls, 'table_name', table_name)
    return cls


def singleton(orig_cls):
    orig_new = orig_cls.__new__
    instance = None

    @wraps(orig_cls.__new__)
    def __new__(cls, *args, **kwargs):
        nonlocal instance
        if instance is None:
            instance = orig_new(cls, *args, **kwargs)
        return instance

    orig_cls.__new__ = __new__
    return orig_cls
