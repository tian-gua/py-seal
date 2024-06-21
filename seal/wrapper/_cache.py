import functools

from ..cache import cache


def cache_func(func):

    @functools.wraps(func)
    def wrapper(*args):
        is_method = True if func.__name__ != func.__qualname__ else False
        args_ = args[1:] if is_method else args

        func_name = func.__qualname__
        key = func_name + '::' + ':'.join(map(str, args_))

        value = cache.get(key)
        if value:
            return value
        else:
            result = func(*args)
            cache.set(key, result)
            return result

    return wrapper
