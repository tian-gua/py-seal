from ..utils.str_utils import snake_case


def parametrized(decorator):
    def layer(*args, **kwargs):
        def repl(cls):
            return decorator(cls, *args, **kwargs)

        return repl

    return layer


@parametrized
def entity(cls, table: str = None, dynamic=False, ignore: list[str] = None):
    def columns(exclude: list[str] = None):
        cols = [key for key in cls.model_fields if
                (exclude is None or key not in exclude) and (ignore is None or key not in ignore)]
        return cols

    def table_name():
        if table is not None:
            return table
        return snake_case(cls.__name__)

    setattr(cls, 'columns', columns)
    setattr(cls, 'table_name', table_name)
    setattr(cls, 'dynamic', dynamic)
    return cls
