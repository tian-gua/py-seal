from pydantic import BaseModel, create_model
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

    # def dynamic_columns() -> dict[str, tuple]:
    #     cols_cache = dynamic_cache[f'dynamic_columns_{cls.table_name()}']
    #     if cols_cache is not None:
    #         return cols_cache
    #
    #     cols = {}
    #     if dynamic is not None and dynamic is DynamicType.Sqlite:
    #         table_info_list = Meta.get_table_info(cls.table_name())
    #         for table_info in table_info_list:
    #             if table_info.type == 'INTEGER':
    #                 cols[table_info.name] = (int, ...)
    #             elif table_info.type == 'TEXT':
    #                 cols[table_info.name] = (str, ...)
    #             elif table_info.type == 'REAL':
    #                 cols[table_info.name] = (float, ...)
    #             elif table_info.type == 'BLOB':
    #                 cols[table_info.name] = (bytes, ...)
    #             elif table_info.type == 'DATETIME':
    #                 cols[table_info.name] = (datetime, ...)
    #             elif table_info.type == 'NULL':
    #                 cols[table_info.name] = (None, ...)
    #             else:
    #                 cols[table_info.name] = (str, ...)
    #
    #     dynamic_cache[f'dynamic_columns_{cls.table_name()}'] = cols
    #     return cols

    # def dynamic_model():
    #     return create_dynamic_model(f'Dynamic{cls.__name__}', cls.dynamic_columns())

    setattr(cls, 'columns', columns)
    setattr(cls, 'table_name', table_name)
    setattr(cls, 'dynamic', dynamic)
    return cls
