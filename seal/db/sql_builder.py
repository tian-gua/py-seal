from loguru import logger


def build_select(query_wrapper) -> (str, tuple):
    sql = f'SELECT {",".join(query_wrapper.field_list)} FROM {query_wrapper.table}'
    args = ()
    if len(query_wrapper.condition_tree.conditions) > 0:
        exp, args = query_wrapper.condition_tree.parse()
        sql += ' WHERE ' + exp
    if query_wrapper.order_by is not None:
        sql += f' ORDER BY {",".join(query_wrapper.order_by)}'
    if query_wrapper.limit is not None:
        sql += f' LIMIT {query_wrapper.limit}'
    if query_wrapper.offset is not None:
        sql += f' OFFSET {query_wrapper.offset}'

    return sql, args


def build_count(query_wrapper) -> (str, tuple):
    sql = f'SELECT COUNT(1) FROM {query_wrapper.table}'
    args = ()
    if len(query_wrapper.condition_tree.conditions) > 0:
        exp, args = query_wrapper.condition_tree.parse()
        sql += ' WHERE ' + exp

    return sql, args


def build_update(update_wrapper) -> (str, tuple):
    sql = f'UPDATE {update_wrapper.table} SET {",".join([f"{k}=?" for k in update_wrapper.update_fields.keys()])}'
    args = tuple(update_wrapper.update_fields.values())
    if len(update_wrapper.condition_tree.conditions) > 0:
        exp, args2 = update_wrapper.condition_tree.parse()
        sql += ' WHERE ' + exp
        args += args2
    return sql, args


def build_delete(update_wrapper) -> (str, tuple):
    sql = f'DELETE FROM {update_wrapper.table}'
    args = ()
    if len(update_wrapper.condition_tree.conditions) > 0:
        exp, args = update_wrapper.condition_tree.parse()
        sql += ' WHERE ' + exp
    return sql, args


def build_insert(insert_wrapper, data, duplicated_key_update=False, duplicated_key_ignore=False) -> (str, tuple):
    keys = None
    if isinstance(data, dict):
        keys = data.keys()

    sql = f'INSERT {"OR IGNORE" if duplicated_key_ignore else ""} INTO {insert_wrapper.table} ({",".join([field for field in insert_wrapper.insert_fields if keys is None or field in keys])}) VALUES ({",".join(["?" for field in insert_wrapper.insert_fields if keys is None or field in keys])})'

    if isinstance(data, dict):
        args = tuple([data[field] for field in insert_wrapper.insert_fields if field in keys])
    else:
        args = tuple([getattr(data, field) for field in insert_wrapper.insert_fields])

    if duplicated_key_update:
        sql += f' ON DUPLICATE KEY UPDATE {",".join([f"{field}=?" for field in insert_wrapper.insert_fields if keys is None or field in keys])}'
        args = args * 2
    return sql, args


def build_insert_bulk(insert_wrapper, data_list, duplicated_key_update=False, duplicated_key_ignore=False) -> (
        str, list[tuple]):
    keys = None
    data = data_list[0]
    if isinstance(data, dict):
        keys = data.keys()

    sql = f'INSERT {"OR IGNORE" if duplicated_key_ignore else ""} INTO {insert_wrapper.table} ({",".join([field for field in insert_wrapper.insert_fields if keys is None or field in keys])}) VALUES ({",".join(["?" for field in insert_wrapper.insert_fields if keys is None or field in keys])})'

    if duplicated_key_update:
        sql += f' ON DUPLICATE KEY UPDATE {",".join([f"{field}=?" for field in insert_wrapper.insert_fields])}'

    if duplicated_key_update:
        if isinstance(data, dict):
            args = [tuple([data[field] for field in insert_wrapper.insert_fields if field in keys]) * 2 for data in
                    data_list]
        else:
            args = [tuple([getattr(data, field) for field in insert_wrapper.insert_fields]) * 2 for data in data_list]
    else:
        if isinstance(data, dict):
            args = [tuple([data[field] for field in insert_wrapper.insert_fields if field in keys]) for data
                    in data_list]
        else:
            args = [tuple([getattr(data, field) for field in insert_wrapper.insert_fields]) for data in data_list]
    return sql, args


def build_insert_iterator(insert_wrapper, data_list, duplicated_key_update=False, duplicated_key_ignore=False):
    keys = None
    if isinstance(data_list[0], dict):
        keys = data_list[0].keys()

    sql = f'INSERT {"OR IGNORE" if duplicated_key_ignore else ""} INTO {insert_wrapper.table} ({",".join([field for field in insert_wrapper.insert_fields if keys is None or field in keys])}) VALUES ({",".join(["?" for field in insert_wrapper.insert_fields if keys is None or field in keys])})'

    if duplicated_key_update:
        sql += f' ON DUPLICATE KEY UPDATE {",".join([f"{field}=?" for field in insert_wrapper.insert_fields])}'

    def data_iterator(callback):
        for data in data_list:
            if isinstance(data, dict):
                args = tuple([data[field] for field in insert_wrapper.insert_fields if field in keys])
            else:
                args = tuple([getattr(data, field) for field in insert_wrapper.insert_fields])
            logger.debug(f'#### args: {args}')
            callback(sql, args)

    logger.debug(f'#### sql: {sql}')
    return data_iterator
