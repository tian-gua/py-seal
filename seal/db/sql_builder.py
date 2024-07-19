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

    logger.debug(f'#### sql: {sql}')
    logger.debug(f'#### args: {args}')
    return sql, args


def build_count(query_wrapper) -> (str, tuple):
    sql = f'SELECT COUNT(1) FROM {query_wrapper.table}'
    args = ()
    if len(query_wrapper.condition_tree.conditions) > 0:
        exp, args = query_wrapper.condition_tree.parse()
        sql += ' WHERE ' + exp

    logger.debug(f'#### sql: {sql}')
    logger.debug(f'#### args: {args}')
    return sql, args


def build_update(update_wrapper) -> (str, tuple):
    sql = f'UPDATE {update_wrapper.table} SET {",".join([f"{k}=?" for k in update_wrapper.update_fields.keys()])}'
    args = tuple(update_wrapper.update_fields.values())
    if len(update_wrapper.condition_tree.conditions) > 0:
        exp, args2 = update_wrapper.condition_tree.parse()
        sql += ' WHERE ' + exp
        args += args2
    logger.debug(f'#### sql: {sql}')
    logger.debug(f'#### args: {args}')

    if len(args) == 0:
        raise ValueError('不支持全量更新')
    return sql, args


def build_delete(update_wrapper) -> (str, tuple):
    sql = f'DELETE FROM {update_wrapper.table}'
    args = ()
    if len(update_wrapper.condition_tree.conditions) > 0:
        exp, args = update_wrapper.condition_tree.parse()
        sql += ' WHERE ' + exp
    logger.debug(f'#### sql: {sql}')
    logger.debug(f'#### args: {args}')

    if len(args) == 0:
        raise ValueError('不支持全量更新')
    return sql, args


def build_insert(insert_wrapper, data, duplicated_key_update=False, duplicated_key_ignore=False) -> (str, tuple):
    sql = f'INSERT {"OR IGNORE" if duplicated_key_ignore else ""} INTO {insert_wrapper.table} ({",".join([field.name for field in insert_wrapper.insert_fields])}) VALUES ({",".join(["?" for _ in insert_wrapper.insert_fields])})'
    if duplicated_key_update:
        sql += f' ON DUPLICATE KEY UPDATE {",".join([f"{field.name}=?" for field in insert_wrapper.insert_fields])}'
        args = tuple([getattr(data, field.name) for field in insert_wrapper.insert_fields]) * 2
    else:
        args = tuple([getattr(data, field.name) for field in insert_wrapper.insert_fields])
    logger.debug(f'#### sql: {sql}')
    logger.debug(f'#### args: {args}')
    return sql, args


def build_insert_bulk(insert_wrapper, data_list, duplicated_key_update=False, duplicated_key_ignore=False) -> (
        str, list[tuple]):
    sql = f'INSERT {"OR IGNORE" if duplicated_key_ignore else ""} INTO {insert_wrapper.table} ({",".join([field.name for field in insert_wrapper.insert_fields])}) VALUES ({",".join(["?" for _ in insert_wrapper.insert_fields])})'
    if duplicated_key_update:
        sql += f' ON DUPLICATE KEY UPDATE {",".join([f"{field.name}=?" for field in insert_wrapper.insert_fields])}'

    if duplicated_key_update:
        args = [tuple([getattr(data, f.name) for f in insert_wrapper.insert_fields]) * 2 for data in data_list]
    else:
        args = [tuple([getattr(data, f.name) for f in insert_wrapper.insert_fields]) for data in data_list]
    logger.debug(f'#### sql: {sql}')
    logger.debug(f'#### args: {args}')
    return sql, args
