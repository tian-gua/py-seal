def group(list_, key_func: callable, value_func: callable = None):
    """
    Group a list of dictionaries by a key.
    """
    grouped = {}
    for item in list_:
        key = key_func(item)
        if key not in grouped:
            grouped[key] = []
        if value_func:
            grouped[key].append(value_func(item))
        else:
            grouped[key].append(item)
    return grouped
