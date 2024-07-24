def group(list_, key):
    """
    Group a list of dictionaries by a key.
    """
    grouped = {}
    for item in list_:
        if isinstance(item, dict):
            val = item[key]
        else:
            val = getattr(item, key)
        if val not in grouped:
            grouped[val] = []
        grouped[val].append(item)
    return grouped
