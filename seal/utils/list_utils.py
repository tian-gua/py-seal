def group(lst, key):
    """
    Group a list of dictionaries by a key.
    """
    grouped = {}
    for item in lst:
        if isinstance(item, dict):
            val = item[key]
        else:
            val = getattr(item, key)
        if item[key] not in grouped:
            grouped[val] = []
        grouped[val].append(item)
    return grouped
