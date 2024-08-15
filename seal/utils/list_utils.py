from typing import List, Callable, Any


def group(list_: List[Any], key_func: Callable, value_func: Callable = None):
    """
    Group a list of data by a key.
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


def merge(list_: List[Any], merge_func: Callable):
    """
    Merge a list of data by a key.
    """
    merged: Any = None
    for item in list_:
        # if merged is None:
        #     merged = item
        #     continue
        merged = merge_func(merged, item)
    return merged
