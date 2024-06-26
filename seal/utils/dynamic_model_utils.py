def to_dict(record) -> dict:
    return {k: v for k, v in record._asdict().items() if v is not None}


def get_attr(record, attr):
    if isinstance(record, dict):
        return record[attr]
    return getattr(record, attr, None)
