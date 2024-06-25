def to_dict(record) -> dict:
    return {k: v for k, v in record._asdict().items() if v is not None}
