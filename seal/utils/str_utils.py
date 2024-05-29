import re


def camel_case(s):
    return ''.join([i.capitalize() for i in s.split('_')])


def snake_case(s):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', s).lower()
