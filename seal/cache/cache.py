from ..wrapper.singleton import singleton


@singleton
class Cache:
    def __init__(self):
        self.__container = {}

    def get(self, key):
        return self.__container.get(key)

    def set(self, key, value):
        self.__container[key] = value
