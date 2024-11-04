import time


class Cache:
    def __init__(self):
        self.cache_dict = {}
        self.count = 0
        self.has_expired_key = False

    def set(self, key, value, ttl=None):
        """
        ttl unit: second
        """
        if ttl is not None:
            self.cache_dict[key] = CacheItem(key, value, expire_at=int(time.time()) + ttl)
            self.has_expired_key = True
        else:
            self.cache_dict[key] = CacheItem(key, value)

    def get(self, key):
        self.remove_expired()
        if key in self.cache_dict:
            item = self.cache_dict[key]
            if item.expire_at is not None and item.expire_at < int(time.time()):
                del self.cache_dict[key]
                return None
            return item.value
        return None

    def remove(self, key):
        if key in self.cache_dict:
            del self.cache_dict[key]

    def remove_expired(self):
        if not self.has_expired_key:
            return

        self.count += 1
        if self.count > 1000:
            self.count = 0

            will_expired_key_count = 0
            for k in list(self.cache_dict.keys()):
                item = self.cache_dict[k]
                if item.expire_at is not None:
                    will_expired_key_count += 1
                    if item.expire_at < int(time.time()):
                        del self.cache_dict[k]
            if will_expired_key_count == 0:
                self.has_expired_key = False


class CacheItem:
    def __init__(self, key, value, expire_at: int | None = None):
        self.key = key
        self.value = value
        self.expire_at = expire_at
