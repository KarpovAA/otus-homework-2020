import redis
import functools
import time
from functools import lru_cache

MAX_RETRIES_RECONNECT = 3
TIME_DELAY_TO_RECONNECT = 1     # ms
MAX_CACHE_SIZE = 256


def retry(exceptions, retries=MAX_RETRIES_RECONNECT, time_delay=TIME_DELAY_TO_RECONNECT):
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            for n in range(retries):
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    time.sleep(time_delay)
                    if n == retries:
                        raise ConnectionError
        return wrapper
    return decorator


class RedisStorage:
    def __init__(self, host='172.17.0.2', port='6379', timeout=1):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.db = None

    def reconnect(self):
        try:
            if not self.db:
                self.db = redis.StrictRedis(
                    host=self.host,
                    port=self.port,
                    db=0,
                    socket_timeout=self.timeout,
                    socket_connect_timeout=self.timeout,
                    decode_responses=True
                )
        except Exception as e:
            raise ConnectionError

    def get(self, key):
        if not self.db:
            self.reconnect()
        try:
            res = self.db.get(key)
        except redis.exceptions.TimeoutError:
            raise TimeoutError
        except redis.exceptions.ConnectionError:
            raise ConnectionError
        except Exception as e:
            raise e
        return res

    def set(self, key, value, expires=None):
        if not self.db:
            self.reconnect()
        try:
            self.db.set(key, value, ex=expires)
        except redis.exceptions.TimeoutError:
            raise TimeoutError
        except redis.exceptions.ConnectionError:
            raise ConnectionError


class Storage:
    def __init__(self, storage):
        self.storage = storage

    @retry((TimeoutError, ConnectionError))
    def get(self, key):
        return self.storage.get(key)

    @retry((TimeoutError, ConnectionError))
    def set(self, key, value, time_expires=None):
        return self.storage.set(key, value, expires=time_expires)

    @lru_cache(maxsize=MAX_CACHE_SIZE)
    @retry((TimeoutError, ConnectionError))
    def cache_get(self, key):
        try:
            result_get = self.storage.get(key)
        except Exception:
            result_get = None
        return result_get

    @retry((TimeoutError, ConnectionError))
    def cache_set(self, key, value, time_expires=60*60):
        return self.storage.set(key, value, expires=time_expires)


if __name__ == "__main__":
    pass
