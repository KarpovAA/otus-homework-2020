import redis
import functools
import time


MAX_RETRIES_RECONNECT = 5
TIME_DELAY_TO_RECONNECT = 1     # ms


def retry(exceptions, retries=MAX_RETRIES_RECONNECT, time_delay=TIME_DELAY_TO_RECONNECT):
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            for n in range(retries):
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    time.sleep(time_delay)
        return wrapper
    return decorator


class RedisStorage:
    def __init__(self, host='172.17.0.2', port='6379', timeout=5):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.db = None
        self.reconnect()

    def reconnect(self):
        self.db = redis.StrictRedis(
            host=self.host,
            port=self.port,
            db=0,
            socket_timeout=self.timeout,
            socket_connect_timeout=self.timeout,
            decode_responses=True
        )

    def get(self, key):
        try:
            res = self.db.get(key)
        except redis.exceptions.TimeoutError:
            raise TimeoutError
        except redis.exceptions.ConnectionError:
            raise ConnectionError
        return res

    def set(self, key, value, expires=None):
        try:
            self.db.set(key, value, ex=expires)
        except redis.exceptions.TimeoutError:
            raise TimeoutError
        except redis.exceptions.ConnectionError:
            raise ConnectionError


class Storage:
    def __init__(self, storage):
        self.storage = storage

    def get(self, key):
        return self.storage.get(key)

    def set(self, key, value, time_expires=None):
        return self.storage.set(key, value, expires=time_expires)

    @retry((TimeoutError, ConnectionError))
    def cache_get(self, key):
        return self.storage.get(key)

    @retry((TimeoutError, ConnectionError))
    def cache_set(self, key, value, time_expires=60*60):
        return self.storage.set(key, value, expires=time_expires)


if __name__ == "__main__":
    pass
