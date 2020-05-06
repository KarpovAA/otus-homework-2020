import hashlib
import unittest
import random
import time
from unittest.mock import MagicMock
from lib.store import Storage, RedisStorage, MAX_RETRIES_RECONNECT, TIME_DELAY_TO_RECONNECT


class TestStore(unittest.TestCase):

    def test_storage_check_set_get(self):
        redis_storage = RedisStorage()
        storage = Storage(redis_storage)
        tmp_key = hashlib.sha512(str(random.random()).encode()).hexdigest()
        self.assertEqual(storage.set(tmp_key, tmp_key), None)
        self.assertEqual(storage.get(tmp_key), tmp_key)

    def test_storage_check_cache_set_get(self):
        redis_storage = RedisStorage()
        storage = Storage(redis_storage)
        tmp_key = hashlib.sha512(str(random.random()).encode()).hexdigest()
        self.assertEqual(storage.cache_set(tmp_key, tmp_key, time_expires=5), None)
        self.assertEqual(storage.cache_get(tmp_key), tmp_key)
        time.sleep(5)
        self.assertEqual(storage.cache_get(tmp_key), None)

    def test_retry_on_connection_error(self):
        redis_storage = RedisStorage()
        redis_storage.reconnect()
        redis_storage.db.get = MagicMock(side_effect=ConnectionError())
        redis_storage.db.set = MagicMock(side_effect=ConnectionError())
        redis_storage.reconnect()
        storage = Storage(redis_storage)
        self.assertEqual(storage.cache_get("key"), None)
        self.assertEqual(storage.cache_set("key", "value"), None)
        self.assertEqual(redis_storage.db.get.call_count, MAX_RETRIES_RECONNECT)
        self.assertEqual(redis_storage.db.set.call_count, MAX_RETRIES_RECONNECT)


if __name__ == "__main__":
    unittest.main()
