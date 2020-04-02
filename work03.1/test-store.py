import unittest
from unittest.mock import patch, MagicMock
import fakeredis
import store


class TestStore(unittest.TestCase):

    def test_raise_connection_error(self):
        redis_storage = store.RedisStorage("")
        storage = store.Storage(redis_storage)
        with self.assertRaises(ConnectionError):
            storage.get("key")

    @patch("redis.StrictRedis", fakeredis.FakeStrictRedis)
    def test_storage_get(self):
        redis_storage = store.RedisStorage()
        storage = store.Storage(redis_storage)
        storage.set("get_key", "2")
        self.assertEqual(storage.get("get_key"), "2")

    @patch("redis.StrictRedis", fakeredis.FakeStrictRedis)
    def test_storage_cache_get(self):
        redis_storage = store.RedisStorage()
        storage = store.Storage(redis_storage)
        storage.cache_set("key", "1")
        self.assertEqual(storage.cache_get("key"), "1")

    @patch("redis.StrictRedis", fakeredis.FakeStrictRedis)
    def test_retry_on_connection_error(self):
        redis_storage = store.RedisStorage()
        redis_storage.db.connected = False
        redis_storage.db.get = MagicMock(side_effect=ConnectionError())
        redis_storage.db.set = MagicMock(side_effect=ConnectionError())
        storage = store.Storage(redis_storage)
        self.assertEqual(storage.cache_get("key"), None)
        self.assertEqual(storage.cache_set("key", "value"), None)
        self.assertEqual(redis_storage.db.get.call_count, store.MAX_RETRIES_RECONNECT)
        self.assertEqual(redis_storage.db.set.call_count, store.MAX_RETRIES_RECONNECT)


if __name__ == "__main__":
    unittest.main()