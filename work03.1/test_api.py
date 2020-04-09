import hashlib
import datetime
import functools
import unittest
import random
import api
from unittest.mock import MagicMock
from lib.store import Storage, RedisStorage, MAX_RETRIES_RECONNECT, TIME_DELAY_TO_RECONNECT


def cases(cases):
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args):
            for c in cases:
                new_args = args + (c if isinstance(c, tuple) else (c,))
                try:
                    f(*new_args)
                except Exception as e:
                    print('Error in case: %s', new_args)
                    raise e
        return wrapper
    return decorator


class TestCharField(unittest.TestCase):

    @cases(['test', '', None, 10])
    def test_valid_value(self, value):
        self.assertEqual(value, api.CharField(nullable=True).validate(value))

    @cases([10, {'test': None}])
    def test_invalid_type_value(self, value):
        with self.assertRaises(TypeError):
            api.CharField(nullable=True).validate(value)

    @cases(['', None])
    def test_invalid_value(self, value):
        with self.assertRaises(ValueError):
            api.CharField(nullable=False).validate(value)


class TestArgumentsField(unittest.TestCase):

    @cases([{'test': None}, {}, None])
    def test_valid_value(self, value):
        self.assertEqual(value, api.ArgumentsField(nullable=True).validate(value))

    @cases([10, 'test'])
    def test_invalid_type_value(self, value):
        with self.assertRaises(TypeError):
            api.ArgumentsField(nullable=True).validate(value)

    @cases(['', None])
    def test_invalid_value(self, value):
        with self.assertRaises(ValueError):
            api.ArgumentsField(nullable=False).validate(value)


class TestEmailField(TestCharField):

    @cases(['user@example.com', '@'])
    def test_valid_email_address(self, value):
        self.assertEqual(value, api.EmailField(nullable=True).validate(value))

    @cases([10, {'test': None}])
    def test_invalid_type_value(self, value):
        with self.assertRaises(TypeError):
            api.EmailField(nullable=True).validate(value)

    @cases([None, '', 'user'])
    def test_invalid_value(self, value):
        with self.assertRaises(ValueError):
            api.EmailField(nullable=False).validate(value)


class TestPhoneField(unittest.TestCase):

    @cases([79991234567, '79991234567', '', None])
    def test_valid_value(self, value):
        self.assertEqual(value, api.PhoneField(nullable=True).validate(value))

    @cases([7.9991234567, {'test': None}])
    def test_invalid_type_value(self, value):
        with self.assertRaises(TypeError):
            api.PhoneField(nullable=True).validate(value)

    @cases([None, '', '7999123456', '9991234567', 7999123456, 9991234567, '7abcdefghij'])
    def test_invalid_value(self, value):
        with self.assertRaises(ValueError):
            api.PhoneField(nullable=False).validate(value)


class TestDateField(unittest.TestCase):

    @cases(['21.09.2018'])
    def test_valid_value(self, value):
        self.assertEqual(value, api.DateField(nullable=True).validate(value))

    @cases([7.9991234567, {'test': None}])
    def test_invalid_type_value(self, value):
        with self.assertRaises(TypeError):
            api.DateField(nullable=True).validate(value)

    @cases([None, '', '7abcdefghij'])
    def test_invalid_value(self, value):
        with self.assertRaises(ValueError):
            print(api.DateField(nullable=False).validate(value))


class TestBirthDayField(unittest.TestCase):

    @cases(['21.09.2018', datetime.datetime.today().date().strftime("%d.%m.%Y")])
    def test_valid_value(self, value):
        self.assertEqual(value, api.BirthDayField(nullable=True).validate(str(value)))

    @cases([7.9991234567, 21092018, {'test': None}])
    def test_invalid_type_value(self, value):
        with self.assertRaises(TypeError):
            api.BirthDayField(nullable=True).validate(value)

    @cases([None, '', '7abcdefghij'])
    def test_invalid_value(self, value):
        with self.assertRaises(ValueError):
            api.BirthDayField(nullable=False).validate(value)

    @cases([datetime.datetime.today().date() - datetime.timedelta(days=(365.25*70+1))])
    def test_invalid_birthday(self, value):
        value = value.strftime("%d.%m.%Y")
        with self.assertRaises(ValueError):
            api.BirthDayField(nullable=False).validate(value)


class TestGenderField(unittest.TestCase):

    @cases([0, 1, 2, None])
    def test_valid_value(self, value):
        self.assertEqual(value, api.GenderField(nullable=True).validate(value))

    @cases(['0'])
    def test_invalid_type_value(self, value):
        with self.assertRaises(TypeError):
            api.GenderField(nullable=True).validate(value)

    @cases([-1, 3, None])
    def test_invalid_value(self, value):
        with self.assertRaises(ValueError):
            api.GenderField(nullable=False).validate(value)


class TestClientIDsField(unittest.TestCase):

    @cases([[0, 1, 2], [], None])
    def test_valid_value(self, value):
        self.assertEqual(value, api.ClientIDsField(nullable=True).validate(value))

    @cases([[None], ''])
    def test_invalid_type_value(self, value):
        with self.assertRaises(TypeError):
            api.ClientIDsField(nullable=True).validate(value)

    @cases([[0, -1, 2], None])
    def test_invalid_value(self, value):
        with self.assertRaises(ValueError):
            api.ClientIDsField(nullable=False).validate(value)


class TestSuite(unittest.TestCase):
    def setUp(self):
        self.context = {}
        self.headers = {}
        self.settings = Storage(RedisStorage())

    def get_response(self, request):
        return api.method_handler({"body": request, "headers": self.headers}, self.context, self.settings)

    def set_valid_auth(self, request):
        if request.get("login") == api.ADMIN_LOGIN:
            request["token"] = hashlib.sha512((datetime.datetime.now().strftime("%Y%m%d%H") + api.ADMIN_SALT).encode()).hexdigest()
        else:
            msg = request.get("account", "") + request.get("login", "") + api.SALT
            request["token"] = hashlib.sha512(msg.encode()).hexdigest()

    def test_empty_request(self):
        _, code = self.get_response({})
        self.assertEqual(api.INVALID_REQUEST, code)

    @cases([
        {"account": "horns&hoofs", "login": "h&f", "method": "online_score", "token": "", "arguments": {}},
        {"account": "horns&hoofs", "login": "h&f", "method": "online_score", "token": "sdd", "arguments": {}},
        {"account": "horns&hoofs", "login": "admin", "method": "online_score", "token": "", "arguments": {}},
    ])
    def test_bad_auth(self, request):
        _, code = self.get_response(request)
        self.assertEqual(api.FORBIDDEN, code)

    @cases([
        {"account": "horns&hoofs", "login": "h&f", "method": "online_score"},
        {"account": "horns&hoofs", "login": "h&f", "arguments": {}},
        {"account": "horns&hoofs", "method": "online_score", "arguments": {}},
    ])
    def test_invalid_method_request(self, request):
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.INVALID_REQUEST, code)
        self.assertTrue(len(response))

    @cases([
        {},
        {"phone": "79175002040"},
        {"phone": "89175002040", "email": "stupnikov@otus.ru"},
        {"phone": "79175002040", "email": "stupnikovotus.ru"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": -1},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": "1"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": 1, "birthday": "01.01.1890"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": 1, "birthday": "XXX"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": 1, "birthday": "01.01.2000", "first_name": 1},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": 1, "birthday": "01.01.2000",
         "first_name": "s", "last_name": 2},
        {"phone": "79175002040", "birthday": "01.01.2000", "first_name": "s"},
        {"email": "stupnikov@otus.ru", "gender": 1, "last_name": 2},
    ])
    def test_invalid_score_request(self, arguments):
        request = {"account": "horns&hoofs", "login": "h&f", "method": "online_score", "arguments": arguments}
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.INVALID_REQUEST, code, arguments)
        self.assertTrue(len(response))

    @cases([
        {"phone": "79175002040", "email": "stupnikov@otus.ru"},
        {"phone": 79175002040, "email": "stupnikov@otus.ru"},
        {"gender": 1, "birthday": "01.01.2000", "first_name": "a", "last_name": "b"},
        {"gender": 0, "birthday": "01.01.2000"},
        {"gender": 2, "birthday": "01.01.2000"},
        {"first_name": "a", "last_name": "b"},
        {"phone": "79175002040", "email": "stupnikov@otus.ru", "gender": 1, "birthday": "01.01.2000",
         "first_name": "a", "last_name": "b"},
    ])
    def test_ok_score_request(self, arguments):
        request = {"account": "horns&hoofs", "login": "h&f", "method": "online_score", "arguments": arguments}
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.OK, code, arguments)
        score = response.get("score")
        self.assertTrue(isinstance(score, (int, float)) and score >= 0, arguments)
        self.assertEqual(sorted(self.context["has"]), sorted(arguments.keys()))

    def test_ok_score_admin_request(self):
        arguments = {"phone": "79175002040", "email": "stupnikov@otus.ru"}
        request = {"account": "horns&hoofs", "login": "admin", "method": "online_score", "arguments": arguments}
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.OK, code)
        score = response.get("score")
        self.assertEqual(score, 42)

    @cases([
        {},
        {"date": "20.07.2017"},
        {"client_ids": [], "date": "20.07.2017"},
        {"client_ids": {1: 2}, "date": "20.07.2017"},
        {"client_ids": ["1", "2"], "date": "20.07.2017"},
        {"client_ids": [1, 2], "date": "XXX"},
    ])
    def test_invalid_interests_request(self, arguments):
        request = {"account": "horns&hoofs", "login": "h&f", "method": "clients_interests", "arguments": arguments}
        self.set_valid_auth(request)
        response, code = self.get_response(request)
        self.assertEqual(api.INVALID_REQUEST, code, arguments)
        self.assertTrue(len(response))

    @cases([
        {"client_ids": [1, 2, 3], "date": datetime.datetime.today().strftime("%d.%m.%Y")},
        {"client_ids": [1, 2], "date": "19.07.2017"},
        {"client_ids": [1]},
    ])
    def test_ok_interests_request(self, arguments):
        request = {"account": "horns&hoofs", "login": "h&f", "method": "clients_interests", "arguments": arguments}
        self.set_valid_auth(request)
        response, code = self.get_response(request)

        self.assertEqual(api.OK, code, arguments)
        self.assertEqual(len(arguments["client_ids"]), len(response))
        self.assertTrue(all(v and isinstance(v, list) and all(isinstance(i, str) for i in v) for v in response.values()))
        self.assertEqual(self.context.get("nclients"), len(arguments["client_ids"]))


class TestStore(unittest.TestCase):

    def test_storage_invalid_set(self):
        redis_storage = RedisStorage('')
        storage = Storage(redis_storage)
        with self.assertRaises(ConnectionError):
            storage.set("set_key", "2")

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
        self.assertEqual(storage.cache_set(tmp_key, tmp_key), None)
        self.assertEqual(storage.cache_get(tmp_key), tmp_key)

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
