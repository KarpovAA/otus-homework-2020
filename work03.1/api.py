#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import datetime
import logging
import hashlib
import uuid
from lib.store import Storage, RedisStorage
from lib.scoring import get_interests, get_score
from optparse import OptionParser
from http.server import HTTPServer, BaseHTTPRequestHandler
from weakref import WeakKeyDictionary


SALT = "Otus"
ADMIN_LOGIN = "admin"
ADMIN_SALT = "42"
OK = 200
BAD_REQUEST = 400
FORBIDDEN = 403
NOT_FOUND = 404
INVALID_REQUEST = 422
INTERNAL_ERROR = 500
ERRORS = {
    BAD_REQUEST: "Bad Request",
    FORBIDDEN: "Forbidden",
    NOT_FOUND: "Not Found",
    INVALID_REQUEST: "Invalid Request",
    INTERNAL_ERROR: "Internal Server Error",
}
UNKNOWN = 0
MALE = 1
FEMALE = 2
GENDERS = {
    UNKNOWN: "unknown",
    MALE: "male",
    FEMALE: "female",
}


class Field(object):
    def __init__(self, required=False, nullable=False):
        self.required = required
        self.nullable = nullable
        self._data = WeakKeyDictionary()

    def __get__(self, instance, owner):
        return self._data.get(instance)

    def __set__(self, instance, value):
        self.validate(value)
        self._data[instance] = value

    def validate(self, value):
        if value is None and self.required:
            raise ValueError("Обязательное поле")
        if not value and not self.nullable:
            raise ValueError("Поле не может быть пустым")
        if value is not None:
            self.check_value(value)
        return value

    def check_value(self, value):
        pass


class CharField(Field):
    def check_value(self, value):
        if value is not None and not isinstance(value, str):
            raise TypeError("Поле должно быть строкой")

    def __get__(self, instance, owner):
        value = super().__get__(instance, owner)
        if not value:
            value = ''
        return value


class ArgumentsField(Field):
    def check_value(self, value):
        if value is not None and not isinstance(value, dict):
            raise TypeError("Поле должно быть словарем")


class EmailField(CharField):
    def check_value(self, value):
        super().check_value(value)
        if "@" not in value:
            raise ValueError("Ошибка в адресе электронной почты")


class PhoneField(Field):
    def check_value(self, value):
        if not value:
            return
        if not isinstance(value, (str, int)):
            raise TypeError("Поле должно быть числом или строкой")
        if isinstance(value, int):
            value = str(value)
        else:
            try:
                int(value)
            except ValueError:
                raise ValueError("Поле должно содержать только цифры")
        if not value.startswith("7") or len(value) != 11:
            raise ValueError("Неверно указан номер телефона")


class DateField(CharField):
    def check_value(self, value):
        if value:
            try:
                res = self.strftime(value)
            except ValueError:
                raise ValueError("Формат даты должен быть: DD.MM.YYYY")
            return res

    def __get__(self, instance, owner):
        value = super().__get__(instance, owner)
        if not value:
            return None
        return self.strftime(value, "%d.%m.%Y")

    def strftime(self, value, date_format="%d.%m.%Y"):
        return datetime.datetime.strptime(value, date_format).date()


class BirthDayField(DateField):
    def check_value(self, value):
        super().check_value(value)
        if value:
            birthday = self.strftime(value, "%d.%m.%Y")
            year_limit = datetime.date.today() - datetime.timedelta(days=365.25*70)
            if year_limit > birthday:
                raise ValueError("Ввозраст должен быть не старше 70 лет")


class GenderField(Field):
    def check_value(self, value):
        if value is not None and not isinstance(value, int):
            raise TypeError("Поле должно быть целым положительным числом")
        if value not in GENDERS:
            raise ValueError("Поле должно быть задано значеними: 0, 1 или 2")


class ClientIDsField(Field):
    def check_value(self, value):
        if value is not None:
            if not isinstance(value, list) or not all(isinstance(v, int) for v in value):
                raise TypeError("Поле должно содержать список целых чисел")
        if not all(v >= 0 for v in value):
            raise ValueError("Поле должно положительное значение")


class RequestMeta(type):
    def __new__(cls, name, bases, attr):
        fields = {
            filed_name: field
            for filed_name, field in attr.items()
            if isinstance(field, Field)
        }
        new_attr = attr.copy()
        new_attr["_attr_fields"] = fields
        return super().__new__(cls, name, bases, new_attr)


class Request(metaclass=RequestMeta):
    def __init__(self, request=None):
        self.empty_values = (None, '', [], (), {})
        self.data = {} if not request else request
        self._errors = None
        self.non_empty_fields = []

    @property
    def errors(self):
        if self._errors is None:
            self.validate()
        return self._errors

    def validate(self):
        self._errors = {}
        for name, field in self._attr_fields.items():
            try:
                value = self.data.get(name)
                self.__setattr__(name, value)
                if value not in self.empty_values:
                    self.non_empty_fields.append(name)
            except (TypeError, ValueError) as e:
                self._errors[name] = str(e)

    def is_valid(self):
        return not self.errors


class MethodRequest(Request):
    account = CharField(required=False, nullable=True)
    login = CharField(required=True, nullable=True)
    token = CharField(required=True, nullable=True)
    method = CharField(required=True, nullable=False)
    arguments = ArgumentsField(required=True, nullable=True)

    @property
    def is_admin(self):
        return self.login == ADMIN_LOGIN


class OnlineScoreRequest(Request):
    first_name = CharField(required=False, nullable=True)
    last_name = CharField(required=False, nullable=True)
    email = EmailField(required=False, nullable=True)
    phone = PhoneField(required=False, nullable=True)
    birthday = BirthDayField(required=False, nullable=True)
    gender = GenderField(required=False, nullable=True)

    def validate(self):
        super().validate()
        if not self.errors:
            if self.phone and self.email:
                return
            if self.first_name and self.last_name:
                return
            if self.gender is not None and self.birthday:
                return
            self.errors["arguments"] = "Неверный список аргументов"


class ClientsInterestsRequest(Request):
    client_ids = ClientIDsField(required=True, nullable=False)
    date = DateField(required=False, nullable=True)


class OnlineScoreHandler(object):
    def execute_request(self, request, context, store):
        r = OnlineScoreRequest(request.arguments)
        if not r.is_valid():
            return r.errors, INVALID_REQUEST
        if request.is_admin:
            score = 42
        else:
            score = get_score(store, r.phone, r.email, r.birthday, r.gender, r.first_name, r.last_name)
        context["has"] = r.non_empty_fields
        return {"score": score}, OK
    pass


class ClientsInterestsHandler(object):
    def execute_request(self, request, context, store):
        r = ClientsInterestsRequest(request.arguments)
        if not r.is_valid():
            return r.errors, INVALID_REQUEST
        context["nclients"] = len(r.client_ids)
        response_body = {c_id: get_interests(store, c_id) for c_id in r.client_ids}
        return response_body, OK
    pass


def check_auth(request):
    if request.is_admin:
        digest = hashlib.sha512((datetime.datetime.now().strftime("%Y%m%d%H") + ADMIN_SALT).encode()).hexdigest()
    else:
        digest = hashlib.sha512((request.account + request.login + SALT).encode()).hexdigest()
    if digest == request.token:
        return True
    return False


def method_handler(request, ctx, store):
    handlers = {
        "online_score": OnlineScoreHandler,
        "clients_interests": ClientsInterestsHandler
    }
    method_request = MethodRequest(request["body"])
    if not method_request.is_valid():
        return method_request.errors, INVALID_REQUEST
    if not check_auth(method_request):
        return "Forbidden", FORBIDDEN
    handler = handlers[method_request.method]()
    response = handler.execute_request(method_request, ctx, store)
    return response


class MainHTTPHandler(BaseHTTPRequestHandler):
    router = {
        "method": method_handler
    }
    store = Storage(RedisStorage(host='172.17.0.2'))

    def get_request_id(self, headers):
        return headers.get('HTTP_X_REQUEST_ID', uuid.uuid4().hex)

    def do_POST(self):
        response, code = {}, OK
        context = {"request_id": self.get_request_id(self.headers)}
        request = None
        data_string = ''

        try:
            data_string = self.rfile.read(int(self.headers['Content-Length']))
            data_string = data_string.decode('UTF-8')
            request = json.loads(data_string)
        except:
            code = BAD_REQUEST

        if request:
            path = self.path.strip("/")
            logging.info("%s: %s %s" % (self.path, data_string, context["request_id"]))
            code = NOT_FOUND
            if path in self.router:
                try:
                    response, code = self.router[path]({"body": request, "headers": self.headers}, context, self.store)
                except Exception as e:
                    logging.exception("Unexpected error: %s" % e)
                    code = INTERNAL_ERROR

        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()

        if code not in ERRORS:
            r = {"response": response, "code": code}
        else:
            r = {"error": response or ERRORS.get(code, "Unknown Error"), "code": code}

        context.update(r)
        logging.info(context)
        self.wfile.write(json.dumps(r, ensure_ascii=False).encode(encoding='UTF-8'))
        return


if __name__ == "__main__":
    op = OptionParser()
    op.add_option("-p", "--port", action="store", type=int, default=8080)
    op.add_option("-l", "--log", action="store", default=None)
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    server = HTTPServer(("localhost", opts.port), MainHTTPHandler)
    logging.info("Starting server at %s" % opts.port)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    server.server_close()
