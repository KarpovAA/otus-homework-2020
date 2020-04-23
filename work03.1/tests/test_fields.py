import datetime
import unittest
import api
from lib.test import cases


class TestCharField(unittest.TestCase):

    @cases(['test', '', None])
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


if __name__ == "__main__":
    unittest.main()
