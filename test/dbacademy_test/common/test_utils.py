import unittest

import dbacademy.common


class TestUtils(unittest.TestCase):
    from dbacademy.common import deprecated

    @deprecated(reason="Because I'm bored", action="ignore")
    def add(self, value_a, value_b):
        return value_a + value_b

    @deprecated(reason="Because I'm bored", action="warn")
    def add_warn(self, value_a, value_b):
        return value_a+value_b

    @deprecated(reason="Because I'm bored", action="error")
    def add_error(self, value_a, value_b):
        return value_a + value_b

    def test_deprecated_ignore(self):
        result = self.add(1, 3)
        self.assertEqual(result, 4)

    def test_deprecated_warn(self):
        if dbacademy.common.deprecation_log_level == "error":
            self.test_decorator_error()
        else:
            self.test_deprecated_ignore()

    def test_decorator_error(self):
        if dbacademy.common.deprecation_log_level == "ignore":
            self.test_deprecated_ignore()
            return
        try:
            self.add_error(1, 3)
            self.fail("DeprecationWarning exception expected.")
        except DeprecationWarning:
            pass

    def test_validate_type(self):

        def dummy(value: int):
            from dbacademy.common import validate_type
            value = validate_type(value, "value", int)
            self.assertEqual(1, value)

        dummy(1)

        try:
            # noinspection PyTypeChecker
            dummy("1")
        except AssertionError as e:
            self.assertEqual("""Expected the parameter "value" to be of type <class 'int'>, found <class 'str'>""", str(e))


if __name__ == '__main__':
    unittest.main()
