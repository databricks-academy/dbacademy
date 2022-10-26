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
            result = self.add_error(1, 3)
            self.fail("DeprecationWarning exception expected.")
        except DeprecationWarning:
            pass


if __name__ == '__main__':
    unittest.main()
