import unittest


class TestUtils(unittest.TestCase):
    from dbacademy.common import deprecated

    @deprecated(reason="Because I'm bored")
    def add(self, value_a, value_b):
        return value_a+value_b

    def test_decorator(self):
        result = self.add(1, 3)
        self.assertEqual(result, 4)


if __name__ == '__main__':
    unittest.main()
