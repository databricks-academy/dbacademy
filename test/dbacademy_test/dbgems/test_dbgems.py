import unittest


class MyTestCase(unittest.TestCase):

    def test_spark_conf_get(self):
        from dbacademy import dbgems

        value = dbgems.get_spark_config("some.random.value")
        self.assertIsNone(value)

        value = dbgems.get_spark_config("some.random.value", "whatever")
        self.assertEquals("whatever", value)

        dbgems.MOCK_CONFIG["some.random.value"] = "apples"
        value = dbgems.get_spark_config("some.random.value")
        self.assertEquals("apples", value)


if __name__ == '__main__':
    unittest.main()
