import unittest


class MyTestCase(unittest.TestCase):

    def test_lookup_github_tags(self):
        from dbacademy import dbgems

        tags = dbgems.lookup_all_module_versions("dbacademy-gems")
        self.assertEqual(['1.0.0', '1.1.0', '1.1.1', '1.1.2', '1.1.3', '1.1.4', '1.1.5', '1.1.6', '1.1.7', '1.1.8'], tags[:10])

        tags = dbgems.lookup_all_module_versions("dbacademy-rest")
        self.assertEqual(['1.0.0', '1.1.0', '1.1.1'], tags)

        tags = dbgems.lookup_all_module_versions("dbacademy-helper")
        self.assertEqual(['1.1.0', '1.1.1', '1.1.2', '2.0.0', '2.0.1'], tags)


if __name__ == '__main__':
    unittest.main()
