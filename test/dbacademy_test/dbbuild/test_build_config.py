import unittest


class MyTestCase(unittest.TestCase):

    def test_load(self):
        from dbacademy_courseware.dbbuild.build_config_class import BuildConfig
        config = {
            "name": "Test Suite",
            "notebook_config": {},
            "publish_only": {}
        }
        bc = BuildConfig.load_config(config, "1.2.3")

        self.assertIsNotNone(bc)


if __name__ == '__main__':
    unittest.main()
