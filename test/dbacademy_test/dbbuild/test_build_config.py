import unittest


class TestBuildConfig(unittest.TestCase):

    def test_load(self):
        from dbacademy.dbbuild.build_config_class import BuildConfig

        config = {
            "name": "Test Suite",
            "notebook_config": {},
            "publish_only": None
        }
        bc = BuildConfig.load_config(config, "1.2.3")

        self.assertIsNotNone(bc)
        self.assertIsNotNone(self)


if __name__ == '__main__':
    unittest.main()
