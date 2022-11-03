import unittest


class TestBuildConfig(unittest.TestCase):

    # def test_load(self):
    #     from dbacademy.dbbuild.build_config_class import BuildConfig
    #
    #     config = {
    #         "name": "Test Suite",
    #         "notebook_config": {},
    #         "publish_only": None
    #     }
    #     bc = BuildConfig.load_config(config, "1.2.3")
    #
    #     self.assertIsNotNone(bc)
    #     self.assertIsNotNone(self)

    def test_initialize_notebooks(self):
        from dbacademy.dbbuild.build_config_class import BuildConfig
        build_config = BuildConfig(name="Test Suite")

        try:
            build_config.validate()
        except AssertionError as e:
            expected = "The notebooks have not yet been initialized; Please call BuildConfig.initialize_notebooks() before proceeding."
            self.assertEqual(expected, str(e))


if __name__ == '__main__':
    unittest.main()
