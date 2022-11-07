import unittest


class MyTestCase(unittest.TestCase):

    def test_step_2_load_config(self):
        from dbacademy.dbbuild.build_config_class import BuildConfig

        config = {
            "name": "Some Random Unit Test",
            "source_repo": "/Repos/Examples/example-course-source"
        }
        self.build_config = BuildConfig.load_config(config, version="0.0.0")
        self.assertIsNotNone(self.build_config)

        try:
            self.build_config.validate_all_tests_passed("TES")
        except AssertionError as e:
            self.assertEqual("Cannot validate smoke-tests until the build configuration passes validation. See BuildConfig.validate()", str(e))

        # self.build_config.validate()
        # self.build_config.validate_all_tests_passed("TES")


if __name__ == '__main__':
    unittest.main()
