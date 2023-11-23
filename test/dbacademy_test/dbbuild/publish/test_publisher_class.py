import unittest


class MyTestCase(unittest.TestCase):

    def test_step_2_load_config(self):
        from dbacademy.dbbuild import create_build_config

        config = {
            "name": "Some Random Unit Test",
            "source_repo": "/Repos/Examples/example-course-source"
        }
        build_config = create_build_config(config, version="0.0.0")
        self.assertIsNotNone(build_config)

        try:
            build_config.validate_all_tests_passed("TES")
        except AssertionError as e:
            self.assertEqual("Cannot validate smoke-tests until the build configuration passes validation. See BuildConfig.validate()", str(e))


if __name__ == '__main__':
    unittest.main()
