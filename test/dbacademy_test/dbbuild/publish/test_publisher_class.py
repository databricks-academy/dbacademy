import unittest

from dbacademy.common import Cloud


class MyTestCase(unittest.TestCase):

    def test_step_2_load_config(self):
        from dbacademy.dbbuild.build_config import BuildConfig

        build_config = BuildConfig(name="Some Random Unit Test",
                                   version="0.0.0",
                                   source_repo="/Repos/Examples/example-course-source",
                                   supported_dbrs="13.3.x-cpu-ml-scala2.12")

        try:
            build_config.validate_all_tests_passed(Cloud.AWS)
        except AssertionError as e:
            self.assertEqual("Cannot validate smoke-tests until the build configuration passes validation. See BuildConfig.validate()", str(e))


if __name__ == '__main__':
    unittest.main()
