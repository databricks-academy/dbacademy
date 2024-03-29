__all__ = ["CourseConfigTests"]

import unittest
from dbacademy.dbhelper.course_config import CourseConfig


class CourseConfigTests(unittest.TestCase):

    def test_course_config_supported_dbrs_empty(self):
        from dbacademy.common import ValidationError

        try:
            CourseConfig(course_code="test",
                         course_name="Some Unit Test",
                         # data_source_name="some-unit-test",
                         data_source_version="v03",
                         install_min_time="5 min",
                         install_max_time="25 min",
                         supported_dbrs=[],
                         expected_dbrs="{{supported_dbrs}}")
            raise Exception("Expected Assertion Error")

        except ValidationError as e:
            self.assertEqual("Error-Min-Len | The parameter 'supported_dbrs' must have a minimum length of 1, found 0.", e.message)

    def test_course_config_supported_dbrs_length(self):
        try:
            CourseConfig(course_code="test",
                         course_name="Some Unit Test",
                         data_source_version="v03",
                         install_min_time="5 min",
                         install_max_time="25 min",
                         supported_dbrs=["version-1", "version-2"],
                         expected_dbrs="version-1, version-2,version-3")
            raise Exception("Expected Assertion Error")

        except AssertionError as e:
            self.assertEqual("The supported and expected list of DBRs does not match: 2 (supported) vs 3 (expected)", str(e))

    def test_course_config_supported_dbrs_mismatch(self):
        try:
            CourseConfig(course_code="test",
                         course_name="Some Unit Test",
                         # data_source_name="some-unit-test",
                         data_source_version="v03",
                         install_min_time="5 min",
                         install_max_time="25 min",
                         supported_dbrs=["version-1", "version-2", "version-A"],
                         expected_dbrs="version-1, version-2, version-3")
            raise Exception("Expected Assertion Error")

        except AssertionError as e:
            self.assertEqual("The supported DBR \"version-A\" was not find in the list of expected dbrs: ['version-1', 'version-2', 'version-3']", str(e))

    def test_course_config_no_expected_dbrs(self):
        config = CourseConfig(course_code="test",
                              course_name="Some Unit Test",
                              data_source_version="v03",
                              install_min_time="5 min",
                              install_max_time="25 min",
                              supported_dbrs=["spark-a", "spark-b"],
                              expected_dbrs="{{supported_dbrs}}")

        self.assertEqual("test", config.course_code)
        self.assertEqual("Some Unit Test", config.course_name)
        self.assertEqual("some-unit-test", config.build_name)
        self.assertEqual("some-unit-test", config.data_source_name)
        self.assertEqual("v03", config.data_source_version)
        self.assertEqual("5 min", config.install_min_time)
        self.assertEqual("25 min", config.install_max_time)
        self.assertEqual(["spark-a", "spark-b"], config.supported_dbrs)
        self.assertEqual("{{supported_dbrs}}", config.expected_dbrs)

    def test_course_config_supported_dbrs(self):
        config = CourseConfig(course_code="test",
                              course_name="Some Unit Test",
                              # data_source_name="some-unit-test",
                              data_source_version="v03",
                              install_min_time="5 min",
                              install_max_time="25 min",
                              supported_dbrs=["version-1", "version-2", "version-3"],
                              expected_dbrs="version-1, version-2,version-3")

        self.assertEqual("test", config.course_code)
        self.assertEqual("Some Unit Test", config.course_name)
        self.assertEqual("some-unit-test", config.build_name)
        self.assertEqual("some-unit-test", config.data_source_name)
        self.assertEqual("v03", config.data_source_version)
        self.assertEqual("5 min", config.install_min_time)
        self.assertEqual("25 min", config.install_max_time)
        self.assertEqual(["version-1", "version-2", "version-3"], config.supported_dbrs)
        self.assertEqual("version-1, version-2,version-3", config.expected_dbrs)

    def test_to_build_name(self):
        name = CourseConfig.to_build_name("Testing Courses, Part 1 & 2")
        self.assertEqual("testing-courses-part-1-2", name)

        self.assertIsNone(CourseConfig.to_build_name(None))


if __name__ == '__main__':
    unittest.main()
