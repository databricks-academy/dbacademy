import unittest
from dbacademy.dbhelper.course_config import CourseConfig


class MyTestCase(unittest.TestCase):

    def test_course_config_supported_dbrs_empty(self):
        try:
            CourseConfig(_course_code="test",
                         _course_name="Some Unit Test",
                         # data_source_name="some-unit-test",
                         _data_source_version="v03",
                         _install_min_time="5 min",
                         _install_max_time="25 min",
                         _supported_dbrs=[],
                         _expected_dbrs="{{supported_dbrs}}")
            raise Exception("Expected Assertion Error")

        except AssertionError as e:
            self.assertEquals("At least one supported DBR must be defined.", str(e))

    def test_course_config_supported_dbrs_length(self):
        try:
            CourseConfig(_course_code="test",
                         _course_name="Some Unit Test",
                         _data_source_version="v03",
                         _install_min_time="5 min",
                         _install_max_time="25 min",
                         _supported_dbrs=["version-1", "version-2"],
                         _expected_dbrs="version-1, version-2,version-3")
            raise Exception("Expected Assertion Error")

        except AssertionError as e:
            self.assertEquals("The supported and expected list of DBRs does not match: 2 (supported) vs 3 (expected)", str(e))

    def test_course_config_supported_dbrs_mismatch(self):
        try:
            CourseConfig(_course_code="test",
                         _course_name="Some Unit Test",
                         # data_source_name="some-unit-test",
                         _data_source_version="v03",
                         _install_min_time="5 min",
                         _install_max_time="25 min",
                         _supported_dbrs=["version-1", "version-2", "version-A"],
                         _expected_dbrs="version-1, version-2, version-3")
            raise Exception("Expected Assertion Error")

        except AssertionError as e:
            self.assertEquals("The supported DBR \"version-A\" was not find in the list of expected dbrs: ['version-1', 'version-2', 'version-3']", str(e))

    def test_course_config_no_expected_dbrs(self):
        config = CourseConfig(_course_code="test",
                              _course_name="Some Unit Test",
                              _data_source_version="v03",
                              _install_min_time="5 min",
                              _install_max_time="25 min",
                              _supported_dbrs=["spark-a", "spark-b"],
                              _expected_dbrs="{{supported_dbrs}}")

        self.assertEquals("test", config.course_code)
        self.assertEquals("Some Unit Test", config.course_name)
        self.assertEquals("some-unit-test", config.build_name)
        self.assertEquals("some-unit-test", config.data_source_name)
        self.assertEquals("v03", config.data_source_version)
        self.assertEquals("5 min", config.install_min_time)
        self.assertEquals("25 min", config.install_max_time)
        self.assertEquals(["spark-a", "spark-b"], config.supported_dbrs)
        self.assertEquals("{{supported_dbrs}}", config.expected_dbrs)

    def test_course_config_supported_dbrs(self):
        config = CourseConfig(_course_code="test",
                              _course_name="Some Unit Test",
                              # data_source_name="some-unit-test",
                              _data_source_version="v03",
                              _install_min_time="5 min",
                              _install_max_time="25 min",
                              _supported_dbrs=["version-1", "version-2", "version-3"],
                              _expected_dbrs="version-1, version-2,version-3")

        self.assertEquals("test", config.course_code)
        self.assertEquals("Some Unit Test", config.course_name)
        self.assertEquals("some-unit-test", config.build_name)
        self.assertEquals("some-unit-test", config.data_source_name)
        self.assertEquals("v03", config.data_source_version)
        self.assertEquals("5 min", config.install_min_time)
        self.assertEquals("25 min", config.install_max_time)
        self.assertEquals(["version-1", "version-2", "version-3"], config.supported_dbrs)
        self.assertEquals("version-1, version-2,version-3", config.expected_dbrs)

    def test_to_build_name(self):
        name = CourseConfig.to_build_name("Testing Courses, Part 1 & 2")
        self.assertEquals("testing-courses-part-1-2", name)

        self.assertIsNone(CourseConfig.to_build_name(None))


if __name__ == '__main__':
    unittest.main()
