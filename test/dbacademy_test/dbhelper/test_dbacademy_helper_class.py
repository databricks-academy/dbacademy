import unittest
from dbacademy.dbhelper import dbh_constants


class TestDBAcademyHelper(unittest.TestCase):

    def setUp(self) -> None:
        from dbacademy import dbgems

        self.tearDown()
        dbgems.MOCK_VALUES["workspace_id"] = "9876543210"

    def tearDown(self) -> None:
        from dbacademy import dbgems

        dbgems.MOCK_VALUES = dict()
        dbgems.MOCK_CONFIG = dict()

    def test_get_dbacademy_datasets_path(self):
        from dbacademy import dbgems
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper

        path = DBAcademyHelper.get_dbacademy_datasets_path()
        self.assertEquals("dbfs:/mnt/dbacademy-datasets", path)

        dbgems.MOCK_CONFIG[dbh_constants.DBACADEMY_HELPER.SPARK_CONF_PATHS_DATASETS] = "dbfs:/alternative/dbacademy-datasets"
        path = DBAcademyHelper.get_dbacademy_datasets_path()
        self.assertEquals("dbfs:/alternative/dbacademy-datasets", path)

    def test_get_dbacademy_users_path(self):
        from dbacademy import dbgems
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper

        path = DBAcademyHelper.get_dbacademy_users_path()
        self.assertEquals("dbfs:/mnt/dbacademy-users", path)

        dbgems.MOCK_CONFIG[dbh_constants.DBACADEMY_HELPER.SPARK_CONF_PATHS_USERS] = "dbfs:/alternative/dbacademy-users"
        path = DBAcademyHelper.get_dbacademy_users_path()
        self.assertEquals("dbfs:/alternative/dbacademy-users", path)

    def test_get_dbacademy_datasets_staging(self):
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper

        path = DBAcademyHelper.get_dbacademy_datasets_staging()
        self.assertEquals("dbfs:/mnt/dbacademy-datasets-staging", path)

    def test_to_unique_name_no_lesson(self):
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper
        from dbacademy.dbhelper.lesson_config import LessonConfig
        from dbacademy.dbhelper.course_config import CourseConfig

        course_config = CourseConfig(course_code="test",
                                     course_name="Testing 123",
                                     data_source_version="v01",
                                     install_min_time="1 min",
                                     install_max_time="5 min",
                                     supported_dbrs=["spark-123"],
                                     expected_dbrs="spark-123")

        lesson_config = LessonConfig(name=None,
                                     create_schema=False,
                                     create_catalog=False,
                                     requires_uc=False,
                                     install_datasets=False,
                                     enable_streaming_support=False,
                                     enable_ml_support=False,
                                     mocks={
                                         "__username": "mickey.mouse@disney.com"
                                     })

        lesson_config.lock_mutations(course_config)

        name = DBAcademyHelper.to_unique_name(lesson_config=lesson_config, sep="-")
        self.assertEquals("mickey-mouse-g4qd-da-test", name)

    def test_to_unique_name_with_lesson(self):
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper
        from dbacademy.dbhelper.lesson_config import LessonConfig
        from dbacademy.dbhelper.course_config import CourseConfig

        course_config = CourseConfig(course_code="test",
                                     course_name="Testing 123",
                                     data_source_version="v01",
                                     install_min_time="1 min",
                                     install_max_time="5 min",
                                     supported_dbrs=["spark-123"],
                                     expected_dbrs="spark-123")

        lesson_config = LessonConfig(name="Smoke Test",
                                     create_schema=False,
                                     create_catalog=False,
                                     requires_uc=False,
                                     install_datasets=False,
                                     enable_streaming_support=False,
                                     enable_ml_support=False,
                                     mocks={
                                         "__username": "mickey.mouse@disney.com"
                                     })

        lesson_config.lock_mutations(course_config)

        name = DBAcademyHelper.to_unique_name(lesson_config=lesson_config, sep="_")
        self.assertEquals("mickey_mouse_g4qd_da_test_smoke_test", name)

    def test_to_catalog_name_prefix(self):
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper

        name = DBAcademyHelper.to_catalog_name_prefix(username="mickey.mouse@disney.com")
        self.assertEquals("mickey_mouse_g4qd_da", name)

    def test_to_catalog_name(self):
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper

        name = DBAcademyHelper.to_catalog_name(username="donald.duck@disney.com", lesson_name=None)
        self.assertEquals("donald_duck_511r_da", name)

        name = DBAcademyHelper.to_catalog_name(username="mickey.mouse@disney.com", lesson_name="Lesson #12")
        self.assertEquals("mickey_mouse_g4qd_da_lesson_12", name)

    def test_to_schema_name_prefix(self):
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper

        name = DBAcademyHelper.to_schema_name_prefix(username="mickey.mouse@disney.com", course_code="test")
        self.assertEquals("mickey_mouse_g4qd_da_test", name)

    def test_to_schema_name(self):
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper

        name = DBAcademyHelper.to_schema_name(username="donald.duck@disney.com", course_code="qwer", lesson_name=None)
        self.assertEquals("donald_duck_511r_da_qwer", name)

        name = DBAcademyHelper.to_schema_name(username="mickey.mouse@disney.com", course_code="asdf", lesson_name="Skinning Cats")
        self.assertEquals("mickey_mouse_g4qd_da_asdf_skinning_cats", name)

    def test_is_smoke_test(self):
        from dbacademy import dbgems
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper

        self.assertFalse(DBAcademyHelper.is_smoke_test())

        dbgems.MOCK_CONFIG[dbh_constants.DBACADEMY_HELPER.SPARK_CONF_SMOKE_TEST] = "True"
        self.assertTrue(DBAcademyHelper.is_smoke_test())

    # def test_to_username_hash(self):
    #     username = "mickey.mouse@disney.com"
    #     da_name, da_hash = DBAcademyHelper.to_unique_name(username, "ex")
    #
    #     self.assertEqual("mickey.mouse", da_name)
    #     self.assertEqual(9984, da_hash)
    #
    #     username = "donald.duck@disney.com"
    #     da_name, da_hash = DBAcademyHelper.to_unique_name(username, "tst")
    #
    #     self.assertEqual("donald.duck", da_name)
    #     self.assertEqual(8192, da_hash)

    # def test_to_catalog_name(self):
    #     catalog_name = DBAcademyHelper.to_catalog_name(username="mickey.mouse@disney.com")
    #     self.assertEqual("mickey_mouse_9744_dbacademy", catalog_name)  # add assertion here
    #
    #     catalog_name = DBAcademyHelper.to_catalog_name(username="donald.duck@disney.com")
    #     self.assertEqual("donald_duck_8464_dbacademy", catalog_name)  # add assertion here
    #
    # def test_to_schema_name(self):
    #     schema_name = DBAcademyHelper.to_schema_name("mickey.mouse@disney.com", "ex")
    #     self.assertEqual("da_mickey_mouse_9984_ex", schema_name)  # add assertion here
    #
    #     schema_name = DBAcademyHelper.to_schema_name("donald.duck@disney.com", "tst")
    #     self.assertEqual("da_donald_duck_8192_tst", schema_name)  # add assertion here
    #
    #     schema_name = DBAcademyHelper.to_schema_name("donald.duck@disney.com", "tst-123")
    #     self.assertEqual("da_donald_duck_8192_tst_123", schema_name)  # add assertion here


if __name__ == '__main__':
    unittest.main()
