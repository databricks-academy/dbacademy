import unittest
from dbacademy.dbhelper import dbh_constants


class TestLessonConfig(unittest.TestCase):

    def test_to_clean_lesson_name(self):
        from dbacademy.dbhelper.lesson_config import LessonConfig

        # Converts [^a-zA-Z\d] to underscore
        # Replaces resulting duplicate __ with a single underscore (" - " becomes "_")
        # Preserves case
        lesson_name = "Test #123 - Getting Started"
        clean_lesson_name = LessonConfig.to_clean_lesson_name(lesson_name)
        self.assertEqual("Test_123_Getting_Started", clean_lesson_name)

        # None name should produce None result
        self.assertIsNone(LessonConfig.to_clean_lesson_name(None))

    def test_is_uc_enabled_workspace_CATALOG_UC_DEFAULT(self):
        from dbacademy.dbhelper.lesson_config import LessonConfig

        config = LessonConfig(_name=None,
                              _create_schema=False,
                              _create_catalog=True,
                              _requires_uc=True,
                              _install_datasets=False,
                              _enable_streaming_support=False,
                              _enable_ml_support=False,
                              _mocks={"__initial_catalog": dbh_constants.DBACADEMY_HELPER.CATALOG_UC_DEFAULT})

        self.assertIsNone(config.name)
        self.assertIsNone(config.clean_name)
        self.assertFalse(config.create_schema)
        self.assertTrue(config.create_catalog)
        self.assertTrue(config.requires_uc)
        self.assertFalse(config.installing_datasets)
        self.assertFalse(config.enable_streaming_support)

        self.assertTrue(config.is_uc_enabled_workspace)
        self.assertIsNone(config.username)
        self.assertIsNone(config.initial_schema)
        self.assertEquals(dbh_constants.DBACADEMY_HELPER.CATALOG_UC_DEFAULT, config.initial_catalog)

    def test_is_uc_enabled_workspace_STUDENT(self):
        from dbacademy.dbhelper.lesson_config import LessonConfig

        config = LessonConfig(_name=None,
                              _create_schema=False,
                              _create_catalog=True,
                              _requires_uc=True,
                              _install_datasets=False,
                              _enable_streaming_support=False,
                              _enable_ml_support=False,
                              _mocks={"__initial_catalog": "i_have_no_idea"})

        self.assertIsNone(config.name)
        self.assertIsNone(config.clean_name)
        self.assertFalse(config.create_schema)
        self.assertTrue(config.create_catalog)
        self.assertTrue(config.requires_uc)
        self.assertFalse(config.installing_datasets)
        self.assertFalse(config.enable_streaming_support)

        self.assertTrue(config.is_uc_enabled_workspace)
        self.assertIsNone(config.username)
        self.assertIsNone(config.initial_schema)
        self.assertEquals("i_have_no_idea", config.initial_catalog)

    def test_lesson_config_create_catalog_no_uc_support_CATALOG_SPARK_DEFAULT(self):
        from dbacademy.dbhelper.lesson_config import LessonConfig

        config = LessonConfig(_name=None,
                              _create_schema=False,
                              _create_catalog=True,
                              _requires_uc=True,
                              _install_datasets=False,
                              _enable_streaming_support=False,
                              _enable_ml_support=False,
                              _mocks={"__initial_catalog": dbh_constants.DBACADEMY_HELPER.CATALOG_SPARK_DEFAULT})
        try:
            config.assert_valid()
            raise Exception("Expected AssertionError")
        except AssertionError as e:
            msg = str(e)
            self.assertEquals(f"Cannot create a catalog, UC is not enabled for this workspace/cluster.", msg)

    def test_lesson_config_create_catalog_no_uc_support_CATALOG_NONE(self):
        from dbacademy.dbhelper.lesson_config import LessonConfig

        config = LessonConfig(_name=None,
                              _create_schema=False,
                              _create_catalog=True,
                              _requires_uc=True,
                              _install_datasets=False,
                              _enable_streaming_support=False,
                              _enable_ml_support=False)
        try:
            config.assert_valid()
            raise Exception("Expected AssertionError")
        except AssertionError as e:
            msg = str(e)
            self.assertEquals(f"Cannot create a catalog, UC is not enabled for this workspace/cluster.", msg)

    def test_lesson_config_create_with_catalog_and_schema(self):
        from dbacademy.dbhelper.lesson_config import LessonConfig

        config = LessonConfig(_name=None,
                              _create_schema=True,
                              _create_catalog=True,
                              _requires_uc=True,
                              _install_datasets=False,
                              _enable_streaming_support=False,
                              _enable_ml_support=False,
                              _mocks={"__initial_catalog": dbh_constants.DBACADEMY_HELPER.CATALOG_UC_DEFAULT})
        try:
            config.assert_valid()
            raise Exception("Expected AssertionError")
        except AssertionError as e:
            msg = str(e)
            self.assertEquals(f"Cannot create a user-specific schema when creating UC catalogs", msg)

    def test_lesson_config_create_schema(self):
        from dbacademy.dbhelper.lesson_config import LessonConfig

        config = LessonConfig(_name="Test 123 - Whatever",
                              _create_schema=True,
                              _create_catalog=False,
                              _requires_uc=True,
                              _install_datasets=False,
                              _enable_streaming_support=False,
                              _enable_ml_support=False,
                              _mocks={
                                 "__username": "mickey.mouse@disney.com",
                                 "__initial_schema": dbh_constants.DBACADEMY_HELPER.SCHEMA_DEFAULT,
                                 "__initial_catalog": "whatever_dude"
                              })

        self.assertEquals("Test 123 - Whatever", config.name)
        self.assertEquals("Test_123_Whatever", config.clean_name)
        self.assertTrue(config.create_schema)
        self.assertFalse(config.create_catalog)
        self.assertTrue(config.requires_uc)
        self.assertFalse(config.installing_datasets)
        self.assertFalse(config.enable_streaming_support)

        self.assertTrue(config.is_uc_enabled_workspace)
        self.assertEquals("mickey.mouse@disney.com", config.username)
        self.assertEquals(dbh_constants.DBACADEMY_HELPER.SCHEMA_DEFAULT, config.initial_schema)
        self.assertEquals("whatever_dude", config.initial_catalog)

    def test_immutable(self):
        from dbacademy.dbhelper.lesson_config import LessonConfig
        from dbacademy.dbhelper.course_config import CourseConfig

        course_config = CourseConfig(_course_code="asdf",
                                     _course_name="Apples, Spinach, Dates & Figs",
                                     _data_source_version="v01",
                                     _install_max_time="100 min",
                                     _install_min_time="1 min",
                                     _supported_dbrs=["whatever.x.y.z"],
                                     _expected_dbrs="whatever.x.y.z")

        config = LessonConfig(_name="Test 123 - Whatever",
                              _create_schema=True,
                              _create_catalog=False,
                              _requires_uc=True,
                              _install_datasets=False,
                              _enable_streaming_support=False,
                              _enable_ml_support=False,
                              _mocks={
                                 "__username": "mickey.mouse@disney.com",
                                 "__initial_schema": dbh_constants.DBACADEMY_HELPER.SCHEMA_DEFAULT,
                                 "__initial_catalog": "whatever_dude"
                              })

        config.lock_mutations(course_config)
        error_message = "LessonConfig is no longer mutable; DBAcademyHelper has already been initialized."

        try:
            config.name = "Something Else"
        except AssertionError as e:
            self.assertEquals(error_message, str(e))

        try:
            config.installing_datasets = True
        except AssertionError as e:
            self.assertEquals(error_message, str(e))

        try:
            config.enable_streaming_support = True
        except AssertionError as e:
            self.assertEquals(error_message, str(e))

        try:
            config.requires_uc = False
        except AssertionError as e:
            self.assertEquals(error_message, str(e))

        try:
            config.create_schema = False
        except AssertionError as e:
            self.assertEquals(error_message, str(e))

        try:
            config.create_catalog = True
        except AssertionError as e:
            self.assertEquals(error_message, str(e))


if __name__ == '__main__':
    unittest.main()
