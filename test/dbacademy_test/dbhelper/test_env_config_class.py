import unittest
from dbacademy.dbhelper.lesson_config_class import LessonConfig


class TestLessonConfig(unittest.TestCase):

    def test_to_catalog_name(self):
        catalog_name = LessonConfig.to_catalog_name("mickey.mouse@disney.com")
        self.assertEqual("mickey_mouse_9744_dbacademy", catalog_name)  # add assertion here

        catalog_name = LessonConfig.to_catalog_name("donald.duck@disney.com")
        self.assertEqual("donald_duck_8464_dbacademy", catalog_name)  # add assertion here

    def test_to_schema_name(self):
        schema_name = LessonConfig.to_schema_name("mickey.mouse@disney.com", "ex")
        self.assertEqual("da_mickey_mouse_9984_ex", schema_name)  # add assertion here

        schema_name = LessonConfig.to_schema_name("donald.duck@disney.com", "tst")
        self.assertEqual("da_donald_duck_8192_tst", schema_name)  # add assertion here


if __name__ == '__main__':
    unittest.main()
