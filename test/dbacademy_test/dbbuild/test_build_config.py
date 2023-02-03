import unittest
from typing import Dict


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

    def test_get_lesson_number(self):
        from dbacademy.dbbuild.build_config_class import BuildConfig

        self.assertEqual(-1, BuildConfig.get_lesson_number("Includes/Whatever"))
        self.assertEqual(-1, BuildConfig.get_lesson_number("AB - 03 - Lesson Three"))
        self.assertEqual(-1, BuildConfig.get_lesson_number(" - 03 - Lesson Three"))
        self.assertEqual(-1, BuildConfig.get_lesson_number("32A - Lesson Three"))
        self.assertEqual(-1, BuildConfig.get_lesson_number("32 B - Lesson Three"))

        self.assertEqual(2, BuildConfig.get_lesson_number("ADE 2 - Streaming Architecture with DLT/ADE 2.1 - Operations"))
        self.assertEqual(6, BuildConfig.get_lesson_number("ADE 6 - Automate Production Jobs - WIP/ADE 6.1 - Getting Started"))
        self.assertEqual(0, BuildConfig.get_lesson_number("DE 0 - Intro to PySpark/DE 0.00 - Module Introduction"))
        self.assertEqual(1, BuildConfig.get_lesson_number("DE 1 - Databricks Workspace/DE 1.1 - Create and Manage Interactive Clusters"))

        self.assertEqual(1, BuildConfig.get_lesson_number("1 - Lesson One"))
        self.assertEqual(2, BuildConfig.get_lesson_number("02 - Lesson Two"))
        self.assertEqual(3, BuildConfig.get_lesson_number("P03 - Lesson Three"))

    def test_ignore_failures(self):
        from dbacademy.dbbuild.build_config_class import BuildConfig
        from dbacademy.dbbuild.publish.notebook_def_class import NotebookDef

        version = "vTEST"

        build_config = BuildConfig(name="Test Suite")
        build_config.load_config({"name": "Data Engineering with Databricks"}, version)

        build_config.notebooks = {}
        paths = [
            "A00 - Intro to PySpark/DE 0.00 - Module Introduction",
            "A01 - Databricks Workspace/DE 1.0 - Module Introduction",
            "A02 - ETL with Spark/DE 2.0 - Module Introduction",
        ]
        for path in paths:
            build_config.notebooks[path] = NotebookDef(
                build_config=build_config,
                path=path,
                replacements={},
                include_solution=True,
                test_round=-1,
                ignored=False,
                order=0,
                i18n=False,
                i18n_language=None,
                ignoring=[],
                version=version)

        build_config.ignore_failures(lambda p, n: n == 0)

        self.assertTrue(build_config.notebooks["A00 - Intro to PySpark/DE 0.00 - Module Introduction"].ignored)
        self.assertFalse(build_config.notebooks["A01 - Databricks Workspace/DE 1.0 - Module Introduction"].ignored)
        self.assertFalse(build_config.notebooks["A02 - ETL with Spark/DE 2.0 - Module Introduction"].ignored)

    def set_test_round(self):
        from dbacademy.dbbuild.build_config_class import BuildConfig
        from dbacademy.dbbuild.publish.notebook_def_class import NotebookDef

        version = "vTEST"

        build_config = BuildConfig(name="Test Suite")
        build_config.load_config({"name": "Data Engineering with Databricks"}, version)

        build_config.notebooks = {}
        paths = [
            "Includes/Whatever",
            "A00 - Intro to PySpark/DE 0.00 - Module Introduction",
            "A01 - Databricks Workspace/DE 1.0 - Module Introduction",
            "A02 - ETL with Spark/DE 2.0 - Module Introduction",
        ]
        for path in paths:
            build_config.notebooks[path] = NotebookDef(
                build_config=build_config,
                path=path,
                replacements={},
                include_solution=True,
                test_round=-1,
                ignored=False,
                order=0,
                i18n=False,
                i18n_language=None,
                ignoring=[],
                version=version)

        build_config.set_test_round(9, lambda p, n: n == 3)

        self.assertEqual(0, build_config.notebooks["Includes/Whatever"].ignored)
        self.assertEqual(2, build_config.notebooks["A00 - Intro to PySpark/DE 0.00 - Module Introduction"].ignored)
        self.assertEqual(9, build_config.notebooks["A01 - Databricks Workspace/DE 1.0 - Module Introduction"].ignored)
        self.assertEqual(2, build_config.notebooks["A02 - ETL with Spark/DE 2.0 - Module Introduction"].ignored)

    def test_exclude_notebook(self):
        from dbacademy.dbbuild.build_config_class import BuildConfig
        from dbacademy.dbbuild.publish.notebook_def_class import NotebookDef

        version = "vTEST"

        build_config = BuildConfig(name="Test Suite")
        build_config.load_config({"name": "Data Engineering with Databricks"}, version)

        build_config.notebooks = {}
        paths = [
            "A00 - Intro to PySpark/DE 0.00 - Module Introduction",
            "A01 - Databricks Workspace/DE 1.0 - Module Introduction",
            "A02 - ETL with Spark/DE 2.0 - Module Introduction",
        ]
        for path in paths:
            build_config.notebooks[path] = NotebookDef(
                build_config=build_config,
                path=path,
                replacements={},
                include_solution=True,
                test_round=-1,
                ignored=False,
                order=0,
                i18n=False,
                i18n_language=None,
                ignoring=[],
                version=version)

        build_config.exclude_notebook(lambda p, n: n == 1)

        self.assertTrue("A00 - Intro to PySpark/DE 0.00 - Module Introduction" in build_config.notebooks)
        self.assertFalse("A01 - Databricks Workspace/DE 1.0 - Module Introduction" in build_config.notebooks)
        self.assertTrue("A02 - ETL with Spark/DE 2.0 - Module Introduction" in build_config.notebooks)


if __name__ == '__main__':
    unittest.main()
