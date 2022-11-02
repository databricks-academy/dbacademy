import unittest
import typing

from dbacademy.common import deprecated
from dbacademy.dbbuild.publish.notebook_def_class import NotebookDef, NotebookError


class TestNotebookDef(unittest.TestCase):

    def __init__(self, method_name):
        super().__init__(method_name)

    def assert_n_errors(self, expected, notebook: NotebookDef):
        message = f"Expected {expected} errors, found {len(notebook.errors)}"
        for error in notebook.errors:
            message += f"\n{error.message}"

        self.assertEqual(expected, len(notebook.errors), f"Expected {expected} errors, found {len(notebook.errors)}")

    def assert_n_warnings(self, expected, notebook: NotebookDef):
        message = f"Expected {expected} errors, found {len(notebook.warnings)}"
        for warning in notebook.warnings:
            message += f"\n{warning.message}"

        self.assertEqual(expected, len(notebook.warnings), message)

    def assert_message(self, messages: typing.List[NotebookError], index, message):
        self.assertEqual(message, messages[index].message)

    @staticmethod
    def create_notebook():
        from dbacademy.dbbuild import BuildConfig
        version = "1.2.3"
        config = {
            "name": "Unit Test"
        }
        build_config = BuildConfig.load_config(config, version)
        return NotebookDef(build_config=build_config,
                           path="Agenda",
                           replacements={},
                           include_solution=False,
                           test_round=2,
                           ignored=False,
                           order=0,
                           i18n=True,
                           i18n_language="English",
                           ignoring=[],
                           version=version)

    @deprecated(reason="Just because")
    def dummy(self, arg_1, arg_2, *args, **kwargs):
        pass

    def test_dummy(self):
        self.dummy("apples", "blue", "red", happy=True)

    def test_parse_version_no_version(self):
        command = r"""
        # MAGIC %pip install \
        # MAGIC git+https://github.com/databricks-academy/dbacademy-gems \
        # MAGIC git+https://github.com/databricks-academy/dbacademy-rest \
        # MAGIC git+https://github.com/databricks-academy/dbacademy-helper \
        # MAGIC --quiet --disable-pip-version-check""".strip()

        version = NotebookDef.parse_version(command, "git+https://github.com/databricks-academy/dbacademy-rest")
        self.assertEqual("", version)

    def test_parse_version_with_commit_hash(self):
        command = r"""
        # MAGIC %pip install \
        # MAGIC git+https://github.com/databricks-academy/dbacademy-gems@123 \
        # MAGIC git+https://github.com/databricks-academy/dbacademy-rest@123 \
        # MAGIC git+https://github.com/databricks-academy/dbacademy-helper@123 \
        # MAGIC --quiet --disable-pip-version-check""".strip()

        version = NotebookDef.parse_version(command, "git+https://github.com/databricks-academy/dbacademy-rest@")
        self.assertEqual("123", version)

    def test_parse_version_with_tagged_version(self):
        command = r"""
        # MAGIC %pip install \
        # MAGIC git+https://github.com/databricks-academy/dbacademy-gems@v1.2.3 \
        # MAGIC git+https://github.com/databricks-academy/dbacademy-rest@v4.5.6 \
        # MAGIC git+https://github.com/databricks-academy/dbacademy-helper@v7.8.9 \
        # MAGIC --quiet --disable-pip-version-check""".strip()

        version = NotebookDef.parse_version(command, "git+https://github.com/databricks-academy/dbacademy-rest@")
        self.assertEqual("v4.5.6", version)

    def test_parse_version_with_eof(self):
        command = r"""
        # MAGIC %pip install \
        # MAGIC git+https://github.com/databricks-academy/dbacademy-gems@v1.2.3 \
        # MAGIC git+https://github.com/databricks-academy/dbacademy-rest@v4.5.6""".strip()

        version = NotebookDef.parse_version(command, "git+https://github.com/databricks-academy/dbacademy-rest@")
        self.assertEqual("v4.5.6", version)

    def test_test_pip_cells_pinned(self):
        command = r"""
# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems@123 \
# MAGIC git+https://github.com/databricks-academy/dbacademy-rest@123 \
# MAGIC git+https://github.com/databricks-academy/dbacademy-helper@123 \
# MAGIC --quiet --disable-pip-version-check""".strip()

        notebook = self.create_notebook()
        notebook.test_pip_cells(language="Python", command=command, i=3)

        self.assertEqual(0, len(notebook.warnings), f"Expected 0 warnings, found {len(notebook.errors)}")
        self.assertEqual(0, len(notebook.errors), f"Expected 0 error, found {len(notebook.errors)}")

    def test_test_pip_cells_not_pinned(self):
        command = r"""
            # MAGIC %pip install \
            # MAGIC git+https://github.com/databricks-academy/dbacademy-moo \
            # MAGIC --quiet --disable-pip-version-check""".strip()

        notebook = self.create_notebook()
        notebook.test_pip_cells(language="Python", command=command, i=3)

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(1, notebook)

        self.assertEqual("Cmd #4 | The library is not pinned to a specific version: git+https://github.com/databricks-academy/dbacademy-moo", notebook.errors[0].message)

    def test_good_single_space_i18n(self):
        command = """
# MAGIC %md --i18n-TBD
# MAGIC 
# MAGIC # Build-Time Substitutions""".strip()

        notebook = self.create_notebook()
        notebook.update_md_cells(language="Python", command=command, i=3, i18n_guid_map={"--i18n-TBD": "whatever"}, other_notebooks=[])

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(0, notebook)

        self.assertEqual(1, len(notebook.i18n_guids), f"Expected 1 GUID, found {len(notebook.i18n_guids)}")
        self.assertEqual("--i18n-TBD", notebook.i18n_guids[0])

    def test_good_double_spaced_i18n(self):
        command = """
            # MAGIC %md  --i18n-TBD
            # MAGIC 
            # MAGIC # Build-Time Substitutions""".strip()

        notebook = self.create_notebook()
        notebook.update_md_cells(language="Python", command=command, i=3, i18n_guid_map={"--i18n-TBD": "whatever"}, other_notebooks=[])

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(0, notebook)

        self.assertEqual(1, len(notebook.i18n_guids), f"Expected 1 GUID, found {len(notebook.i18n_guids)}")
        self.assertEqual("--i18n-TBD", notebook.i18n_guids[0])

    def test_good_md_sandbox_i18n(self):
        command = """
            # MAGIC %md-sandbox --i18n-TBD
            # MAGIC 
            # MAGIC # Build-Time Substitutions""".strip()

        notebook = self.create_notebook()
        notebook.update_md_cells(language="Python", command=command, i=3, i18n_guid_map={"--i18n-TBD": "whatever"}, other_notebooks=[])

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(0, notebook)

        self.assertEqual(1, len(notebook.i18n_guids), f"Expected 1 GUID, found {len(notebook.i18n_guids)}")
        self.assertEqual("--i18n-TBD", notebook.i18n_guids[0])

    def test_missing_i18n_multi(self):
        command = """
            # MAGIC %md
            # MAGIC 
            # MAGIC # Build-Time Substitutions""".strip()

        notebook = self.create_notebook()
        notebook.update_md_cells(language="Python", command=command, i=3, i18n_guid_map={"--i18n-TBD": "whatever"}, other_notebooks=[])

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(1, notebook)

        self.assertEqual("Cmd #4 | Missing the i18n directive: %md", notebook.errors[0].message)

    def test_missing_i18n_single(self):
        command = """
            # MAGIC %md | # Build-Time Substitutions""".strip()

        notebook = self.create_notebook()
        notebook.update_md_cells(language="Python", command=command, i=3, i18n_guid_map={"--i18n-TBD": "whatever"}, other_notebooks=[])

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(1, notebook)

        self.assertEqual("Cmd #4 | Expected MD to have more than 1 line of code with i18n enabled: %md | # Build-Time Substitutions", notebook.errors[0].message)

    def test_extra_word_i18n(self):
        command = """
            # MAGIC %md --i18n-TBD # Title
            # MAGIC 
            # MAGIC # Build-Time Substitutions""".strip()

        notebook = self.create_notebook()
        notebook.update_md_cells(language="Python", command=command, i=3, i18n_guid_map={"--i18n-TBD": "whatever"}, other_notebooks=[])

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(1, notebook)

        self.assertEqual("Cmd #4 | Expected the first line of MD to have only two words, found 4: %md --i18n-TBD # Title", notebook.errors[0].message)

    def test_duplicate_i18n_guid(self):
        command_a = """
            # MAGIC %md --i18n-a6e39b59-1715-4750-bd5d-5d638cf57c3a
            # MAGIC # Some Title""".strip()
        command_b = """
            # MAGIC %md --i18n-a6e39b59-1715-4750-bd5d-5d638cf57c3a
            # MAGIC # Some Title""".strip()

        i18n_guid_map = {
            "--i18n-a6e39b59-1715-4750-bd5d-5d638cf57c3a": "whatever"
        }

        notebook = self.create_notebook()
        notebook.update_md_cells(language="Python", command=command_a, i=3, i18n_guid_map=i18n_guid_map, other_notebooks=[])
        notebook.update_md_cells(language="Python", command=command_b, i=4, i18n_guid_map=i18n_guid_map, other_notebooks=[])

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(1, notebook)

        self.assertEqual("Cmd #5 | Duplicate i18n GUID found: --i18n-a6e39b59-1715-4750-bd5d-5d638cf57c3a", notebook.errors[0].message)

    def test_unique_i18n_guid(self):
        guid_a = "--i18n-a6e39b59-1715-4750-bd5d-5d638cf57c3a"
        command_a = f"""
            # MAGIC %md {guid_a}
            # MAGIC # Some Title""".strip()

        guid_b = "--i18n-9d06d80d-2381-42d5-8f9e-cc99ee3cd82a"
        command_b = f"""
            # MAGIC %md {guid_b}
            # MAGIC # Some Title""".strip()

        i18n_guid_map_a = {guid_a: "# MAGIC # Some Title"}
        i18n_guid_map_b = {guid_b: "# MAGIC # Some Title"}

        notebook = self.create_notebook()
        notebook.update_md_cells(language="Python", command=command_a, i=3, i18n_guid_map=i18n_guid_map_a, other_notebooks=[])
        notebook.update_md_cells(language="Python", command=command_b, i=4, i18n_guid_map=i18n_guid_map_b, other_notebooks=[])

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(0, notebook)

    def test_md_i18n_guid_replacement(self):
        command = """# MAGIC %md --i18n-a6e39b59-1715-4750-bd5d-5d638cf57c3a\n# MAGIC # Some Title""".strip()

        i18n_guid_map = {"--i18n-a6e39b59-1715-4750-bd5d-5d638cf57c3a": "# MAGIC # Some Title"}

        notebook = self.create_notebook()
        actual = notebook.update_md_cells(language="Python", command=command, i=4, i18n_guid_map=i18n_guid_map, other_notebooks=[])

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(0, notebook)

        expected = """# MAGIC %md <i18n value="a6e39b59-1715-4750-bd5d-5d638cf57c3a"/>\n# MAGIC # Some Title""".strip()
        self.assertEqual(expected, actual)

    def test_md_sandbox_i18n_guid_replacement(self):
        command = """# MAGIC %md-sandbox --i18n-a6e39b59-1715-4750-bd5d-5d638cf57c3a\n# MAGIC # Some Title""".strip()

        i18n_guid_map = {"--i18n-a6e39b59-1715-4750-bd5d-5d638cf57c3a": "# MAGIC # Some Title"}

        notebook = self.create_notebook()
        actual = notebook.update_md_cells(language="Python", command=command, i=4, i18n_guid_map=i18n_guid_map, other_notebooks=[])

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(0, notebook)

        expected = """# MAGIC %md-sandbox <i18n value="a6e39b59-1715-4750-bd5d-5d638cf57c3a"/>\n# MAGIC # Some Title""".strip()
        self.assertEqual(expected, actual)

    def test_i18n_sql(self):
        command = """-- MAGIC %md-sandbox --i18n-a6e39b59-1715-4750-bd5d-5d638cf57c3a\n-- MAGIC # Some Title""".strip()

        i18n_guid_map = {"--i18n-a6e39b59-1715-4750-bd5d-5d638cf57c3a": "-- MAGIC # Some Title"}

        notebook = self.create_notebook()
        actual = notebook.update_md_cells(language="SQL", command=command, i=4, i18n_guid_map=i18n_guid_map, other_notebooks=[])

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(0, notebook)

        expected = """-- MAGIC %md-sandbox <i18n value="a6e39b59-1715-4750-bd5d-5d638cf57c3a"/>\n-- MAGIC # Some Title""".strip()
        self.assertEqual(expected, actual)

    def test_i18n_single_line(self):
        command = """-- MAGIC %md --i18n-a6e39b59-1715-4750-bd5d-5d638cf57c3a # Some Title""".strip()

        notebook = self.create_notebook()
        notebook.update_md_cells(language="SQL", command=command, i=4, i18n_guid_map={"--i18n-TBD": "whatever"}, other_notebooks=[])

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(1, notebook)

        self.assertEqual("Cmd #5 | Expected MD to have more than 1 line of code with i18n enabled: %md --i18n-a6e39b59-1715-4750-bd5d-5d638cf57c3a # Some Title", notebook.errors[0].message)

    def test_parse_links(self):
        notebook = self.create_notebook()

        links = notebook.parse_html_links("""
            # MAGIC %md --i18n-TBD
            # MAGIC # Bla bla bla""".strip())
        self.assertEqual(0, len(links))

        links = notebook.parse_html_links("""
            # MAGIC %md --i18n-TBD
            # MAGIC # <a href="https://example.com" target="_blank">some link</a>
            # MAGIC # Bla bla bla""".strip())
        self.assertEqual(1, len(links))

        links = notebook.parse_html_links("""
            # MAGIC %md --i18n-TBD
            # MAGIC # <a href="https://example.com" target="_blank">some link</a>
            # MAGIC # <a href="https://google.com" target="_blank">some link</a>
            # MAGIC # Bla bla bla""".strip())
        self.assertEqual(2, len(links))

        links = notebook.parse_html_links("""
            # MAGIC %md --i18n-TBD
            # MAGIC # <a href="https://example.com" target="_blank">some link</a><a href="https://google.com" target="_blank">some link</a><a href="https://databricks.com" target="_blank">some link</a>
            # MAGIC # Bla bla bla""".strip())
        self.assertEqual(3, len(links))

    def test_validate_html_link_with_target(self):
        command = """
        # MAGIC %md --i18n-TBD
        # MAGIC # <a href="https://example.com" target="_blank">some link</a>""".strip()

        notebook = self.create_notebook()
        notebook.validate_html_link(3, command)

        self.assert_n_errors(0, notebook)
        self.assert_n_warnings(0, notebook)

    def test_validate_html_link_no_target(self):
        command = """
        # MAGIC %md --i18n-TBD
        # MAGIC # <a href="https://example.com">some link</a>""".strip()

        notebook = self.create_notebook()
        notebook.validate_html_link(3, command)

        self.assert_n_errors(0, notebook)
        self.assert_n_warnings(1, notebook)

        self.assert_message(notebook.warnings, 0, "Cmd #4 | Found HTML link without the required target=\"_blank\": <a href=\"https://example.com\">some link</a>")

    def test_get_latest_commit_id(self):
        commit_id = NotebookDef.get_latest_commit_id("dbacademy-gems")
        self.assertIsNotNone(commit_id, f"Expected non-None value for dbacademy-gems")

        commit_id = NotebookDef.get_latest_commit_id("dbacademy-rest")
        self.assertIsNotNone(commit_id, f"Expected non-None value for dbacademy-rest")

        commit_id = NotebookDef.get_latest_commit_id("dbacademy-helper")
        self.assertIsNotNone(commit_id, f"Expected non-None value for dbacademy-helper")

    @staticmethod
    def test_replacement():
        command = """# Databricks notebook source
# MAGIC %md --i18n-5f2cfc0b-1998-4182-966d-8efed6020eb2
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC # Getting Started with the Databricks Platform
# MAGIC 
# MAGIC This notebook provides a hands-on review of some of the basic functionality of the Databricks Data Science and Engineering Workspace.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lessons, you will be able to:
# MAGIC - Rename a notebook and change the default language
# MAGIC - Attach a cluster
# MAGIC - Use the **`%run`** magic command
# MAGIC - Run Python and SQL cells
# MAGIC - Create a Markdown cell

# COMMAND ----------
# MAGIC %md --i18n-05dca5e4-6c50-4b39-a497-a35cd6d99434
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC # Renaming a Notebook
# MAGIC 
# MAGIC Changing the name of a notebook is easy. Click on the name at the top of this page, then make changes to the name. To make it easier to navigate back to this notebook later in case you need to, append a short test string to the end of the existing name."""
        m = "#"
        result = command.replace(f"{m} MAGIC ", "")
        print(result)


if __name__ == '__main__':
    unittest.main()
