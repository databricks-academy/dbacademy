__all__ = ["TestNotebookDef"]

import unittest
import typing
from dbacademy.dbbuild.publish.notebook_def_class import NotebookDef, NotebookError


class TestNotebookDef(unittest.TestCase):

    def __init__(self, method_name):
        super().__init__(method_name)

    def assert_n_errors(self, expected: int, notebook: NotebookDef):
        message = f"Expected {expected} errors, found {len(notebook.logger.errors)}"
        for error in notebook.logger.errors:
            message += f"\n{error.message}"

        self.assertEqual(expected, len(notebook.logger.errors), f"Expected {expected} errors, found {len(notebook.logger.errors)}")

    def assert_n_warnings(self, expected: int, notebook: NotebookDef):
        message = f"Expected {expected} errors, found {len(notebook.logger.warnings)}"
        for warning in notebook.logger.warnings:
            message += f"\n{warning.message}"

        self.assertEqual(expected, len(notebook.logger.warnings), message)

    def assert_message(self, messages: typing.List[NotebookError], index, message):
        self.assertEqual(message, messages[index].message)

    @staticmethod
    def create_notebook():
        from dbacademy.dbbuild.build_config_class import BuildConfig

        version = "1.2.3"
        build_config = BuildConfig(name="Unit Test",
                                   version=version)

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

    # def test_parse_version_no_version(self):
    #     command = r"""
    #     # MAGIC %pip install \
    #     # MAGIC git+https://github.com/databricks-academy/dbacademy-gems \
    #     # MAGIC git+https://github.com/databricks-academy/dbacademy-rest \
    #     # MAGIC git+https://github.com/databricks-academy/dbacademy-helper \
    #     # MAGIC --quiet --disable-pip-version-check""".strip()
    #
    #     version = NotebookDef.parse_version(command, "git+https://github.com/databricks-academy/dbacademy-rest")
    #     self.assertEqual("", version)

    # def test_parse_version_with_commit_hash(self):
    #     command = r"""
    #     # MAGIC %pip install \
    #     # MAGIC git+https://github.com/databricks-academy/dbacademy-gems@123 \
    #     # MAGIC git+https://github.com/databricks-academy/dbacademy-rest@123 \
    #     # MAGIC git+https://github.com/databricks-academy/dbacademy-helper@123 \
    #     # MAGIC --quiet --disable-pip-version-check""".strip()
    #
    #     version = NotebookDef.parse_version(command, "git+https://github.com/databricks-academy/dbacademy-rest@")
    #     self.assertEqual("123", version)

    # def test_parse_version_with_tagged_version(self):
    #     command = r"""
    #     # MAGIC %pip install \
    #     # MAGIC git+https://github.com/databricks-academy/dbacademy-gems@v1.2.3 \
    #     # MAGIC git+https://github.com/databricks-academy/dbacademy-rest@v4.5.6 \
    #     # MAGIC git+https://github.com/databricks-academy/dbacademy-helper@v7.8.9 \
    #     # MAGIC --quiet --disable-pip-version-check""".strip()
    #
    #     version = NotebookDef.parse_version(command, "git+https://github.com/databricks-academy/dbacademy-rest@")
    #     self.assertEqual("v4.5.6", version)

    # def test_parse_version_with_eof(self):
    #     command = r"""
    #     # MAGIC %pip install \
    #     # MAGIC git+https://github.com/databricks-academy/dbacademy-gems@v1.2.3 \
    #     # MAGIC git+https://github.com/databricks-academy/dbacademy-rest@v4.5.6""".strip()
    #
    #     version = NotebookDef.parse_version(command, "git+https://github.com/databricks-academy/dbacademy-rest@")
    #     self.assertEqual("v4.5.6", version)

    def test_test_pip_cells(self):

        command = r"# MAGIC %pip install git+https://github.com/databricks-academy/dbacademy"
        notebook = self.create_notebook()
        notebook.test_pip_cells(language="Python", command=command, i=3)
        self.assertEqual(0, len(notebook.logger.warnings), f"Expected 0 warnings, found {len(notebook.logger.errors)}")
        self.assertEqual(0, len(notebook.logger.errors), f"Expected 0 error, found {len(notebook.logger.errors)}")

        command = r"# MAGIC %pip install git+https://github.com/databricks-academy/dbacademy-gems"
        notebook = self.create_notebook()
        notebook.test_pip_cells(language="Python", command=command, i=3)
        self.assertEqual(0, len(notebook.logger.warnings), f"Expected 0 warnings, found {len(notebook.logger.errors)}")
        self.assertEqual(1, len(notebook.logger.errors), f"Expected 0 error, found {len(notebook.logger.errors)}")
        expected = "Cmd #4 | Using unsupported repo, https://github.com/databricks-academy/dbacademy-gems is no longer supported; please use https://github.com/databricks-academy/dbacademy instead."
        self.assertEqual(expected, notebook.logger.errors[0].message)

        command = r"# MAGIC %pip install git+https://github.com/databricks-academy/dbacademy-rest"
        notebook = self.create_notebook()
        notebook.test_pip_cells(language="Python", command=command, i=3)
        self.assertEqual(0, len(notebook.logger.warnings), f"Expected 0 warnings, found {len(notebook.logger.errors)}")
        self.assertEqual(1, len(notebook.logger.errors), f"Expected 0 error, found {len(notebook.logger.errors)}")
        expected = "Cmd #4 | Using unsupported repo, https://github.com/databricks-academy/dbacademy-rest is no longer supported; please use https://github.com/databricks-academy/dbacademy instead."
        self.assertEqual(expected, notebook.logger.errors[0].message)

        command = r"# MAGIC %pip install git+https://github.com/databricks-academy/dbacademy-helper"
        notebook = self.create_notebook()
        notebook.test_pip_cells(language="Python", command=command, i=3)
        self.assertEqual(0, len(notebook.logger.warnings), f"Expected 0 warnings, found {len(notebook.logger.errors)}")
        self.assertEqual(1, len(notebook.logger.errors), f"Expected 0 error, found {len(notebook.logger.errors)}")
        expected = "Cmd #4 | Using unsupported repo, https://github.com/databricks-academy/dbacademy-helper is no longer supported; please use https://github.com/databricks-academy/dbacademy instead."
        self.assertEqual(expected, notebook.logger.errors[0].message)

        command = r"# MAGIC %pip install git+https://github.com/databricks-academy/dbacademy-courseware"
        notebook = self.create_notebook()
        notebook.test_pip_cells(language="Python", command=command, i=3)
        self.assertEqual(0, len(notebook.logger.warnings), f"Expected 0 warnings, found {len(notebook.logger.errors)}")
        self.assertEqual(1, len(notebook.logger.errors), f"Expected 0 error, found {len(notebook.logger.errors)}")
        expected = "Cmd #4 | Using unsupported repo, https://github.com/databricks-academy/dbacademy-courseware is no longer supported; please use https://github.com/databricks-academy/dbacademy instead."
        self.assertEqual(expected, notebook.logger.errors[0].message)

        # MAGIC git+https://github.com/databricks-academy/dbacademy-gems@4.5.6 \
        # MAGIC git+https://github.com/databricks-academy/dbacademy-@7.8.9 \
        # MAGIC git+https://github.com/databricks-academy/dbacademy-@10.11.12 \
        # MAGIC --quiet --disable-pip-version-check""".strip()

#     def test_test_pip_cells_not_pinned(self):
#         command = r"""
# # MAGIC %pip install \
# # MAGIC git+https://github.com/databricks-academy/dbacademy-moo \
# # MAGIC --quiet --disable-pip-version-check""".strip()
#
#         notebook = self.create_notebook()
#         notebook.test_pip_cells(language="Python", command=command, i=3)
#
#         self.assert_n_warnings(0, notebook)
#         self.assert_n_errors(1, notebook)
#
#         actual_message = notebook.errors[0].message
#         expected_message = r"""
# Cmd #4 | The library is not pinned to a specific version: git+https://github.com/databricks-academy/dbacademy-moo
# # MAGIC %pip install \
# # MAGIC git+https://github.com/databricks-academy/dbacademy-moo \
# # MAGIC --quiet --disable-pip-version-check
# """.strip()
#         self.assertEqual(expected_message, actual_message)

    def test_validate_html_link_with_target(self):
        command = """
# MAGIC %md --i18n-TBD
# MAGIC # <a href="https://example.com" target="_blank">some link</a>""".strip()

        notebook = self.create_notebook()
        notebook.validate_html_link(i=3, command=command)

        self.assert_n_errors(0, notebook)
        self.assert_n_warnings(0, notebook)

    def test_validate_html_link_no_target(self):
        command = """
# MAGIC %md --i18n-TBD
# MAGIC # <a href="https://example.com">some link</a>""".strip()

        notebook = self.create_notebook()
        notebook.validate_html_link(i=3, command=command)

        self.assert_n_errors(0, notebook)
        self.assert_n_warnings(1, notebook)

        self.assert_message(notebook.logger.warnings, 0, "Cmd #4 | Found HTML link without the required target=\"_blank\": <a href=\"https://example.com\">some link</a>")

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

    def test_build_install_libraries_cell_v1(self):
        command = r"""
    # INSTALL_LIBRARIES
    version = "v9.8.7"
    if not version.startswith("v"): library_url = f"git+https://github.com/databricks-academy/dbacademy@{version}"
    else: library_url = f"https://github.com/databricks-academy/dbacademy/releases/download/{version}/dbacademy-{version[1:]}-py3-none-any.whl"
    pip_command = f"install --quiet --disable-pip-version-check {library_url}"
    """.strip()

        notebook = self.create_notebook()
        actual_command = notebook.build_install_libraries_cell(i=3, command=command)

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(0, notebook)

        expected_command = """
def __validate_libraries():
    import requests
    try:
        site = "https://github.com/databricks-academy/dbacademy"
        response = requests.get(site)
        error = f"Unable to access GitHub or PyPi resources (HTTP {response.status_code} for {site})."
        assert response.status_code == 200, "{error} Please see the \\"Troubleshooting | {section}\\" section of the \\"Version Info\\" notebook for more information.".format(error=error, section="Cannot Install Libraries")
    except Exception as e:
        if type(e) is AssertionError: raise e
        error = f"Unable to access GitHub or PyPi resources ({site})."
        raise AssertionError("{error} Please see the \\"Troubleshooting | {section}\\" section of the \\"Version Info\\" notebook for more information.".format(error=error, section="Cannot Install Libraries")) from e

def __install_libraries():
    global pip_command
    
    specified_version = f"v9.8.7"
    key = "dbacademy.library.version"
    version = spark.conf.get(key, specified_version)

    if specified_version != version:
        print("** Dependency Version Overridden *******************************************************************")
        print(f"* This course was built for {specified_version} of the DBAcademy Library, but it is being overridden via the Spark")
        print(f"* configuration variable \\"{key}\\". The use of version v9.8.7 is not advised as we")
        print(f"* cannot guarantee compatibility with this version of the course.")
        print("****************************************************************************************************")

    try:
        from dbacademy import dbgems  
        installed_version = dbgems.lookup_current_module_version("dbacademy")
        if installed_version == version:
            pip_command = "list --quiet"  # Skipping pip install of pre-installed python library
        else:
            print(f"WARNING: The wrong version of dbacademy is attached to this cluster. Expected {version}, found {installed_version}.")
            print(f"Installing the correct version.")
            raise Exception("Forcing re-install")

    except Exception as e:
        # The import fails if library is not attached to cluster
        if not version.startswith("v"): library_url = f"git+https://github.com/databricks-academy/dbacademy@{version}"
        else: library_url = f"https://github.com/databricks-academy/dbacademy/releases/download/{version}/dbacademy-{version[1:]}-py3-none-any.whl"

        default_command = f"install --quiet --disable-pip-version-check {library_url}"
        pip_command = spark.conf.get("dbacademy.library.install", default_command)

        if pip_command != default_command:
            print(f"WARNING: Using alternative library installation:\\n| default: %pip {default_command}\\n| current: %pip {pip_command}")
        else:
            # We are using the default libraries; next we need to verify that we can reach those libraries.
            __validate_libraries()

__install_libraries()
    """.strip()
        self.maxDiff = None
        self.assertEqual(expected_command, actual_command)

    def test_build_install_libraries_cell_v2(self):
        command = r"""
# INSTALL_LIBRARIES
version="v6.5.4"
if not version.startswith("v"): library_url = f"git+https://github.com/databricks-academy/dbacademy@{version}"
else: library_url = f"https://github.com/databricks-academy/dbacademy/releases/download/{version}/dbacademy-{version[1:]}-py3-none-any.whl"
pip_command = f"install --quiet --disable-pip-version-check {library_url}"
""".strip()

        notebook = self.create_notebook()
        actual_command = notebook.build_install_libraries_cell(i=3, command=command)

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(0, notebook)

        expected_command = """
def __validate_libraries():
    import requests
    try:
        site = "https://github.com/databricks-academy/dbacademy"
        response = requests.get(site)
        error = f"Unable to access GitHub or PyPi resources (HTTP {response.status_code} for {site})."
        assert response.status_code == 200, "{error} Please see the \\"Troubleshooting | {section}\\" section of the \\"Version Info\\" notebook for more information.".format(error=error, section="Cannot Install Libraries")
    except Exception as e:
        if type(e) is AssertionError: raise e
        error = f"Unable to access GitHub or PyPi resources ({site})."
        raise AssertionError("{error} Please see the \\"Troubleshooting | {section}\\" section of the \\"Version Info\\" notebook for more information.".format(error=error, section="Cannot Install Libraries")) from e

def __install_libraries():
    global pip_command
    
    specified_version = f"v6.5.4"
    key = "dbacademy.library.version"
    version = spark.conf.get(key, specified_version)

    if specified_version != version:
        print("** Dependency Version Overridden *******************************************************************")
        print(f"* This course was built for {specified_version} of the DBAcademy Library, but it is being overridden via the Spark")
        print(f"* configuration variable \\"{key}\\". The use of version v6.5.4 is not advised as we")
        print(f"* cannot guarantee compatibility with this version of the course.")
        print("****************************************************************************************************")

    try:
        from dbacademy import dbgems  
        installed_version = dbgems.lookup_current_module_version("dbacademy")
        if installed_version == version:
            pip_command = "list --quiet"  # Skipping pip install of pre-installed python library
        else:
            print(f"WARNING: The wrong version of dbacademy is attached to this cluster. Expected {version}, found {installed_version}.")
            print(f"Installing the correct version.")
            raise Exception("Forcing re-install")

    except Exception as e:
        # The import fails if library is not attached to cluster
        if not version.startswith("v"): library_url = f"git+https://github.com/databricks-academy/dbacademy@{version}"
        else: library_url = f"https://github.com/databricks-academy/dbacademy/releases/download/{version}/dbacademy-{version[1:]}-py3-none-any.whl"

        default_command = f"install --quiet --disable-pip-version-check {library_url}"
        pip_command = spark.conf.get("dbacademy.library.install", default_command)

        if pip_command != default_command:
            print(f"WARNING: Using alternative library installation:\\n| default: %pip {default_command}\\n| current: %pip {pip_command}")
        else:
            # We are using the default libraries; next we need to verify that we can reach those libraries.
            __validate_libraries()

__install_libraries()
""".strip()
        self.maxDiff = None
        self.assertEqual(expected_command, actual_command)

    def test_build_install_libraries_cell_errors_v1(self):
        command = r"myversion = \"v9.8.7\"".strip()

        notebook = self.create_notebook()
        notebook.build_install_libraries_cell(i=1, command=command)

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(1, notebook)
        expected = """Expected one and only one line that starts with "version =", found 0."""
        self.assertEqual(expected, notebook.logger.errors[0].message)

    def test_build_install_libraries_cell_errors_v2(self):
        command = f"version = \"v9.8.7\"\nversion=\"v0.0.0\"".strip()

        notebook = self.create_notebook()
        notebook.build_install_libraries_cell(i=3, command=command)

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(1, notebook)
        expected = """Expected one and only one line that starts with "version =", found 2."""
        self.assertEqual(expected, notebook.logger.errors[0].message)

    def test_build_install_libraries_cell_errors_v3(self):
        command = f"version = \"v9.8.7".strip()

        notebook = self.create_notebook()
        notebook.build_install_libraries_cell(i=3, command=command)

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(1, notebook)
        expected = """Cmd #4 | Unable to parse the dbacademy library version for the INSTALL_LIBRARIES directive: version = "v9.8.7."""
        actual = notebook.logger.errors[0].message
        self.assertEqual(expected, actual)

    def test_build_install_libraries_cell_errors_v4(self):
        command = f"version = v9.8.7\"".strip()

        notebook = self.create_notebook()
        notebook.build_install_libraries_cell(i=3, command=command)

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(1, notebook)
        expected = """Cmd #4 | Unable to parse the dbacademy library version for the INSTALL_LIBRARIES directive: version = v9.8.7"."""
        actual = notebook.logger.errors[0].message
        self.assertEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()
