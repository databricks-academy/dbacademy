import unittest
from dbacademy.dbbuild.publish.notebook_def import NotebookDef


class TestNotebookDefI18NTitle(unittest.TestCase):

    def __init__(self, method_name):
        super().__init__(method_name)

    def assert_n_errors(self, expected, notebook: NotebookDef):
        message = f"Expected {expected} errors, found {len(notebook.logger.errors)}"
        for error in notebook.logger.errors:
            message += f"\n{error.message}"

        self.assertEqual(expected, len(notebook.logger.errors), message)

    def assert_n_warnings(self, expected, notebook: NotebookDef):
        message = f"Expected {expected} errors, found {len(notebook.logger.warnings)}"
        for warning in notebook.logger.warnings:
            message += f"\n{warning.message}"

        self.assertEqual(expected, len(notebook.logger.warnings), message)

    @staticmethod
    def create_notebook():
        from dbacademy.dbbuild.build_config import BuildConfig

        build_config = BuildConfig(name="Unit Test", version="1.2.3")

        return NotebookDef(client=build_config.client,
                           path="Agenda",
                           replacements={},
                           include_solution=False,
                           test_round=2,
                           ignored=False,
                           order=0,
                           i18n=True,
                           i18n_language="English",
                           ignoring=[],
                           version=build_config.version)

    def test_good_no_space_i18n(self):
        from dbacademy.dbbuild.publish.notebook_def import StateVariables

        command = """
# DBTITLE 1,--i18n-TBD
# MAGIC %md
# MAGIC 
# MAGIC # Build-Time Substitutions""".strip()

        notebook = self.create_notebook()
        state = StateVariables()
        state.i18n_guid_map = {"--i18n-TBD": "whatever"}
        notebook.update_command(state=state, language="Python", command=command, i=3, other_notebooks=list(), debugging=False)

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(0, notebook)

        self.assertEqual(1, len(notebook.i18n_guids), f"Expected 1 GUID, found {len(notebook.i18n_guids)}")
        self.assertEqual("--i18n-TBD", notebook.i18n_guids[0])

    def test_good_single_space_i18n(self):
        from dbacademy.dbbuild.publish.notebook_def import StateVariables

        command = """
# DBTITLE 1, --i18n-TBD
# MAGIC %md
# MAGIC 
# MAGIC # Build-Time Substitutions""".strip()

        notebook = self.create_notebook()
        state = StateVariables()
        state.i18n_guid_map = {"--i18n-TBD": "whatever"}
        notebook.update_command(state=state, language="Python", command=command, i=3, other_notebooks=list(), debugging=False)

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(0, notebook)

        self.assertEqual(1, len(notebook.i18n_guids), f"Expected 1 GUID, found {len(notebook.i18n_guids)}")
        self.assertEqual("--i18n-TBD", notebook.i18n_guids[0])

    def test_good_double_spaced_i18n(self):
        from dbacademy.dbbuild.publish.notebook_def import StateVariables

        command = """
# DBTITLE 1,  --i18n-TBD
# MAGIC %md
# MAGIC 
# MAGIC # Build-Time Substitutions""".strip()

        notebook = self.create_notebook()
        state = StateVariables()
        state.i18n_guid_map = {"--i18n-TBD": "whatever"}
        notebook.update_command(state=state, language="Python", command=command, i=3, other_notebooks=list(), debugging=False)

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(0, notebook)

        self.assertEqual(1, len(notebook.i18n_guids), f"Expected 1 GUID, found {len(notebook.i18n_guids)}")
        self.assertEqual("--i18n-TBD", notebook.i18n_guids[0])

    def test_good_md_sandbox_i18n(self):
        from dbacademy.dbbuild.publish.notebook_def import StateVariables

        command = """
# DBTITLE 1,--i18n-TBD
# MAGIC %md-sandbox
# MAGIC 
# MAGIC # Build-Time Substitutions""".strip()

        notebook = self.create_notebook()
        state = StateVariables()
        state.i18n_guid_map = {"--i18n-TBD": "whatever"}
        notebook.update_command(state=state, language="Python", command=command, i=3, other_notebooks=list(), debugging=False)

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(0, notebook)

        self.assertEqual(1, len(notebook.i18n_guids), f"Expected 1 GUID, found {len(notebook.i18n_guids)}")
        self.assertEqual("--i18n-TBD", notebook.i18n_guids[0])

    def test_missing_title(self):
        from dbacademy.dbbuild.publish.notebook_def import StateVariables

        command = """
# MAGIC %md
# MAGIC 
# MAGIC # Build-Time Substitutions""".strip()

        notebook = self.create_notebook()
        state = StateVariables()
        state.i18n_guid_map = {"--i18n-TBD": "whatever"}
        notebook.update_command(state=state, language="Python", command=command, i=3, other_notebooks=list(), debugging=False)

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(1, notebook)

        self.assertEqual("Cmd #4 | Missing the i18n directive: %md", notebook.logger.errors[0].message)

    def test_funky_title(self):
        from dbacademy.dbbuild.publish.notebook_def import StateVariables

        command = """
# DBTITLE 1
# MAGIC %md
# MAGIC 
# MAGIC # Build-Time Substitutions""".strip()

        notebook = self.create_notebook()
        state = StateVariables()
        state.i18n_guid_map = {"--i18n-TBD": "whatever"}
        notebook.update_command(state=state, language="Python", command=command, i=3, other_notebooks=list(), debugging=False)

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(1, notebook)

        self.assertEqual("Cmd #4 | Missing the i18n directive, no title.", notebook.logger.errors[0].message)

    def test_missing_i18n(self):
        from dbacademy.dbbuild.publish.notebook_def import StateVariables

        command = """
# DBTITLE 1,
# MAGIC %md
# MAGIC 
# MAGIC # Build-Time Substitutions""".strip()

        notebook = self.create_notebook()
        state = StateVariables()
        state.i18n_guid_map = {"--i18n-TBD": "whatever"}
        notebook.update_command(state=state, language="Python", command=command, i=3, other_notebooks=list(), debugging=False)

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(1, notebook)

        self.assertEqual("Cmd #4 | Missing the i18n directive, no title.", notebook.logger.errors[0].message)

    # def test_missing_i18n_single(self):
    #     command = "# MAGIC %md | # Build-Time Substitutions".strip()
    #
    #     notebook = self.create_notebook()
    #     notebook.update_command(state=state, language="Python", command=command, i=3, i18n_guid_map={"--i18n-TBD": "whatever"}, other_notebooks=[])
    #
    #     self.assert_n_warnings(0, notebook)
    #     self.assert_n_errors(1, notebook)
    #
    #     self.assertEqual("Cmd #4 | Expected MD to have more than 1 line of code with i18n enabled: %md | # Build-Time Substitutions", notebook.errors[0].message)

    def test_extra_word_i18n(self):
        from dbacademy.dbbuild.publish.notebook_def import StateVariables

        command = """
# DBTITLE 1,--i18n-TBD some garbage
# MAGIC %md # Title
# MAGIC 
# MAGIC # Build-Time Substitutions""".strip()

        notebook = self.create_notebook()
        state = StateVariables()
        state.i18n_guid_map = {"--i18n-TBD": "whatever"}
        notebook.update_command(state=state, language="Python", command=command, i=3, other_notebooks=list(), debugging=False)

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(1, notebook)

        self.assertEqual("Cmd #4 | Expected the title to have only one word, found 3: --i18n-TBD some garbage", notebook.logger.errors[0].message)

    def test_duplicate_i18n_guid(self):
        from dbacademy.dbbuild.publish.notebook_def import StateVariables

        command_a = """
# DBTITLE 1,--i18n-a6e39b59-1715-4750-bd5d-5d638cf57c3a
# MAGIC %md
# MAGIC # Some Title""".strip()
        command_b = """
# DBTITLE 1,--i18n-a6e39b59-1715-4750-bd5d-5d638cf57c3a
# MAGIC %md
# MAGIC # Some Title""".strip()

        notebook = self.create_notebook()
        state = StateVariables()
        state.i18n_guid_map = {
            "--i18n-a6e39b59-1715-4750-bd5d-5d638cf57c3a": "whatever"
        }
        notebook.update_command(state=state, language="Python", command=command_a, i=3, other_notebooks=list(), debugging=False)
        notebook.update_command(state=state, language="Python", command=command_b, i=4, other_notebooks=list(), debugging=False)

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(1, notebook)

        self.assertEqual("Cmd #5 | Duplicate i18n GUID found: --i18n-a6e39b59-1715-4750-bd5d-5d638cf57c3a", notebook.logger.errors[0].message)

    def test_unique_i18n_guid(self):
        from dbacademy.dbbuild.publish.notebook_def import StateVariables

        guid_a = "--i18n-a6e39b59-1715-4750-bd5d-5d638cf57c3a"
        command_a = f"""
# DBTITLE 1,{guid_a}
# MAGIC %md
# MAGIC # Some Title""".strip()

        guid_b = "--i18n-9d06d80d-2381-42d5-8f9e-cc99ee3cd82a"
        command_b = f"""
# DBTITLE 1,{guid_b}
# MAGIC %md
# MAGIC # Some Title""".strip()

        notebook = self.create_notebook()
        state = StateVariables()
        state.i18n_guid_map = {
            guid_a: "# MAGIC # Some Title",
            guid_b: "# MAGIC # Some Title"
        }
        notebook.update_command(state=state, language="Python", command=command_a, i=3, other_notebooks=list(), debugging=False)
        notebook.update_command(state=state, language="Python", command=command_b, i=4, other_notebooks=list(), debugging=False)

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(0, notebook)

    def test_md_i18n_guid_replacement(self):
        from dbacademy.dbbuild.publish.notebook_def import StateVariables

        guid = "--i18n-a6e39b59-1715-4750-bd5d-5d638cf57c3a"
        command = f"""
# DBTITLE 1,{guid}
# MAGIC %md
# MAGIC # Some Title
""".strip()

        notebook = self.create_notebook()
        state = StateVariables()
        state.i18n_guid_map = {guid: f"Some other language"}
        actual = notebook.update_command(state=state, language="Python", command=command, i=4, other_notebooks=list(), debugging=False)

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(0, notebook)

        self.assertEqual(f"# DBTITLE 0,{guid}\nSome other language", actual)

    def test_md_sandbox_i18n_guid_replacement(self):
        from dbacademy.dbbuild.publish.notebook_def import StateVariables

        guid = "--i18n-a6e39b59-1715-4750-bd5d-5d638cf57c3a"
        command = f"""
# DBTITLE 1,{guid}
# MAGIC %md-sandbox
# MAGIC # Some Title""".strip()

        notebook = self.create_notebook()
        state = StateVariables()
        state.i18n_guid_map = {guid: f"Some other language"}
        actual = notebook.update_command(state=state, language="Python", command=command, i=4, other_notebooks=list(), debugging=False)

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(0, notebook)

        self.assertEqual(f"# DBTITLE 0,{guid}\nSome other language", actual)

    def test_i18n_sql(self):
        from dbacademy.dbbuild.publish.notebook_def import StateVariables

        command = """
-- DBTITLE 1,--i18n-a6e39b59-1715-4750-bd5d-5d638cf57c3a
-- MAGIC %md-sandbox
-- MAGIC # Some Title
""".strip()

        notebook = self.create_notebook()
        state = StateVariables()
        state.i18n_guid_map = {"--i18n-a6e39b59-1715-4750-bd5d-5d638cf57c3a": "-- MAGIC %md-sandbox\n-- MAGIC # Some Title"}
        actual = notebook.update_command(state=state, language="SQL", command=command, i=4, other_notebooks=list(), debugging=False)

        self.assert_n_warnings(0, notebook)
        self.assert_n_errors(0, notebook)

        expected = """
-- DBTITLE 0,--i18n-a6e39b59-1715-4750-bd5d-5d638cf57c3a
-- MAGIC %md-sandbox
-- MAGIC # Some Title""".strip()
        self.assertEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()
