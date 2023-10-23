# Databricks notebook source
# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

import unittest

from dbacademy.clients.rest.factory import dbrest_factory
from dbacademy_test.clients.databricks import DBACADEMY_UNIT_TESTS


class TestDBAcademyRestClient(unittest.TestCase):
    """
    General test of API connectivity for each of the main Databricks Workspace Rest APIs.
    """

    def test_create(self):
        from dbacademy.clients import databricks

        client = databricks.from_token(scope=DBACADEMY_UNIT_TESTS)
        self.assertIsNotNone(client)
        self.assertEqual("https://curriculum-unit-tests.cloud.databricks.com", client.endpoint)

        client = databricks.from_token(scope=DBACADEMY_UNIT_TESTS, endpoint="https://curriculum-unit-tests.cloud.databricks.com")
        self.assertIsNotNone(client)
        self.assertEqual("https://curriculum-unit-tests.cloud.databricks.com", client.endpoint)

        client = databricks.from_token(scope=DBACADEMY_UNIT_TESTS, endpoint="https://curriculum-unit-tests.cloud.databricks.com/")
        self.assertIsNotNone(client)
        self.assertEqual("https://curriculum-unit-tests.cloud.databricks.com", client.endpoint)

        client = databricks.from_token(scope=DBACADEMY_UNIT_TESTS, endpoint="https://curriculum-unit-tests.cloud.databricks.com/api")
        self.assertIsNotNone(client)
        self.assertEqual("https://curriculum-unit-tests.cloud.databricks.com", client.endpoint)

        client = databricks.from_token(scope=DBACADEMY_UNIT_TESTS, endpoint="https://curriculum-unit-tests.cloud.databricks.com/api/")
        self.assertIsNotNone(client)
        self.assertEqual("https://curriculum-unit-tests.cloud.databricks.com", client.endpoint)

    def testParentheses(self):
        ws = dbrest_factory.test_client()
        result = ws().workspace().ls("/")
        self.assertIsInstance(result, list)

    def testWorkspace(self):
        ws = dbrest_factory.test_client()
        result = ws.workspace.ls("/")
        self.assertIsInstance(result, list)

    def testClusters(self):
        ws = dbrest_factory.test_client()
        result = ws.clusters.list_clusters()
        self.assertIsNotNone(result)

    def testJobs(self):
        ws = dbrest_factory.test_client()
        result = ws.jobs.list()
        self.assertIsInstance(result, list)

    def testPermissions(self):
        ws = dbrest_factory.test_client()
        jobs = ws.jobs.list()
        if not jobs:
            return
        job_id = jobs[0]["job_id"]
        result = ws.permissions.jobs.get(job_id)
        self.assertIsNotNone(result)

    def testPipelines(self):
        ws = dbrest_factory.test_client()
        result = ws.pipelines.list()
        self.assertIsInstance(result, list)

    def testRepos(self):
        ws = dbrest_factory.test_client()
        result = ws.repos.list()
        self.assertIsNotNone(result)

    def testRuns(self):
        ws = dbrest_factory.test_client()
        result = ws.runs.list()
        self.assertIsInstance(result, list)

    def testUsers(self):
        ws = dbrest_factory.test_client()
        result = ws.scim.users.list()
        self.assertIsInstance(result, list)

    def testGroups(self):
        ws = dbrest_factory.test_client()
        result = ws.scim.groups.list()
        self.assertIsInstance(result, list)

    def testSqlWarehouses(self):
        ws = dbrest_factory.test_client()
        result = ws.sql.warehouses.list()
        self.assertIsInstance(result, list)

    def testTokens(self):
        ws = dbrest_factory.test_client()
        result = ws.tokens.list()
        self.assertIsInstance(result, list)

    def testLegacyExecuteGet(self):
        ws = dbrest_factory.test_client()
        result = ws.workspace.get_status("/")
        self.assertIsNotNone(result)
        self.assertEqual(result.get("object_type"), "DIRECTORY")
        result = ws.workspace.get_status("/Does_Not_Exist")
        self.assertIsNone(result)

    def testExportNotebook(self):
        # TODO: Upload a test notebook to a known test path.
        export_path = "/Users/doug.bateman@databricks.com/_Projects/API/Monitor-Classrooms"
        ws = dbrest_factory.test_client()
        notebook = ws.workspace.export_notebook(export_path)
        self.assertIsInstance(notebook, str)
        lines = notebook.split("\n")
        self.assertEqual(lines[0], "# Databricks notebook source")

    def testExportDBC(self):
        # TODO: Upload a test notebook to a known test path.
        import json
        export_path = "/Users/doug.bateman@databricks.com/_Projects/API/Monitor-Classrooms"
        notebook_name = "Monitor-Classrooms"
        notebook_type = "python"
        notebook_path = f"{notebook_name}.{notebook_type}"
        ws = dbrest_factory.test_client()
        buffer = ws.workspace.export_dbc(export_path)
        self.assertIsInstance(buffer, bytes)
        from zipfile import ZipFile
        from io import BytesIO
        with ZipFile(BytesIO(buffer)) as z:
            files = z.filelist
            self.assertEqual(files[0].filename, notebook_path)
            notebook_bytes = z.read(notebook_path)

        notebook_json = notebook_bytes.decode("UTF-8")
        notebook_data = json.loads(notebook_json)
        self.assertEqual(notebook_data.get("name"), notebook_name)
        self.assertIsInstance(notebook_data.get("commands"), list, "Malformed Notebook JSON")


# COMMAND ----------

def main():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestDBAcademyRestClient))
    runner = unittest.TextTestRunner()
    runner.run(suite)


# COMMAND ----------

if __name__ == '__main__':
    main()
