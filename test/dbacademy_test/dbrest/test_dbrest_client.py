# Databricks notebook source
# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

import unittest


def create_client():
    from dbacademy.dbrest import DBAcademyRestClient
    import os
    import configparser

    for path in ('.databrickscfg', '~/.databrickscfg'):
        path = os.path.expanduser(path)
        if not os.path.exists(path):
            continue
        config = configparser.ConfigParser()
        config.read(path)
        if 'DEFAULT' not in config:
            print('No Default')
            continue
        host = config['DEFAULT']['host'].rstrip("/")
        token = config['DEFAULT']['token']
        return DBAcademyRestClient(token, host)
    return DBAcademyRestClient()


databricks = create_client()


class TestDBAcademyRestClient(unittest.TestCase):
    """
    General test of API connectivity for each of the main Databricks Workspace Rest APIs.
    """

    def testParentheses(self):
        result = databricks().workspace().ls("/")
        self.assertIsInstance(result, list)

    def testWorkspace(self):
        result = databricks.workspace.ls("/")
        self.assertIsInstance(result, list)

    def testClusters(self):
        result = databricks.clusters.list()
        self.assertIsNotNone(result)

    def testJobs(self):
        result = databricks.jobs.list()
        self.assertIsInstance(result, list)

    def testPermissions(self):
        jobs = databricks.jobs.list()
        if not jobs:
            return
        job_id = jobs[0]["job_id"]
        result = databricks.permissions.jobs.get(job_id)
        self.assertIsNotNone(result)

    def testPipelines(self):
        result = databricks.pipelines.list()
        self.assertIsInstance(result, list)

    def testRepos(self):
        result = databricks.repos.list()
        self.assertIsNotNone(result)

    def testRuns(self):
        result = databricks.runs.list()
        self.assertIsInstance(result, list)

    def testUsers(self):
        result = databricks.scim.users.list()
        self.assertIsInstance(result, list)

    def testGroups(self):
        result = databricks.scim.groups.list()
        self.assertIsInstance(result, list)

    def testSqlWarehouses(self):
        result = databricks.sql.endpoints.list()
        self.assertIsInstance(result, list)

    def testTokens(self):
        result = databricks.tokens.list()
        self.assertIsInstance(result, list)

    def testLegacyExecuteGet(self):
        result = databricks.workspace.get_status("/")
        self.assertIsNotNone(result)
        self.assertEqual(result.get("object_type"), "DIRECTORY")
        result = databricks.workspace.get_status("/Does_Not_Exist")
        self.assertIsNone(result)

    def testExportNotebook(self):
        # TODO: Upload a test notebook to a known test path.
        export_path = "/Users/doug.bateman@databricks.com/_Projects/API/Monitor-Classrooms"
        notebook = databricks.workspace.export_notebook(export_path)
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
        buffer = databricks.workspace.export_dbc(export_path)
        self.assertIsInstance(buffer, bytes)
        from zipfile import ZipFile
        from io import BytesIO
        with ZipFile(BytesIO(buffer)) as z:
            files = z.filelist
            self.assertEqual(files[0].filename, notebook_path)
            notebook_bytes = z.read(notebook_path)
        notebook_json = notebook_bytes.decode("UTF-8")
        notebook_data = json.loads(notebook_bytes.decode("UTF-8"))
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
