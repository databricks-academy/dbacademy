# Databricks notebook source
# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

import unittest

from dbacademy.rest.factory import dougrest_factory


class TestDatabricksApiClient(unittest.TestCase):
    """
    General test of API connectivity for each of the main Databricks Workspace Rest APIs.
    """

    def testWorkspace(self):
        ws = dougrest_factory.test_client()
        result = ws.workspace.list("/")
        self.assertIsInstance(result, list)

    def testClusters(self):
        ws = dougrest_factory.test_client()
        result = ws.clusters.list()
        self.assertIsNotNone(result)

    def testJobs(self):
        ws = dougrest_factory.test_client()
        result = ws.jobs.list()
        result = list(result)
        if not result:
            return
        job = result[0]
        job_id = job.get("job_id", "DoesNotExist")
        self.assertIsInstance(job_id, int)

    def testRepos(self):
        ws = dougrest_factory.test_client()
        result = ws.repos.list()
        self.assertIsNotNone(result)

    def testUsers(self):
        ws = dougrest_factory.test_client()
        result = ws.users.list()
        self.assertIsInstance(result, list)

    def testGroups(self):
        ws = dougrest_factory.test_client()
        result = ws.scim.groups.list()
        self.assertIsInstance(result, list)

    def testSqlWarehouses(self):
        ws = dougrest_factory.test_client()
        result = ws.sql.endpoints.list()
        self.assertIsInstance(result, list)

    def testScim(self):
        ws = dougrest_factory.test_client()
        result = ws.scim.groups.list()
        self.assertIsInstance(result, list)

    def testPools(self):
        ws = dougrest_factory.test_client()
        result = ws.pools.list()
        self.assertIsInstance(result, list)

    def testMlFlow(self):
        ws = dougrest_factory.test_client()
        response = ws.mlflow.registered_models.list()
        result = list(response)
        self.assertIsInstance(result, list)
        self.assertTrue(result)

    def testPermissions(self):
        ws = dougrest_factory.test_client()
        pool_id = ws.pools.list()[0]["instance_pool_id"]
        result = ws.permissions.pools.get_levels(pool_id)
        self.assertTrue(result)


# COMMAND ----------

def main():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestDatabricksApiClient))
    runner = unittest.TextTestRunner()
    runner.run(suite)


# COMMAND ----------

if __name__ == '__main__':
    main()
