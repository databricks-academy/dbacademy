# Databricks notebook source
# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

import unittest

from dbacademy.dougrest import databricks


class TestHighLevelFeatures(unittest.TestCase):
    """
    General test of API connectivity for each of the main Databricks Workspace Rest APIs.
    """

    def testWorkspace(self):
        result = databricks.workspace.list("/")
        self.assertIsInstance(result, list)

    def testClusters(self):
        result = databricks.clusters.list()
        self.assertIsNotNone(result)

    def testJobs(self):
        result = databricks.jobs.list()
        self.assertIsInstance(result, list)

    def testRepos(self):
        result = databricks.repos.list()
        self.assertIsNotNone(result)

    def testUsers(self):
        result = databricks.users.list()
        self.assertIsInstance(result, list)

    def testGroups(self):
        result = databricks.scim.groups.list()
        self.assertIsInstance(result, list)

    def testSqlWarehouses(self):
        result = databricks.sql.endpoints.list()
        self.assertIsInstance(result, list)

    def testScim(self):
        result = databricks.scim.groups.list()
        self.assertIsInstance(result, list)

    def testPools(self):
        result = databricks.pools.list()
        self.assertIsInstance(result, list)

    def testMlFlow(self):
        result = list(databricks.mlflow.registered_models.list())
        self.assertIsInstance(result, list)
        self.assertTrue(result)

    def testPermissions(self):
        pool_id = databricks.pools.list()[0]["instance_pool_id"]
        databricks.permissions.pools.get_levels(pool_id)


# COMMAND ----------

def main():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestHighLevelFeatures))
    runner = unittest.TextTestRunner()
    runner.run(suite)


# COMMAND ----------

if __name__ == '__main__':
    main()
