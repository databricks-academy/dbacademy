# Databricks notebook source
# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

import unittest

from dbacademy.clients.rest.factory import dougrest_factory


class TestAccountsApi(unittest.TestCase):
    """
    General test of API connectivity for each of the main Databricks Workspace Rest APIs.
    """

    def testListWorkspaces(self):
        account = dougrest_factory.test_account()
        result = account.workspaces.list()
        self.assertIsInstance(result, list)

    def testWorkspaceAsDatabricksApi(self):
        account = dougrest_factory.test_account()
        ws = account.workspaces.list()[0]
        result = ws.workspace.list("/")
        self.assertIsInstance(result, list)

    def testListUsers(self):
        account = dougrest_factory.test_account()
        result = account.users.list(count=10)
        self.assertIsInstance(result, list)

    def testListMetastores(self):
        account = dougrest_factory.test_account()
        result = account.metastores.list()
        self.assertIsInstance(result, list)


# COMMAND ----------

suite = unittest.TestSuite()
suite.addTest(unittest.makeSuite(TestAccountsApi))


def main():
    runner = unittest.TextTestRunner()
    runner.run(suite)


# COMMAND ----------

if __name__ == '__main__':
    main()
