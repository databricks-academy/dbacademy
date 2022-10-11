# Databricks notebook source
# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

import unittest

from dbacademy.dougrest import AccountsApi


class TestAccountsApi(unittest.TestCase):
    """
    General test of API connectivity for each of the main Databricks Workspace Rest APIs.
    """

    def testListWorkspaces(self):
        accounts = AccountsApi.default_account
        result = accounts.workspaces.list()
        self.assertIsInstance(result, list)

    def testWorkspaceAsDatabricksApi(self):
        accounts = AccountsApi.default_account
        ws = accounts.workspaces.list()[0]
        result = ws.workspace.list("/")
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
