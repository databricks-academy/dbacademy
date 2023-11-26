import unittest
from dbacademy.clients.dbrest.accounts_client import from_args
from dbacademy_test.clients.dbrest import DBACADEMY_UNIT_TESTS


class TestAccountsApi(unittest.TestCase):
    """
    General test of API connectivity for each of the main Databricks Workspace Rest APIs.
    """

    def setUp(self) -> None:
        from dbacademy.common import Cloud
        self.__client = from_args(scope=DBACADEMY_UNIT_TESTS,
                                  cloud=Cloud.AWS)

    @property
    def client(self):
        return self.__client

    def test_list_workspaces(self):
        workspaces = self.__client.workspaces.list()
        self.assertIsInstance(workspaces, list)
        self.assertTrue(len(workspaces) > 0)
        for workspace in workspaces:
            self.assertEqual("b6e87bd6-c65c-46d3-abde-6840bf706d39", workspace.get("account_id"))

    def test_list_users(self):
        users = self.__client.scim.users.list(users_per_request=10)
        self.assertIsInstance(users, list)
        self.assertTrue(len(users) > 0)
        for user in users:
            self.assertIsNotNone(user.get("userName"))

    def test_list_metastores(self):
        metastores = self.__client.metastores.list()
        self.assertIsInstance(metastores, list)
        self.assertTrue(len(metastores) > 1)
        for metastore in metastores:
            self.assertEqual("aws", metastore.get("cloud"))


suite = unittest.TestSuite()
suite.addTest(unittest.makeSuite(TestAccountsApi))


def main():
    runner = unittest.TextTestRunner()
    runner.run(suite)


if __name__ == '__main__':
    main()
