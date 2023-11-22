__all__ = ["TestUsers"]

import unittest
from dbacademy.clients import darest
from dbacademy_test.clients.darest import DBACADEMY_UNIT_TESTS


class TestUsers(unittest.TestCase):

    def setUp(self) -> None:
        self.__client = darest.from_token(scope=DBACADEMY_UNIT_TESTS)
        self.tearDown()

    def tearDown(self) -> None:
        pass

    @property
    def client(self):
        return self.__client

    def test_lifecycles(self):
        from dbacademy.clients.rest.common import DatabricksApiException

        mickey = "mickey.mouse@disney.com"
        user = self.client.scim.users.get_by_username(mickey)

        if user is not None:
            # Required simply for execution
            self.client.scim.users.delete_by_username(mickey)

        user = self.client.scim.users.get_by_username(mickey)
        self.assertIsNone(user)  # it shouldn't exist

        user = self.client.scim.users.create(mickey)
        self.assertIsNotNone(user)
        self.assertEqual(mickey, user.get("displayName"))

        # The user will exist at the account level and so the entitlements are still hanging around.
        # user_id = user.get("id")
        user.get("id")
        entitlements = user.get("entitlements", [])
        for entitlement in entitlements:
            # value = entitlement.get("value")
            entitlement.get("value")
            # TODO this is failing
            # self.client.scim.users.remove_entitlement(user_id, value)

        try:
            # Second create should fail
            self.client.scim.users.create(mickey)
            raise Exception("Expected DatabricksApiException")
        except DatabricksApiException as e:
            self.assertTrue("User with username mickey.mouse@disney.com already exists." in e.message)

        # Test entitlements
        # TODO these are not working right if we cannot remove entitlements
        # entitlements = user.get("entitlements")
        # self.assertIsNotNone(entitlements)
        # self.assertEqual(1, len(entitlements))
        # self.assertEqual("allow-cluster-create", entitlements[0].get("value"))

        # Test unacceptable entitlement
        user_id = user.get("id")
        self.client.scim.users.add_entitlement(user_id, "some-random-value")

        # Test get_by_id
        user = self.client.scim.users.get_by_id(user_id)
        entitlements = user.get("entitlements")
        self.assertIsNotNone(entitlements)
        # TODO - still not working 
        # entitlements = user.get("entitlements")
        # self.assertEqual(1, len(entitlements))
        # self.assertEqual("allow-cluster-create", entitlements[0].get("value"))

        # Test acceptable entitlement
        self.client.scim.users.add_entitlement(user_id, "allow-instance-pool-create")
        user = self.client.scim.users.get_by_id(user_id)
        # entitlements = user.get("entitlements", [])
        user.get("entitlements", [])

        # TODO - still nt working
        # entitlements = user.get("entitlements")
        # self.assertEqual(2, len(entitlements))
        # self.assertEqual("allow-cluster-create", entitlements[0].get("value"))
        # self.assertEqual("allow-instance-pool-create", entitlements[1].get("value"))

        # Test delete by id
        self.client.scim.users.delete_by_id(user_id)

        # Test delete by username
        user = self.client.scim.users.create(mickey)
        self.assertIsNotNone(user)
        self.client.scim.users.delete_by_username(mickey)

        user = self.client.scim.users.get_by_username(mickey)
        self.assertIsNone(user)

    def test_get_by_id(self):
        user = self.client.scim.users.get_by_username("jacob.parr@databricks.com")
        self.assertIsNotNone(user)
        self.assertEqual("jacob.parr@databricks.com", user.get("userName"))

        user_id = user.get("id")
        user = self.client.scim.users.get_by_id(user_id)
        self.assertIsNotNone(user)
        self.assertEqual("jacob.parr@databricks.com", user.get("userName"))

    def test_get_by_name(self):
        user = self.client.scim.users.get_by_username("jacob.parr@databricks.com")
        self.assertIsNotNone(user)
        self.assertEqual("jacob.parr@databricks.com", user.get("userName"))

    def test_get_by_username(self):
        user = self.client.scim.users.get_by_username("jacob.parr@databricks.com")
        self.assertIsNotNone(user)
        self.assertEqual("jacob.parr@databricks.com", user.get("userName"))

    def test_help(self):
        # Just make sure we can invoke it.
        # I don't care about the output
        self.client.scim.users.help()

    def test_list(self):
        users = self.client.scim.users.list()
        self.assertTrue(len(users) >= 3)

        found_class = False
        found_jacob = False

        for user in users:
            if user.get("userName") == "class+000@databricks.com":
                found_class = True

            elif user.get("userName") == "jacob.parr@databricks.com":
                found_jacob = True

        self.assertTrue(found_class)
        self.assertTrue(found_jacob)

    def test_me(self):
        user = self.client.scim.me()
        self.assertIsNotNone(user)
        self.assertEqual("d8835420-9797-45f5-897b-6d81d7f80023", user.get("userName"))
        self.assertEqual("Unit Tests", user.get("displayName"))


if __name__ == "__main__":
    unittest.main()
