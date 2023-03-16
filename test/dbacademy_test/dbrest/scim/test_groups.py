from typing import List
import unittest

unit_test_service_principle = "d8835420-9797-45f5-897b-6d81d7f80023"


class TestGroups(unittest.TestCase):

    def setUp(self) -> None:
        import os
        from dbacademy.dbrest import DBAcademyRestClient

        token = os.getenv("DBACADEMY_UNIT_TESTS_API_TOKEN", None)
        endpoint = os.getenv("DBACADEMY_UNIT_TESTS_API_ENDPOINT", None)

        if token is None or endpoint is None:
            self.skipTest("Missing DBACADEMY_UNIT_TESTS_API_TOKEN or DBACADEMY_UNIT_TESTS_API_ENDPOINT environment variables")

        self.__client = DBAcademyRestClient(token=token, endpoint=endpoint)

        self.tearDown()

    def tearDown(self) -> None:
        pass

    @property
    def client(self):
        return self.__client

    def test_list(self):
        groups = self.client.scim.groups.list()
        self.assertIsNotNone(groups)
        self.assertTrue(len(groups) >= 2)

        found_admin = False
        found_users = False

        for group in groups:
            if group.get("displayName") == "admins":
                found_admin = True
            elif group.get("displayName") == "users":
                found_users = True

        self.assertTrue(found_admin)
        self.assertTrue(found_users)

    def test_get_by_id(self):
        for group in self.client.scim.groups.list():
            id = group.get("id")
            g = self.client.scim.groups.get_by_id(id)
            self.assertEqual(id, g.get("id"))

    def test_get_by_name(self):
        for group in self.client.scim.groups.list():
            name = group.get("displayName")
            g = self.client.scim.groups.get_by_name(name)
            self.assertEqual(name, g.get("displayName"))

    def test_delete_by_id(self):
        name = "dummies"
        group = self.client.scim.groups.get_by_name(name)
        group = group or self.client.scim.groups.create(name)

        id = group.get("id")
        self.client.scim.groups.delete_by_id(id)

        group = self.client.scim.groups.get_by_id(id)
        self.assertIsNone(group)

    def test_delete_by_name(self):
        name = "dummies"
        group = self.client.scim.groups.get_by_name(name)
        group = group or self.client.scim.groups.create(name)

        name = group.get("displayName")
        self.client.scim.groups.delete_by_name(name)

        group = self.client.scim.groups.get_by_name(name)
        self.assertIsNone(group)

    def test_create(self):
        from dbacademy.rest.common import DatabricksApiException

        name = "dummies"
        if self.client.scim.groups.get_by_name(name) is not None:
            self.client.scim.groups.delete_by_name(name)

        group = self.client.scim.groups.create(name)
        self.assertIsNotNone(group)
        self.assertEqual(name, group.get("displayName"))

        try:
            # Second create should fail
            self.client.scim.groups.create(name)
            raise Exception("Expected exception")
        except DatabricksApiException as e:
            self.assertTrue("Group with name dummies already exists." in e.message)

    def test_add_member(self):
        self.skipTest("Not implemented")

    def test_add_entitlement(self):
        self.skipTest("Not implemented")

    def test_remove_entitlement(self):
        self.skipTest("Not implemented")


if __name__ == "__main__":
    unittest.main()
