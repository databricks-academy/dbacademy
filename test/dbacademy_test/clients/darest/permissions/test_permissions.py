import unittest

from dbacademy.clients.darest import DBAcademyRestClient
from dbacademy_test.clients.darest import DBACADEMY_UNIT_TESTS


class TestPermissionsApi(unittest.TestCase):
    """
    Test the Databricks Permissions APIs
    """

    def setUp(self) -> None:
        from dbacademy.clients import darest

        self.__client = darest.from_token(scope=DBACADEMY_UNIT_TESTS)
        self.tearDown()

    @property
    def client(self) -> DBAcademyRestClient:
        return self.__client

    def test_cluster_policy_permissions(self):
        policy_spec = {
            "cluster_type": {
                "type": "fixed",
                "value": "all-purpose",
            },
            "autotermination_minutes": {
                "type": "fixed",
                "value": 120,
                "hidden": False,
            },
        }
        policy = self.client.cluster_policies.get_by_name("test-policy")

        if policy is None:
            policy = self.client.cluster_policies.create("test-policy", policy_spec)

        policy_id = policy["policy_id"]
        levels = self.client.permissions.clusters.policies.get_levels(policy_id)

        self.client.permissions.clusters.policies.update(id_value=policy_id,
                                                         what="group_name",
                                                         value="users",
                                                         permission_level="CAN_USE")

        acl = self.client.permissions.clusters.policies.get(policy_id)["access_control_list"]

        found = False
        for ac in acl:
            if ac.get("group_name") == "users":
                for perm in ac["all_permissions"]:
                    if perm["permission_level"] == "CAN_USE" and not perm["inherited"]:
                        found = True
        self.assertTrue(found)
        self.client.cluster_policies.delete_by_id(policy_id)
        self.assertIsInstance(levels, list)


def main():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestPermissionsApi))
    runner = unittest.TextTestRunner()
    runner.run(suite)


if __name__ == '__main__':
    main()
