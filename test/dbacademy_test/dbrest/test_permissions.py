# Databricks notebook source
# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

import unittest

from dbacademy.rest.factory import dougrest_factory


class TestPermissionsApi(unittest.TestCase):
    """
    Test the Databricks Permissions APIs
    """

    def testClusterPolicyPermissions(self):
        ws = dougrest_factory.default_client()
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
        policy = ws.clusters.policies.get_by_name("test-policy")
        if policy is None:
            policy = ws.clusters.policies.create("test-policy", policy_spec)
        policy_id = policy["policy_id"]
        levels = ws.permissions.clusters.policies.get_levels(policy_id)
        ws.permissions.clusters.policies.update(policy_id, "group_name", "users", "CAN_USE")
        acl = ws.permissions.clusters.policies.get(policy_id)["access_control_list"]
        found = False
        for ac in acl:
            if ac.get("group_name") == "users":
                for perm in ac["all_permissions"]:
                    if perm["permission_level"] == "CAN_USE" and not perm["inherited"]:
                        found = True
        self.assertTrue(found)
        ws.clusters.policies.delete_by_id(policy_id)
        self.assertIsInstance(levels, list)


# COMMAND ----------

def main():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestPermissionsApi))
    runner = unittest.TextTestRunner()
    runner.run(suite)


# COMMAND ----------

if __name__ == '__main__':
    main()
