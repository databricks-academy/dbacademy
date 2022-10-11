# Databricks notebook source
# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

import unittest

from dbacademy.dougrest import databricks


class TestPermissionsApi(unittest.TestCase):
    """
    Test the Databricks Permissions APIs
    """

    def testClusterPolicyPermissions(self):
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
        policy = databricks.clusters.policies.get_by_name("test-policy")
        if policy is None:
            policy = databricks.clusters.policies.create("test-policy", policy_spec)
        policy_id = policy["policy_id"]
        levels = databricks.permissions.clusters.policies.get_levels(policy_id)
        databricks.permissions.clusters.policies.update(policy_id, "group_name", "users", "CAN_USE")
        acl = databricks.permissions.clusters.policies.get(policy_id)["access_control_list"]
        found = False
        for ac in acl:
            if ac.get("group_name") == "users":
                for perm in ac["all_permissions"]:
                    if perm["permission_level"] == "CAN_USE" and not perm["inherited"]:
                        found = True
        self.assertTrue(found)
        databricks.clusters.policies.delete_by_id(policy_id)
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
