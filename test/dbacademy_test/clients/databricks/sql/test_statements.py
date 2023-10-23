__all__ = ["StatementTests"]

import unittest
from dbacademy.clients import databricks
from dbacademy.dbhelper import WorkspaceHelper
from dbacademy.clients.databricks.sql.endpoints import CLUSTER_SIZE_2X_SMALL, RELIABILITY_OPTIMIZED, CHANNEL_NAME_CURRENT
from dbacademy_test.clients.databricks import DBACADEMY_UNIT_TESTS


class StatementTests(unittest.TestCase):

    warehouse_id: str = None
    client = databricks.none_reference()

    @classmethod
    def setUpClass(cls) -> None:
        cls.client = databricks.from_token(scope=DBACADEMY_UNIT_TESTS)

        warehouse = cls.client.sql.warehouses.create_or_update(
            name="Unit-Tests Warehouse",
            cluster_size=CLUSTER_SIZE_2X_SMALL,
            enable_serverless_compute=True,
            min_num_clusters=1,
            max_num_clusters=1,
            auto_stop_mins=5,
            enable_photon=True,
            spot_instance_policy=RELIABILITY_OPTIMIZED,
            channel=CHANNEL_NAME_CURRENT,
            tags={
                f"dbacademy.{WorkspaceHelper.PARAM_EVENT_ID}": "n/a",
                f"dbacademy.{WorkspaceHelper.PARAM_EVENT_DESCRIPTION}": "SQL warehouse created for unit testing",
                f"dbacademy.workspace": "curriculum-unit-tests.cloud.databricks.com",
                f"dbacademy.org_id": "2967772011441968",
                f"dbacademy.source": "DBAcademy Library Unit-Tests",
            })
        cls.warehouse_id = warehouse.get("id")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.sql.warehouses.delete_by_id(cls.warehouse_id)

    def test_execute(self):
        results = self.client.sql.statements.execute(warehouse_id=self.warehouse_id,
                                                     catalog="main",
                                                     schema="default",
                                                     statement="SHOW DATABASES")
        self.assertIsNotNone(results)
        self.assertEquals("SUCCEEDED", results.get("status", dict()).get("state"))

    def test_get_statement(self):
        self.skipTest("Not yet implemented")

    def test_get_chunk_index(self):
        self.skipTest("Not yet implemented")

    def test_cancel_statement(self):
        self.skipTest("Not yet implemented")


if __name__ == '__main__':
    unittest.main()
