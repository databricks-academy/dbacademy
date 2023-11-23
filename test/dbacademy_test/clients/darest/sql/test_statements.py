__all__ = ["StatementTests"]

import unittest


class StatementTests(unittest.TestCase):
    from dbacademy.clients import darest

    warehouse_id: str = None
    client: darest.DBAcademyRestClient = None

    @classmethod
    def setUpClass(cls) -> None:
        from dbacademy.clients import darest
        from dbacademy.dbhelper import dbh_constants
        from dbacademy.clients.darest.sql_api.warehouses_api import CLUSTER_SIZE_2X_SMALL, RELIABILITY_OPTIMIZED, CHANNEL_NAME_CURRENT
        from dbacademy_test.clients.darest import DBACADEMY_UNIT_TESTS

        cls.client = darest.from_token(scope=DBACADEMY_UNIT_TESTS)

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
                f"dbacademy.{dbh_constants.WORKSPACE_HELPER.PARAM_EVENT_ID}": "n/a",
                f"dbacademy.{dbh_constants.WORKSPACE_HELPER.PARAM_EVENT_DESCRIPTION}": "SQL warehouse created for unit testing",
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
        self.assertEqual("SUCCEEDED", results.get("status", dict()).get("state"))

    def test_get_statement(self):
        self.skipTest("Not yet implemented")

    def test_get_chunk_index(self):
        self.skipTest("Not yet implemented")

    def test_cancel_statement(self):
        self.skipTest("Not yet implemented")


if __name__ == '__main__':
    unittest.main()
