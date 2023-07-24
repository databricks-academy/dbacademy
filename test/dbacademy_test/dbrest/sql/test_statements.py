import unittest

unit_test_service_principle = "d8835420-9797-45f5-897b-6d81d7f80023"


class StatementTests(unittest.TestCase):
    from dbacademy.dbrest import DBAcademyRestClient

    warehouse_id: str = None
    client: DBAcademyRestClient = None

    @classmethod
    def setUpClass(cls) -> None:
        import os
        from dbacademy.dbhelper import WorkspaceHelper
        from dbacademy.dbrest import DBAcademyRestClient
        from dbacademy.common.unit_tests import DBACADEMY_UNIT_TESTS_API_TOKEN, DBACADEMY_UNIT_TESTS_API_ENDPOINT
        from dbacademy.dbrest.sql.endpoints import CLUSTER_SIZE_2X_SMALL, RELIABILITY_OPTIMIZED, CHANNEL_NAME_CURRENT

        token = os.getenv(DBACADEMY_UNIT_TESTS_API_TOKEN)
        endpoint = os.getenv(DBACADEMY_UNIT_TESTS_API_ENDPOINT)

        if token is None or endpoint is None:
            raise AssertionError(f"Missing {DBACADEMY_UNIT_TESTS_API_TOKEN} or {DBACADEMY_UNIT_TESTS_API_ENDPOINT} environment variables")

        cls.client = DBAcademyRestClient(token=token, endpoint=endpoint)

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
