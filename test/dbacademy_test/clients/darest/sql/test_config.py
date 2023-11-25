__all__ = ["SqlConfigTests"]

import unittest
from typing import Dict, Any
from dbacademy.clients import dbrest
from dbacademy_test.clients.darest import DBACADEMY_UNIT_TESTS


class SqlConfigTests(unittest.TestCase):

    warehouse_id: str = None
    client: dbrest.DBAcademyRestClient = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.client = dbrest.from_token(scope=DBACADEMY_UNIT_TESTS)

    def test_get(self):
        config = self.client.sql.config.get()

        self.assertIsNotNone(config)
        self.assertEqual("DATA_ACCESS_CONTROL", config.get("security_policy"))
        self.assertEqual(True, config.get("enable_serverless_compute"))

    def test_update_all(self):
        config = self.client.sql.config.get()
        enable_serverless_compute = config.get("enable_serverless_compute")

        # Toggle value and update
        config["enable_serverless_compute"] = not enable_serverless_compute
        self.client.sql.config.update_all(config)
        self.__validate_config(self.client.sql.config.get(), not enable_serverless_compute)

        # Put the value back
        config["enable_serverless_compute"] = True
        self.client.sql.config.update_all(config)
        self.__validate_config(self.client.sql.config.get(), True)

    def test_update(self):
        config = self.client.sql.config.get()
        enable_serverless_compute = config.get("enable_serverless_compute")

        # Toggle the one parameter & validate
        self.client.sql.config.update("enable_serverless_compute", not enable_serverless_compute)
        self.__validate_config(self.client.sql.config.get(), not enable_serverless_compute)

        # Put the value back & validate
        self.client.sql.config.update("enable_serverless_compute", True)
        self.__validate_config(self.client.sql.config.get(), True)

    def __validate_config(self, config: Dict[str, Any], enable_serverless_compute: bool):
        self.assertEqual("DATA_ACCESS_CONTROL", config.get("security_policy"))
        self.assertEqual(enable_serverless_compute, config.get("enable_serverless_compute"))
        self.assertEqual(0,  len(config.get("sql_configuration_parameters")))

        enabled_types = config.get("enabled_warehouse_types")
        self.assertEqual(2,  len(enabled_types))

        for i in range(0, len(enabled_types)):
            enabled_type = enabled_types[i]
            self.assertEqual(2, len(enabled_type))
            self.assertTrue(enabled_type.get("warehouse_type"), ["CLASSIC", "PRO"])
            self.assertTrue(enabled_type.get("enabled"))


if __name__ == '__main__':
    unittest.main()
