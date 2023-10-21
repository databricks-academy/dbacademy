__all__ = ["TestAirTableRestClient"]

import unittest
from dbacademy.clients import airtable


class TestAirTableRestClient(unittest.TestCase):

    def test_from_environment(self):

        client = airtable.from_environment(access_token="asdf", base_id="whatever", throttle_seconds=1)
        self.assertIsNotNone(client)
        self.assertEqual("whatever", client.base_id)
        self.assertEqual("Bearer asdf", client.authorization_header)
        self.assertEqual(1, client.throttle_seconds)
        self.assertIsNotNone(client.error_handler)

        client = airtable.from_environment(base_id="test_base")
        self.assertIsNotNone(client)
        self.assertEqual("test_base", client.base_id)
        self.assertTrue(client.authorization_header.startswith("Bearer patvvnvou5"))
        self.assertTrue(client.authorization_header.endswith("3b6d8a6ad7"))
        self.assertEqual(0, client.throttle_seconds)
        self.assertIsNotNone(client.error_handler)

    def test_create(self):

        client = airtable.from_environment(base_id="some_base_id", access_token="some_access_token", throttle_seconds=1)
        self.assertIsNotNone(client)
        self.assertEqual("some_base_id", client.base_id)
        self.assertEqual("Bearer some_access_token", client.authorization_header)
        self.assertEqual(1, client.throttle_seconds)
        self.assertIsNotNone(client.error_handler)

    def test_from_table_config(self):

        config = airtable.TableConfig(base_id="some_base_id", table_id="some_table_id")
        self.assertEqual("some_base_id", config.base_id)
        self.assertEqual("some_table_id", config.table_id)

        table = airtable.from_table_config(config)
        self.assertIsNotNone(table)
        self.assertEqual("some_table_id", table.table_id)


if __name__ == '__main__':
    unittest.main()

