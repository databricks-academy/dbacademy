__all__ = ["TestAirClientTable"]

import unittest
from dbacademy.clients import airtable

smoke_test_config = airtable.TableConfig(base_id="appijNwbRAAYFLcQr",    # Smoke-Tests
                                         table_id="tblmBKMz2uyFdM2Dj")   # Test-Table


class TestAirClientTable(unittest.TestCase):

    def setUp(self) -> None:
        self.tables_api = self.tables_api = airtable.from_table_config(smoke_test_config)
        self.assertIsNotNone(self.tables_api)
        self.assertEqual(self.tables_api.table_id, smoke_test_config.table_id)

    def test_read(self):
        records = self.tables_api.query()

        self.assertIsNotNone(records)
        self.assertTrue(len(records) > 3, f"Expected at least 3, found {len(records)}")

    def test_read_sorted(self):
        records = self.tables_api.query(sort_by="id")

        self.assertIsNotNone(records)
        self.assertTrue(len(records) > 3, f"Expected at least 3, found {len(records)}")
        self.assertEquals(1, records[0].get("fields").get("id"))
        self.assertEquals(2, records[1].get("fields").get("id"))
        self.assertEquals(3, records[2].get("fields").get("id"))

    def test_read_filtered(self):
        records = self.tables_api.query(filter_by_formula="id = 2")

        self.assertIsNotNone(records)
        self.assertEquals(1, len(records))
        self.assertEquals(2, records[0].get("fields").get("id"))

        url = "https://training-classroom-767-knzbh.cloud.databricks.com"
        records = self.tables_api.query(filter_by_formula=f"{{AWS Workspace URL}} = '{url}'")
        self.assertIsNotNone(records)
        self.assertEquals(1, len(records))
        self.assertEquals(url, records[0].get("fields").get("AWS Workspace URL"))

    def test_read_sorted_filtered(self):
        records = self.tables_api.query(filter_by_formula="Assignee = 'Jacob Parr'", sort_by="id")

        self.assertIsNotNone(records)
        self.assertEquals(2, len(records))

        self.assertEquals(1, records[0].get("fields").get("id"))
        self.assertEquals("Jacob Parr", records[0].get("fields").get("Assignee").get("name"))

        self.assertEquals(1, records[0].get("fields").get("id"))
        self.assertEquals("Jacob Parr", records[1].get("fields").get("Assignee").get("name"))

    def test_update(self):
        from datetime import datetime

        records = self.tables_api.query(filter_by_formula="id = 1")

        self.assertIsNotNone(records)
        self.assertEquals(1, len(records))
        self.assertEquals(1, records[0].get("fields").get("id"))

        record_id = records[0].get("id")
        when_a = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")+".000Z"

        self.tables_api.update_by_id(record_id, fields={
            "When": when_a
        })

        records = self.tables_api.query(filter_by_formula="id = 1")
        when_b = records[0].get("fields").get("When")
        self.assertEquals(when_a, when_b)

    def test_insert_delete(self):
        from datetime import datetime

        response = self.tables_api.insert({
            "Notes": "This is a test",
            "Assignee": None,
            "Status": "Whatever",
            "When": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")+".000Z",
            "AWS Workspace URL": "https://example.com",
        })
        record_id = response.get("id")
        self.tables_api.delete_by_id(record_id)


if __name__ == '__main__':
    unittest.main()
