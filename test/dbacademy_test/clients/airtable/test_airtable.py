import os
import unittest
from dbacademy.clients.airtable import AirTableClient

BASE_ID = "appijNwbRAAYFLcQr"   # Smoke-Tests
TABLE_ID = "tblmBKMz2uyFdM2Dj"  # Test-Table
ACCESS_TOKEN = os.environ.get("AIR-TABLE-PERSONAL-ACCESS-TOKEN")


class TestAirTAble(unittest.TestCase):

    def test_read(self):

        air_table = AirTableClient(access_token=ACCESS_TOKEN, base_id=BASE_ID, table_id=TABLE_ID)
        records = air_table.read()

        self.assertIsNotNone(records)
        self.assertEquals(3, len(records))

    def test_read_sorted(self):

        air_table = AirTableClient(access_token=ACCESS_TOKEN, base_id=BASE_ID, table_id=TABLE_ID)
        records = air_table.read(sort_by="id")

        self.assertIsNotNone(records)
        self.assertEquals(3, len(records))
        self.assertEquals(1, records[0].get("fields").get("id"))
        self.assertEquals(2, records[1].get("fields").get("id"))
        self.assertEquals(3, records[2].get("fields").get("id"))

    def test_read_filtered(self):

        air_table = AirTableClient(access_token=ACCESS_TOKEN, base_id=BASE_ID, table_id=TABLE_ID)
        records = air_table.read(filter_by_formula="id = 2")

        self.assertIsNotNone(records)
        self.assertEquals(1, len(records))
        self.assertEquals(2, records[0].get("fields").get("id"))

    def test_read_sorted_filtered(self):

        air_table = AirTableClient(access_token=ACCESS_TOKEN, base_id=BASE_ID, table_id=TABLE_ID)
        records = air_table.read(filter_by_formula="Assignee = 'Jacob Parr'", sort_by="id")

        self.assertIsNotNone(records)
        self.assertEquals(2, len(records))

        self.assertEquals(1, records[0].get("fields").get("id"))
        self.assertEquals("Jacob Parr", records[0].get("fields").get("Assignee").get("name"))

        self.assertEquals(1, records[0].get("fields").get("id"))
        self.assertEquals("Jacob Parr", records[1].get("fields").get("Assignee").get("name"))

    def test_update(self):
        from datetime import datetime

        air_table = AirTableClient(access_token=ACCESS_TOKEN, base_id=BASE_ID, table_id=TABLE_ID)
        records = air_table.read(filter_by_formula="id = 1")

        self.assertIsNotNone(records)
        self.assertEquals(1, len(records))
        self.assertEquals(1, records[0].get("fields").get("id"))

        record_id = records[0].get("id")
        when_a = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")+".000Z"

        air_table.update(record_id, fields={
            "When": when_a
        })

        records = air_table.read(filter_by_formula="id = 1")
        when_b = records[0].get("fields").get("When")
        self.assertEquals(when_a, when_b)

    def test_insert_delete(self):
        from datetime import datetime

        air_table = AirTableClient(access_token=ACCESS_TOKEN, base_id=BASE_ID, table_id=TABLE_ID)
        response = air_table.insert({
            "Notes": "This is a test",
            "Assignee": None,
            "Status": "Whatever",
            "When": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")+".000Z"
        })
        record_id = response.get("id")
        air_table.delete(record_id)


if __name__ == '__main__':
    unittest.main()
