from typing import Dict, Any
import unittest


class TestDoceboRestClient(unittest.TestCase):

    def test_create(self):
        from dbacademy.docebo import DoceboRestClient

        client = DoceboRestClient.from_environ()
        self.assertIsNotNone(client)

        self.assertEquals("https://databricks.docebosaas.com/", client.url)


if __name__ == '__main__':
    unittest.main()
