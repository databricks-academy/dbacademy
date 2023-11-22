import unittest
from dbacademy.clients import docebo


class TestDoceboRestClient(unittest.TestCase):

    def test_create(self):
        client = docebo.from_environment()
        self.assertIsNotNone(client)
        self.assertEqual("https://databricks.docebosaas.com", client.endpoint)


if __name__ == '__main__':
    unittest.main()
