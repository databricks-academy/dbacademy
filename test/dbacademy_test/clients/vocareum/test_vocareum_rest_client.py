import unittest
from dbacademy.clients import vocareum


class TestVocareumRestClient(unittest.TestCase):

    def test_create_client(self):
        client = vocareum.from_environ()
        self.assertIsNotNone(client)

        self.assertEquals("https://api.vocareum.com/api/v2", client.endpoint)


if __name__ == '__main__':
    unittest.main()
