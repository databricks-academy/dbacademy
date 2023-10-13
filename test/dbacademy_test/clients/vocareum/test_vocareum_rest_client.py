import unittest


class TestVocareumRestClient(unittest.TestCase):

    def test_create_client(self):
        from dbacademy.clients.vocareum import VocareumRestClient

        client = VocareumRestClient.from_environ()
        self.assertIsNotNone(client)

        self.assertEquals("https://api.vocareum.com/api/v2/", client.url)


if __name__ == '__main__':
    unittest.main()
