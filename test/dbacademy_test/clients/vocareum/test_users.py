import unittest
from dbacademy.clients import vocareum


class TestVocareumRestClient(unittest.TestCase):

    def test_list_users(self):
        client = vocareum.from_environ()
        self.assertIsNotNone(client)

        users = client.courses.id("87112").users.list()
        self.assertIsNotNone(users)

        user_ids = [u.get("id") for u in users]

        expected_count = 511
        self.assertEqual(expected_count, len(user_ids))
        self.assertEqual(expected_count, len(set(user_ids)))

    def test_get_user(self):
        client = vocareum.from_environ()
        self.assertIsNotNone(client)

        user = client.courses.id("87112").users.id("2615039").get()
        self.assertIsNotNone(user)
        self.assertEqual("2615039", user.get("id"))
        self.assertEqual("87112", user.get("courseid"))
        self.assertEqual("Aditya Baghel", user.get("name"))
        self.assertEqual("aditya.baghel@databricks.com", user.get("email"))


if __name__ == '__main__':
    unittest.main()
