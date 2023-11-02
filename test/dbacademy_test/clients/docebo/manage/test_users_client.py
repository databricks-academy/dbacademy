import unittest


class TestUsers(unittest.TestCase):

    def test_login_failure(self):
        from dbacademy.clients.rest.common import DatabricksApiException
        from dbacademy.clients import docebo

        client = docebo.from_environment()

        try:
            client.manage.user_login(username="mickey.mouse@disney.com",
                                     password="asdf")
        except DatabricksApiException as e:
            self.assertEquals(400, e.http_code)
            self.assertEquals("Wrong credentials provided", e.message[0])

    def test_login_success(self):
        import os
        from dbacademy.clients import docebo

        client = docebo.from_environment()
        username = os.environ.get("DOCEBO_UNIT_TEST_USERNAME")
        password = os.environ.get("DOCEBO_UNIT_TEST_PASSWORD")

        if username is None or password is None:
            self.skipTest("DOCEBO_UNIT_TEST_USERNAME or DOCEBO_UNIT_TEST_PASSWORD were not found")

        response = client.manage.user_login(username=username, password=password)

        self.assertIsNotNone(response)
        self.assertEquals("7200", response.get("expires_in"))
        self.assertEquals("bearer", response.get("token_type"))

    def test_find_users(self):
        from dbacademy.clients import docebo

        client = docebo.from_environment()
        users = client.manage.users.find_user("jacob.parr@databricks.com")
        self.assertIsNotNone(users)
        self.assertTrue(len(users) > 0)

        user_0 = users[0]
        self.assertEquals("jacob.parr@databricks.com", user_0.get("username"))
        self.assertEquals("Jacob", user_0.get("first_name"))
        self.assertEquals("Parr", user_0.get("last_name"))
        self.assertEquals("13903", user_0.get("user_id"))


if __name__ == '__main__':
    unittest.main()
