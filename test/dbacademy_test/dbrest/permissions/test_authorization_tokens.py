import unittest


class AuthorizationTokenTests(unittest.TestCase):

    def setUp(self) -> None:
        import os
        from dbacademy.dbrest import DBAcademyRestClient
        from dbacademy.common.unit_tests import DBACADEMY_UNIT_TESTS_API_TOKEN, DBACADEMY_UNIT_TESTS_API_ENDPOINT

        token = os.getenv(DBACADEMY_UNIT_TESTS_API_TOKEN)
        endpoint = os.getenv(DBACADEMY_UNIT_TESTS_API_ENDPOINT)

        if token is None or endpoint is None:
            self.skipTest(
                f"Missing {DBACADEMY_UNIT_TESTS_API_TOKEN} or {DBACADEMY_UNIT_TESTS_API_ENDPOINT} environment variables")

        self.__client = DBAcademyRestClient(token=token, endpoint=endpoint)

        self.tearDown()

    @property
    def client(self):
        return self.__client

    def test_update_users_can_use(self):
        self.__test_update("group_name", "users", "CAN_USE", False)

    def test_update_users_can_manage(self):
        from dbacademy.clients.rest.common import DatabricksApiException

        try:
            self.__test_update("group_name", "users", "CAN_MANAGE", False)
            self.fail("Expected DatabricksApiException")

        except DatabricksApiException as e:
            self.assertEqual(400, e.http_code)
            self.assertEqual("Only admins can be given CAN_MANAGE permission on tokens", e.message)

    def __test_update(self, what: str, name: str, value: str, inherited: bool):
        response = self.client.permissions.authorizations.tokens.update(what, name, value)
        self.assertEqual("/authorization/tokens", response.get("object_id"))
        self.assertEqual("tokens", response.get("object_type"))

        access_control_list = response.get("access_control_list")

        found = False
        for item in access_control_list:
            this_what = item.get(what)
            if name == this_what:
                found = True
                permissions = item.get("all_permissions")[0]
                self.assertEqual(inherited, permissions.get("inherited"))
                self.assertEqual(value, permissions.get("permission_level"))

        self.assertTrue(found)

    def test_get_permission_levels(self):
        levels = self.client.permissions.authorizations.tokens.get_permission_levels()
        self.assertEqual(list, type(levels))
        self.assertEqual(2, len(levels))

        for i in range(0, 2):
            level = levels[0]
            permission_level = level.get("permission_level")
            if permission_level == "CAN_MANAGE":
                self.assertEqual("Can use and modify permissions on tokens", level.get("description"))
            elif permission_level == "CAN_USE":
                self.assertEqual("Can use tokens", level.get("description"))
            else:
                raise ValueError(f"""Expected "CAN_MANAGE" or "CAN_USE", found "{permission_level}".""")


if __name__ == '__main__':
    unittest.main()
