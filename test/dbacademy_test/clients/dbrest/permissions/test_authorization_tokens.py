__all__ = ["AuthorizationTokenTests"]

import unittest
from typing import Dict, Any
from dbacademy.clients import dbrest
from dbacademy_test.clients.dbrest import DBACADEMY_UNIT_TESTS


class AuthorizationTokenTests(unittest.TestCase):

    def setUp(self) -> None:
        self.__client = dbrest.from_token(scope=DBACADEMY_UNIT_TESTS)
        self.tearDown()

    @property
    def client(self):
        return self.__client

    def test_update_users_can_use(self):
        group_name = "users"
        permission_level = "CAN_USE"
        response = self.client.permissions.authorizations.tokens.update_group(group_name=group_name,
                                                                              permission_level=permission_level)
        self.__validate_update(response, "group_name", group_name, permission_level, False)

    def test_update_users_can_manage(self):
        from dbacademy.clients.rest.common import DatabricksApiException

        try:
            group_name = "users"
            permission_level = "CAN_MANAGE"
            self.client.permissions.authorizations.tokens.update_group(group_name=group_name,
                                                                       permission_level=permission_level)
            self.fail("Expected DatabricksApiException")

        except DatabricksApiException as e:
            self.assertEqual(400, e.http_code)
            self.assertEqual("Only admins can be given CAN_MANAGE permission on tokens", e.message)

    def __validate_update(self, response: Dict[str, Any], what: str, name: str, value: str, inherited: bool):
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

    def test_get_levels(self):
        levels = self.client.permissions.authorizations.tokens.get_levels()
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
