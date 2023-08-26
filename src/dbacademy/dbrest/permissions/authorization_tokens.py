from typing import Dict, Any, Literal

from dbacademy.dbrest.client import DBAcademyRestClient

from dbacademy.clients.rest.common import ApiContainer

__all__ = ["Tokens"]


class Tokens(ApiContainer):

    # noinspection PyProtectedMember
    from dbacademy.dbrest.permissions.crud import What

    valid_permissions = ("CAN_USE", "CAN_MANAGE")
    # noinspection PyTypeHints
    PermissionLevel = Literal[valid_permissions]

    def __init__(self, client: DBAcademyRestClient):
        self.client = client
        self.base_url = f"{self.client.endpoint}/api/2.0/permissions/authorization/tokens"

    def get_permission_levels(self) -> Dict[str, Any]:
        response = self.client.api("GET", f"2.0/permissions/authorization/tokens/permissionLevels")
        return response.get("permission_levels")

    def update(self, what: What, value: str, permission_level: PermissionLevel):
        from dbacademy.dbrest.permissions.crud import PermissionsCrud

        # noinspection PyProtectedMember
        PermissionsCrud._validate_what(what)

        self._validate_permission_level(permission_level)
        acl = [
                {
                    what: value,
                    "permission_level": permission_level
                }
            ]
        return self.client.api("PATCH", self.base_url, access_control_list=acl)

    def _validate_permission_level(self, permission_level: PermissionLevel):
        if permission_level not in self.valid_permissions:
            raise ValueError(f"Expected 'permission_level' to be one of {self.valid_permissions},"
                             f" found '{permission_level}'")
