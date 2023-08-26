from typing import Dict, Any, Literal

from dbacademy.dbrest.client import DBAcademyRestClient

from dbacademy.clients.rest.common import ApiContainer, ItemId, ApiClient

__all__ = ["Tokens"]

# noinspection PyProtectedMember
from dbacademy.dbrest.permissions.crud import PermissionLevelList, PermissionsCrud


class Tokens(PermissionsCrud):

    valid_permissions = ("CAN_USE", "CAN_MANAGE")
    # noinspection PyTypeHints
    PermissionLevel = Literal[valid_permissions]

    def __init__(self, client: ApiClient):
        super().__init__(client, "2.0/permissions/authorization/tokens", "cluster-policies")

    def get_levels(self, id_value: ItemId = "authorization/tokens") -> PermissionLevelList:
        return self.client.api("GET", f"2.0/permissions/{id_value}/permissionLevels").get("permission_levels")
