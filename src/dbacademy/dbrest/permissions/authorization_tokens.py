from typing import Dict, Any, Literal

from dbacademy.dbrest.client import DBAcademyRestClient

from dbacademy.clients.rest.common import ApiContainer, ItemId, ApiClient

__all__ = ["Tokens"]

# noinspection PyProtectedMember
from dbacademy.dbrest.permissions.crud import PermissionLevelList, PermissionsCrud


class Tokens(PermissionsCrud):

    valid_permissions = ("CAN_USE", "CAN_MANAGE")

    def __init__(self, client: ApiClient):
        super().__init__(client, "2.0/permissions/authorization/tokens", "cluster-policies")
