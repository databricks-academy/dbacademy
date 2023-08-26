from dbacademy.clients.rest.common import ApiClient

__all__ = ["Tokens"]

# noinspection PyProtectedMember
from dbacademy.dbrest.permissions.crud import PermissionsCrud


class Tokens(PermissionsCrud):

    valid_permissions = ("CAN_USE", "CAN_MANAGE")

    def __init__(self, client: ApiClient):
        super().__init__(client, "2.0/permissions/authorization/tokens", "cluster-policies")
