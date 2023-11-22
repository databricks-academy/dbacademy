__all__ = ["Pools"]

from dbacademy.clients.rest.common import ApiClient
from dbacademy.clients.darest.permissions.crud import PermissionsCrud


class Pools(PermissionsCrud):
    valid_permissions = ["CAN_MANAGE", "CAN_ATTACH_TO"]

    def __init__(self, client: ApiClient):
        super().__init__(client, "/api/2.0/permissions/instance-pools", "instance_pool")
