__all__ = ["PoolsPermissionsApi"]

from dbacademy.clients.rest.common import ApiClient
from dbacademy.clients.darest.permissions_api.permission_crud_api import PermissionsCrudApi


class PoolsPermissionsApi(PermissionsCrudApi):
    valid_permissions = ["CAN_MANAGE", "CAN_ATTACH_TO"]

    def __init__(self, client: ApiClient):
        from dbacademy.common import validate

        self.__client = validate(client=client).required.as_type(ApiClient)
        super().__init__(client, "/api/2.0/permissions/instance-pools", "instance_pool")
