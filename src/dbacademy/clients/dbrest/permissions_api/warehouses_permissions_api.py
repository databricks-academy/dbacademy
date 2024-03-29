__all__ = ["WarehousesPermissionsApi"]

from dbacademy.clients.rest.common import ApiClient
from dbacademy.clients.dbrest.permissions_api.permission_crud_api import PermissionsCrudApi


class WarehousesPermissionsApi(PermissionsCrudApi):
    valid_permissions = ["CAN_USE", "CAN_MANAGE"]

    def __init__(self, client: ApiClient):
        from dbacademy.common import validate

        self.__client = validate(client=client).required.as_type(ApiClient)
        super().__init__(client, "/api/2.0/permissions/sql/warehouses", "warehouses")
