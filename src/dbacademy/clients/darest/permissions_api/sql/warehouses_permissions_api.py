__all__ = ["SqlWarehousesPermissionsApi"]

from dbacademy.clients.rest.common import ApiClient
from dbacademy.clients.darest.permissions_api.permission_crud_api import PermissionsCrudApi


class SqlWarehousesPermissionsApi(PermissionsCrudApi):

    valid_permissions = ["CAN_USE", "CAN_MANAGE"]

    def __init__(self, client: ApiClient):
        from dbacademy.common import validate

        self.__client = validate(client=client).required.as_type(ApiClient)
        super().__init__(client, "/api/2.0/permissions/sql/endpoints", "endpoints")
