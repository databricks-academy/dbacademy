from dbacademy.rest.common import ApiClient
from dbacademy.rest.permissions.crud import PermissionsCrud

__all__ = ["SqlWarehouses"]


class SqlWarehouses(PermissionsCrud):
    valid_permissions = ["CAN_USE", "CAN_MANAGE"]

    def __init__(self, client: ApiClient):
        super().__init__(client, "2.0/permissions/sql/endpoints", "endpoints")
