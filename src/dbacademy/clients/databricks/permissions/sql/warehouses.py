__all__ = ["SqlWarehouses"]

from dbacademy.clients.rest.common import ApiClient
from dbacademy.clients.databricks.permissions.crud import PermissionsCrud


class SqlWarehouses(PermissionsCrud):
    valid_permissions = ["CAN_USE", "CAN_MANAGE"]

    def __init__(self, client: ApiClient):
        super().__init__(client, "/api/2.0/permissions/sql/endpoints", "endpoints")
