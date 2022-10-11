from dbacademy.rest.common import ApiClient
from dbacademy.rest.permissions.crud import PermissionsCrud

__all__ = ["Directories"]


class Directories(PermissionsCrud):
    valid_permissions = [None, "CAN_READ", "CAN_RUN", "CAN_EDIT", "CAN_MANAGE"]

    def __init__(self, client: ApiClient):
        super().__init__(client, "2.0/permissions/directories", "directory")
