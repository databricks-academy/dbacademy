from dbacademy.rest.common import ApiClient
from dbacademy.rest.permissions.crud import PermissionsCrud

__all__ = ["SqlCrud"]


class SqlCrud(PermissionsCrud):
    valid_objects = ["alerts", "dashboards", "data_sources", "queries"]
    valid_permissions = [None, "CAN_VIEW", "CAN_RUN", "CAN_MANAGE"]

    def __init__(self,
                 client: ApiClient,
                 singular: str,
                 plural: str = None):
        if plural is None:
            plural = singular + "s"
        if plural not in self.valid_objects:
            raise ValueError(f"Expected 'plural' to be one of {self.valid_objects}, found '{plural}'")
        super().__init__(client, f"2.0/sql/permissions/{plural}", noun=singular, singular=singular, plural=plural)
