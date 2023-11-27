__all__ = ["SqlCrudPermissions"]

from dbacademy.clients.rest.common import ApiClient
from dbacademy.clients.dbrest.permissions_api.permission_crud_api import PermissionsCrudApi


class SqlCrudPermissions(PermissionsCrudApi):
    valid_objects = ["alerts", "dashboards", "data_sources", "queries"]
    valid_permissions = [None, "CAN_VIEW", "CAN_RUN", "CAN_MANAGE"]

    def __init__(self,
                 client: ApiClient,
                 singular: str,
                 plural: str = None):

        from dbacademy.common import validate

        self.__client = validate(client=client).required.as_type(ApiClient)
        singular = validate(singular=singular).required.str()
        plural = validate(plural=plural).optional.str()

        if plural is None:
            plural = singular + "s"

        validate(plural=plural).optional.as_one_of(str, SqlCrudPermissions.valid_objects)

        super().__init__(client, f"/api/2.0/sql/permissions/{plural}", noun=singular, singular=singular, plural=plural)
