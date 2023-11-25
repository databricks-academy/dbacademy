__all__ = ["SqlPermissionsApi"]

from dbacademy.clients.rest.common import ApiContainer, ApiClient
from dbacademy.clients.dbrest.permissions_api.sql.warehouses_permissions_api import SqlWarehousesPermissionsApi
from dbacademy.clients.dbrest.permissions_api.sql.sql_crud_permissions_api import SqlCrudPermissions


class SqlPermissionsApi(ApiContainer):

    def __init__(self, client: ApiClient):
        from dbacademy.common import validate

        self.__client = validate(client=client).required.as_type(ApiClient)

    @property
    def warehouses(self) -> SqlWarehousesPermissionsApi:
        return SqlWarehousesPermissionsApi(self.__client)

    @property
    def queries(self) -> SqlCrudPermissions:
        return SqlCrudPermissions(self.__client, "query", "queries")

    @property
    def dashboards(self) -> SqlCrudPermissions:
        return SqlCrudPermissions(self.__client, "dashboard")

    @property
    def data_sources(self) -> SqlCrudPermissions:
        return SqlCrudPermissions(self.__client, "data_source")

    @property
    def alerts(self) -> SqlCrudPermissions:
        return SqlCrudPermissions(self.__client, "alert")
