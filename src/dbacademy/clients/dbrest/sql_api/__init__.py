__all__ = ["SqlApi"]

from dbacademy.clients.rest.common import ApiContainer, ApiClient
from dbacademy.clients.dbrest.sql_api.config_api import SqlConfigApi
from dbacademy.clients.dbrest.sql_api.warehouses_api import SqlWarehousesApi
from dbacademy.clients.dbrest.sql_api.queries_api import SqlQueriesApi
from dbacademy.clients.dbrest.sql_api.statements_api import StatementsApi


class SqlApi(ApiContainer):

    def __init__(self, client: ApiClient):
        from dbacademy.common import validate

        self.__client = validate(client=client).required.as_type(ApiClient)

    @property
    def config(self) -> SqlConfigApi:
        return SqlConfigApi(self.__client)

    @property
    def warehouses(self) -> SqlWarehousesApi:
        return SqlWarehousesApi(self.__client)

    @property
    def queries(self) -> SqlQueriesApi:
        return SqlQueriesApi(self.__client)

    @property
    def statements(self) -> StatementsApi:
        return StatementsApi(self.__client)

    # @property
    # def permissions(self) -> SqlPermissionsApi:
    #     return SqlPermissionsApi(self.__client)
