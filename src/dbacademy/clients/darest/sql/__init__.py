__all__ = ["SqlClient"]

from dbacademy.clients.rest.common import ApiClient, ApiContainer
from dbacademy.clients.darest.sql.config import SqlConfigClient
from dbacademy.clients.darest.sql.endpoints import SqlWarehousesClient
from dbacademy.clients.darest.sql.queries import SqlQueriesClient
from dbacademy.clients.darest.sql.statements import StatementsClient
from dbacademy.clients.darest.permissions.sql import Sql
from dbacademy.clients.darest import DBAcademyRestClient


class SqlClient(ApiContainer):
    def __init__(self, client: ApiClient):
        from dbacademy.common import validate
        self.__client: DBAcademyRestClient = validate(client=client).required.as_type(DBAcademyRestClient)

    @property
    def client(self) -> DBAcademyRestClient:
        return self.__client

    @property
    def config(self) -> SqlConfigClient:
        return SqlConfigClient(self.client)

    @property
    def warehouses(self) -> SqlWarehousesClient:
        return SqlWarehousesClient(self.client)

    @property
    def queries(self) -> SqlQueriesClient:
        return SqlQueriesClient(self.client)

    @property
    def statements(self) -> StatementsClient:
        return StatementsClient(self.client)

    @property
    def permissions(self) -> Sql:
        return self.client.permissions.sql
