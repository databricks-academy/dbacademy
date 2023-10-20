__all__ = ["SqlClient"]

from dbacademy.clients.rest.common import ApiClient, ApiContainer


class SqlClient(ApiContainer):
    def __init__(self, client: ApiClient):
        from dbacademy.common import validate
        from dbacademy.clients.databricks import DBAcademyRestClient

        self.client: DBAcademyRestClient = validate.any_value(DBAcademyRestClient, client=client, required=True)

        from dbacademy.clients.databricks.sql.config import SqlConfigClient
        self.config = SqlConfigClient(self.client)

        from dbacademy.clients.databricks.sql.endpoints import SqlWarehousesClient
        self.warehouses = SqlWarehousesClient(self.client)
        self.endpoints = SqlWarehousesClient(self.client)  # Backwards Compatibility

        from dbacademy.clients.databricks.sql.queries import SqlQueriesClient
        self.queries = SqlQueriesClient(self.client)

        from dbacademy.clients.databricks.sql.statements import StatementsClient
        self.statements = StatementsClient(self.client)

        self.permissions = self.client.permissions.sql
