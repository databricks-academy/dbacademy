from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer


class SqlClient(ApiContainer):
    def __init__(self, client: DBAcademyRestClient):
        self.client = client      # Client API exposing other operations to this class

        from dbacademy.dbrest.sql.config import SqlConfigClient
        self.config = SqlConfigClient(self.client)

        from dbacademy.dbrest.sql.endpoints import SqlEndpointsClient
        self.endpoints = SqlEndpointsClient(self.client)

        from dbacademy.dbrest.sql.queries import SqlQueriesClient
        self.queries = SqlQueriesClient(self.client)

        self.permissions = client.permissions.sql
