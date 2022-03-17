from dbacademy.dbrest import DBAcademyRestClient

class PermissionsClient():
    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client      # Client API exposing other operations to this class
        self.token = token        # The authentication token
        self.endpoint = endpoint  # The API endpoint

    def jobs(self):
        from dbacademy.dbrest.permissions.jobs import JobsPermissionsClient
        return JobsPermissionsClient(self.client, self.token, self.endpoint)

    def sql(self):
        pass

    def sql_endpoints(self):
        from dbacademy.dbrest.permissions.sql_endpoints import SqlEndpointsClient
        return SqlEndpointsClient(self.client, self.token, self.endpoint)

    def sql_queries(self):
        from dbacademy.dbrest.permissions.sql_permissions_client import SqlPermissionsClient
        return SqlPermissionsClient(self.client, self.token, self.endpoint, "query", "queries")

    def sql_dashboards(self):
        from dbacademy.dbrest.permissions.sql_permissions_client import SqlPermissionsClient
        return SqlPermissionsClient(self.client, self.token, self.endpoint, "dashboard", "dashboards")
