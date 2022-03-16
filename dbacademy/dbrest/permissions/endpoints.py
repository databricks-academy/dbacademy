from dbacademy.dbrest import DBAcademyRestClient

class SqlEndpointsClient:

    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client      # Client API exposing other operations to this class
        self.token = token        # The authentication token
        self.endpoint = endpoint  # The API endpoint
        self.base_uri = f"{self.endpoint}/api/2.0/preview/permissions/sql/endpoints"

    def get_levels(self, id):
        return self.client.execute_get_json(f"{self.base_uri}/{id}/permissionLevels")

    def get(self, id):
        return self.client.execute_get_json(f"{self.base_uri}/{id}")
