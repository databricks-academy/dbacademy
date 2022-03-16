from dbacademy.dbrest import DBAcademyRestClient

class SqlQueriesClient:

    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client
        self.token = token
        self.endpoint = endpoint
        self.base_uri = f"{self.endpoint}/api/2.0/preview/sql/queries"

    def list(self, endpoint_id):
        return self.client.execute_get_json(f"{self.base_uri}")
