from dbacademy.dbrest import DBAcademyRestClient

class SqlConfigClient:

    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client
        self.token = token
        self.endpoint = endpoint

    def get(self, endpoint_id):
        return self.client.execute_get_json(f"{self.endpoint}/api/2.0/sql/config/endpoints")
