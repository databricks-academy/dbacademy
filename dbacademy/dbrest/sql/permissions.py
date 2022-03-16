from dbacademy.dbrest import DBAcademyRestClient
import builtins

class SqlPermissionsClient:

    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client
        self.token = token
        self.endpoint = endpoint
        self.base_uri = f"{self.endpoint}/api/2.0/preview/sql/permissions"

        self.max_page_size = 250

    def get(self, object_type, object_id):
        return self.client.execute_get_json(f"{self.base_uri}/{object_type}/{object_id}")
