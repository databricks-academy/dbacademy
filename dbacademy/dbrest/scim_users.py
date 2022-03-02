from dbacademy.dbrest import DBAcademyRestClient


class ScimUsersClient:

    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client      # Client API exposing other operations to this class
        self.token = token        # The authentication token
        self.endpoint = endpoint  # The API endpoint

    def get_users(self):
        return self.client.execute_get_json(f"{self.endpoint}/api/2.0/preview/scim/v2/Users")

    def get_user_by_id(self, user_id):
        return self.client.execute_get_json(f"{self.endpoint}/api/2.0/preview/scim/v2/Users{user_id}")
