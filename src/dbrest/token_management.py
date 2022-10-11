from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer


class TokenManagementClient(ApiContainer):
    def __init__(self, client: DBAcademyRestClient):
        self.client = client
        self.base_url = f"{self.client.endpoint}/api/2.0/token-management"

    def create_on_behalf_of_service_principal(self, application_id: str, comment: str, lifetime_seconds: int):
        params = {
            "application_id": application_id,
            "comment": comment,
            "lifetime_seconds": lifetime_seconds
        }
        return self.client.execute_post_json(f"{self.base_url}/on-behalf-of/tokens", params=params)

    def list(self):
        results = self.client.execute_get_json(url=f"{self.base_url}/tokens")
        return results.get("token_infos", [])

    def delete_by_id(self, token_id):
        return self.client.execute_delete_json(url=f"{self.base_url}/tokens/{token_id}")

    def get_by_id(self, token_id):
        return self.client.execute_get_json(url=f"{self.base_url}/tokens/{token_id}")

