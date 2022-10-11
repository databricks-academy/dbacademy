from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer


class FeatureStoreClient(ApiContainer):
    def __init__(self, client: DBAcademyRestClient):
        self.client = client
        self.base_uri = f"{self.client.endpoint}/api/2.0/feature-store/"

    def search_tables(self, max_results:int = 10) -> dict:
        results = self.client.execute_get_json(f"{self.base_uri}/feature-tables/search?max_results={max_results}")
        return results.get("feature_tables", [])
