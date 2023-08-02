from typing import List, Dict, Any
from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.clients.rest.common import ApiContainer


class FeatureStoreClient(ApiContainer):
    def __init__(self, client: DBAcademyRestClient):
        self.client = client
        self.base_uri = f"{self.client.endpoint}/api/2.0/feature-store"

    def search_tables(self) -> List[Dict[str, Any]]:
        import sys
        results = []

        response = self.client.api("GET", f"{self.base_uri}/feature-tables/search?max_results={int(1e9)}")
        results.extend(response.get("feature_tables", []))

        while "next_page_token" in response:
            next_page_token = response["next_page_token"]
            response = self.client.api("GET", f"{self.base_uri}/feature-tables/search?max_results={sys.maxsize}&page_token={next_page_token}")
            results.extend(response.get("feature_tables", []))

        return results
