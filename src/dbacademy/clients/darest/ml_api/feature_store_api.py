__all__ = ["FeatureStoreApi"]

from typing import List, Dict, Any
from dbacademy.clients.rest.common import ApiClient, ApiContainer


class FeatureStoreApi(ApiContainer):

    def __init__(self, client: ApiClient):
        from dbacademy.common import validate

        self.__client = validate(client=client).required.as_type(ApiClient)
        self.base_uri = f"{self.__client.endpoint}/api/2.0/feature-store"

    def search_tables(self) -> List[Dict[str, Any]]:
        maxsize = 200
        results = []

        response = self.__client.api("GET", f"{self.base_uri}/feature-tables/search?max_results={maxsize}")
        results.extend(response.get("feature_tables", []))

        while "next_page_token" in response:
            next_page_token = response["next_page_token"]
            response = self.__client.api("GET", f"{self.base_uri}/feature-tables/search?max_results={maxsize}&page_token={next_page_token}")
            results.extend(response.get("feature_tables", []))

        return results
