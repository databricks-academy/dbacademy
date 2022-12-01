from typing import Dict, Any, List, Optional
from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer


class MLflowModelsClient(ApiContainer):

    def __init__(self, client: DBAcademyRestClient):
        self.client = client
        self.base_uri = f"{self.client.endpoint}/api/2.0/preview/mlflow/registered-models"

    def list(self):
        results = []
        max_results = 1000

        url = f"{self.base_uri}/search?max_results={max_results}"

        response = self.client.api("GET", url)
        results.extend(response.get("registered_models", []))

        while "next_page_token" in response:
            next_page_token = response["next_page_token"]
            response = self.client.api("GET", f"{url}&page_token={next_page_token}")
            results.extend(response.get("registered_models", []))

        return results

    def delete(self, name: str):
        url = f"{self.base_uri}/delete"
        payload = {
            "name": name
        }
        self.client.api("DELETE", url, payload)
