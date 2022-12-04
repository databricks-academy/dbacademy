from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer


class MLflowModelVersionsClient(ApiContainer):

    def __init__(self, client: DBAcademyRestClient):
        self.client = client
        self.base_uri = f"{self.client.endpoint}/api/2.0/preview/mlflow/model-versions"

    def list(self, name: str):
        results = []
        max_results = 1000

        url = f"{self.base_uri}/search?max_results={max_results}&filter=name='{name}'"

        response = self.client.api("GET", url)
        results.extend(response.get("model_versions", []))

        while "next_page_token" in response:
            next_page_token = response["next_page_token"]
            response = self.client.api("GET", f"{url}&page_token={next_page_token}")
            results.extend(response.get("model_versions", []))

        return results

    def transition_stage(self, name: str, version: int, new_stage: str, *, archive_existing_versions=None):
        if archive_existing_versions is None:
            archive_existing_versions = new_stage in ("Production", "Staging")

        return self.client.api("POST", f"{self.base_uri}/transition-stage", {
            "name": name,
            "version": version,
            "stage": new_stage,
            "archive_existing_versions": archive_existing_versions,
        })
