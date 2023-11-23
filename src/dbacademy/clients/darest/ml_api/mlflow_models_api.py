__all__ = ["MLflowModelsApi"]

from dbacademy.clients.rest.common import ApiClient, ApiContainer


class MLflowModelsApi(ApiContainer):

    def __init__(self, client: ApiClient):
        from dbacademy.common import validate

        self.__client = validate(client=client).required.as_type(ApiClient)
        self.base_uri = f"{self.__client.endpoint}/api/2.0/preview/mlflow/registered-models"

    def list(self):
        results = []
        max_results = 1000

        url = f"{self.base_uri}/search?max_results={max_results}"

        response = self.__client.api("GET", url)
        results.extend(response.get("registered_models", []))

        while "next_page_token" in response:
            next_page_token = response["next_page_token"]
            response = self.__client.api("GET", f"{url}&page_token={next_page_token}")
            results.extend(response.get("registered_models", []))

        return results

    def delete(self, name: str):
        url = f"{self.base_uri}/delete"
        payload = {
            "name": name
        }
        self.__client.api("DELETE", url, payload)
