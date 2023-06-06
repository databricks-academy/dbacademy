from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer


class ServingEndpointsClient(ApiContainer):
    def __init__(self, client: DBAcademyRestClient):
        self.client = client

    def list_endpoints(self) -> dict:
        response = self.client.api("GET", f"{self.client.endpoint}/api/2.0/serving-endpoints")
        return response.get("endpoints", list())
