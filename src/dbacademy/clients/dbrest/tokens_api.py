__all__ = ["TokensApi"]

from dbacademy.clients.rest.common import ApiClient, ApiContainer


class TokensApi(ApiContainer):
    def __init__(self, client: ApiClient):
        from dbacademy.common import validate

        self.__client = validate(client=client).required.as_type(ApiClient)
        self.base_url = f"{self.__client.endpoint}/api/2.0/token"

    def list(self):
        results = self.__client.api("GET", f"{self.base_url}/list")
        return results["token_infos"]

    def create(self, comment: str, lifetime_seconds: int):
        params = {
            "comment": comment,
            "lifetime_seconds": lifetime_seconds
        }
        return self.__client.api("POST", f"{self.base_url}/create", params)

    def revoke(self, token_id):
        params = {
            "token_id": token_id
        }
        return self.__client.api("POST", f"{self.base_url}/delete", params)
