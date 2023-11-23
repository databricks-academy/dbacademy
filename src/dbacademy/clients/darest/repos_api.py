__all__ = ["ReposApi"]

from dbacademy.clients.rest.common import ApiClient, ApiContainer


class ReposApi(ApiContainer):
    def __init__(self, client: ApiClient):
        from dbacademy.common import validate

        self.__client = validate(client=client).required.as_type(ApiClient)

    def list(self) -> dict:
        return self.__client.api("GET", f"{self.__client.endpoint}/api/2.0/repos")

    def update(self, repo_id, branch) -> dict:
        return self.__client.api("PATCH", f"{self.__client.endpoint}/api/2.0/repos/{repo_id}", branch=branch)

    def get(self, repo_id) -> dict:
        return self.__client.api("GET", f"{self.__client.endpoint}/api/2.0/repos/{repo_id}")

    def create(self, path: str, url: str, provider: str = "gitHub") -> dict:
        # /Repos/{folder}/{repo-name}
        return self.__client.api("POST", f"{self.__client.endpoint}/api/2.0/repos/", {
            "url": url,
            "provider": provider,
            "path": path
        })

    def delete(self, repo_id):
        return self.__client.api("DELETE", f"{self.__client.endpoint}/api/2.0/repos/{repo_id}", _expected=(200, 404))
