from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.clients.rest.common import ApiContainer


class ReposClient(ApiContainer):
    def __init__(self, client: DBAcademyRestClient):
        self.client = client

    def list(self) -> dict:
        return self.client.api("GET", f"{self.client.endpoint}/api/2.0/repos")

    def update(self, repo_id, branch) -> dict:
        return self.client.api("PATCH", f"{self.client.endpoint}/api/2.0/repos/{repo_id}", branch=branch)

    def get(self, repo_id) -> dict:
        return self.client.api("GET", f"{self.client.endpoint}/api/2.0/repos/{repo_id}")

    def create(self, path: str, url: str, provider: str = "gitHub") -> dict:
        # /Repos/{folder}/{repo-name}
        return self.client.api("POST", f"{self.client.endpoint}/api/2.0/repos/", {
            "url": url,
            "provider": provider,
            "path": path
        })

    def delete(self, repo_id):
        return self.client.api("DELETE", f"{self.client.endpoint}/api/2.0/repos/{repo_id}", _expected=(200, 404))
