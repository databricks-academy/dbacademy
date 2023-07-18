from typing import List
from dbacademy.rest.common import ApiClient

__all__ = ["GitHubClient"]


class GitHubClient(ApiClient):

    def __init__(self, org_name: str):

        super().__init__(url="https://api.github.com", token=None)

        self.__org_name = org_name

        # token: str = None,
        # endpoint: str = None,
        # throttle_seconds: int = 0,
        # *,
        # user: str = None,
        # password: str = None,
        # authorization_header: str = None,
        # client: ApiClient = None,
        # verbose: bool = False):

    @property
    def org_name(self) -> str:
        return self.__org_name

    def repo(self, name: str) -> "Repo":
        return Repo(self, name)


class Repo:
    def __init__(self, client: GitHubClient, repo_name: str):
        self.__client = client
        self.__repo_name = repo_name

    @property
    def client(self) -> GitHubClient:
        return self.__client

    @property
    def repo_name(self) -> str:
        return self.__repo_name

    @property
    def org_name(self) -> str:
        return self.client.org_name

    @property
    def commits(self) -> "Commits":
        return Commits(self)

    @staticmethod
    def sort_semantic_versions(versions: List[str]) -> List[str]:
        versions.sort(key=lambda v: (int(v.split(".")[0]) * 10000) + (int(v.split(".")[1]) * 100) + int(v.split(".")[2]))
        return versions

    def list_all_tags(self) -> List[str]:
        import requests

        path = f"/repos/{self.org_name}/{self.repo_name}/tags"
        # , headers = {"User-Agent": "Databricks Academy"}
        response = self.client.api("GET", _endpoint_path=path, _expected=[200, 403], _result_type=requests.Response)

        if response is None or response.status_code == 403:
            return ["v0.0.0"]
        else:
            assert response.status_code == 200, f"Expected HTTP 200, found {response.status_code}:\n{response.text}"

        versions = [t.get("name")[1:] for t in response.json()]
        return self.sort_semantic_versions(versions)


class Commits:
    def __init__(self, repo: Repo):
        self.__repo = repo

    @property
    def client(self) -> GitHubClient:
        return self.repo.client

    @property
    def repo(self):
        return self.__repo

    def get_latest_commit_id(self, branch_name: str):
        path = f"/repos/{self.repo.org_name}/{self.repo.repo_name}/commits/{branch_name}"
        return self.client.api("GET", _endpoint_path=path).get("sha")
