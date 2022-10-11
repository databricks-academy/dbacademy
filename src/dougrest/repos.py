from dbacademy.rest.common import DatabricksApiException, ApiContainer


class Repos(ApiContainer):
    def __init__(self, databricks):
        self.databricks = databricks

    def list(self):
        repos = self.databricks.api("GET", "2.0/repos").get("repos", [])
        return repos

    def list_by_path(self):
        repos = self.list()
        results = {r['path']: r for r in repos}
        return results

    def exists(self, workspace_path):
        return workspace_path in self.list_by_path()

    def create(self, url, path=None, provider="gitHub", *, if_exists="error"):
        data = {
            "url": url,
            "path": path,
            "provider": provider
        }
        #        for k,v in data.items():
        #          if v is None:
        #            del data[k]
        self.databricks.api("POST", "2.0/repos", data)

    def delete(self, repo=None, id=None, workspace_path=None):
        if repo:
            id = repo["id"]
        elif id:
            pass
        elif workspace_path:
            repo = self.list_by_path().get(workspace_path)
            if not repo:
                raise DatabricksApiException("Repo not found at: " + workspace_path)
            id = repo["id"]
        self.databricks.api("DELETE", f"2.0/repos/{id}")
        return id
