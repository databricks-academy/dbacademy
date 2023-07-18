from dbacademy.clients.github.github_client_class import GitHubClient


def default_client() -> GitHubClient:
    return GitHubClient("databricks-academy")
