__all__ = ["ManageAPI", "UsersAPI"]

from typing import Dict, Any, List
from dbacademy.clients.rest.common import ApiClient, ApiContainer


class ManageAPI(ApiContainer):

    def __init__(self, client: ApiClient):
        self.client = client

        self.users = UsersAPI(self.client)


class UsersAPI(ApiContainer):
    def __init__(self, client: ApiClient):
        self.client = client
        url = self.client.endpoint.rstrip("/")
        self.base_uri = f"{url}/manage/v1/user"

    def login(self, username: str, password: str, client_timezone_textual: str = None, client_timezone: int = None, issue_refresh_token: bool = None):
        params = {
            "username": username,
            "password": password,
        }
        if client_timezone_textual:
            params["client_timezone_textual"] = client_timezone_textual
        if client_timezone:
            params["client_timezone"] = client_timezone
        if issue_refresh_token:
            params["issue_refresh_token"] = issue_refresh_token

        response = self.client.api("POST", f"{self.base_uri}/login", _data=params)
        return response.get("data")

    def find_user(self, username: str) -> List[Dict[str, Any]]:
        from urllib import parse

        params = {
            "search_text": parse.quote_plus(username)
        }

        # TODO - make sure that this dataset is not paged
        response = self.client.api("GET", self.base_uri, _data=params)
        data = response.get("data", dict)
        items = data.get("items", list())
        return items
