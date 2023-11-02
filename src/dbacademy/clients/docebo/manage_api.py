__all__ = ["ManageAPI", "UsersAPI"]

from typing import Dict, Any, List
from dbacademy.clients.rest.common import ApiClient, ApiContainer


class UsersAPI(ApiContainer):
    def __init__(self, client: ApiClient):
        self.client = client
        url = self.client.endpoint.rstrip("/")
        self.base_uri = f"{url}/manage/v1/user"

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


class ManageAPI(ApiContainer):

    def __init__(self, client: ApiClient):
        self.__client = client

    @property
    def client(self) -> ApiClient:
        return self.__client

    @property
    def users(self) -> UsersAPI:
        return UsersAPI(self.client)

    def user_login(self, username: str, password: str, client_timezone_textual: str = None, client_timezone: int = None, issue_refresh_token: bool = None) -> Dict[str, Any]:
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

        url = self.client.endpoint.rstrip("/")

        response = self.client.api("POST", f"{url}/manage/v1/user/login", _data=params)
        return response.get("data")
