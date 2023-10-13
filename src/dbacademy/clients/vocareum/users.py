__all__ = ["UsersClient"]

from typing import Dict, Any, List
from dbacademy.clients.rest.common import ApiContainer


class UserClient(ApiContainer):
    from dbacademy.clients.vocareum import VocareumRestClient

    def __init__(self, client: VocareumRestClient, course_id: str, user_id: str):
        self.client = client
        self.__course_id = course_id
        self.__user_id = user_id
        self.user_url = f"{self.client.url[:-1]}/courses/{self.course_id}/users/{self.user_id}"

    @property
    def course_id(self):
        return self.__course_id

    @property
    def user_id(self):
        return self.__user_id

    def get(self) -> Dict[str, Any]:
        response = self.client.api("GET", self.user_url)
        return response.get("users")[0]


class UsersClient(ApiContainer):
    from dbacademy.clients.vocareum import VocareumRestClient

    def __init__(self, client: VocareumRestClient, course_id: str):
        self.client = client
        self.__course_id = course_id
        self.users_url = f"{self.client.url[:-1]}/courses/{self.course_id}/users"

    @property
    def course_id(self):
        return self.__course_id

    def list(self) -> List[Dict[str, Any]]:
        import sys

        users = list()

        for page in range(0, sys.maxsize):
            response = self.client.api("GET", f"{self.users_url}?page={page}")
            records = response.get("users", list())

            if len(records) == 0:
                break
            else:
                users.extend(records)

        return users

    def id(self, user_id: str) -> UserClient:
        return UserClient(self.client, self.course_id, user_id)
