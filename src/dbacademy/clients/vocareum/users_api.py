__all__ = ["UsersApi"]

from typing import Dict, Any, List

from dbacademy.clients.rest.common import ApiClient, ApiContainer


class UserApi(ApiContainer):

    def __init__(self, client: ApiClient, course_id: str, user_id: str):
        self.client = client
        self.__course_id = course_id
        self.__user_id = user_id
        self.user_url = f"{self.client.endpoint}/courses/{self.course_id}/users/{self.user_id}"

    @property
    def course_id(self):
        return self.__course_id

    @property
    def user_id(self):
        return self.__user_id

    def get(self) -> Dict[str, Any]:
        response = self.client.api("GET", self.user_url)
        return response.get("users")[0]


class UsersApi(ApiContainer):

    def __init__(self, client: ApiClient, course_id: str):
        self.client = client
        self.__course_id = course_id
        self.users_url = f"{self.client.endpoint}/courses/{self.course_id}/users"

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

    def id(self, user_id: str) -> UserApi:
        return UserApi(self.client, self.course_id, user_id)
