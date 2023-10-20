__all__ = ["CoursesApi"]

from typing import Dict, Any, List
from dbacademy.clients.rest.common import ApiClient, ApiContainer


class CourseApi(ApiContainer):

    def __init__(self, client: ApiClient, course_id: str):
        self.client = client
        self.__course_id = course_id
        self.course_url = f"{self.client.endpoint}/courses/{self.course_id}"

        from dbacademy.clients.vocareum.users_api import UsersApi
        self.users = UsersApi(self.client, self.course_id)

        from dbacademy.clients.vocareum.assignments_api import AssignmentsApi
        self.assignments = AssignmentsApi(self.client, self.course_id)

    @property
    def course_id(self):
        return self.__course_id

    def clone_course(self) -> Dict[str, Any]:
        raise Exception("This function is not yet implemented")

    def get(self) -> Dict[str, Any]:
        response = self.client.api("GET", self.course_url)
        return response.get("courses")[0]


class CoursesApi(ApiContainer):

    def __init__(self, client: ApiClient):
        self.client = client
        self.courses_url = f"{self.client.endpoint}/courses"

    def id(self, course_id: str) -> CourseApi:
        return CourseApi(self.client, course_id)

    def list(self) -> List[Dict[str, Any]]:
        import sys

        courses = list()

        for page in range(0, sys.maxsize):
            response = self.client.api("GET", f"{self.courses_url}?page={page}")
            records = response.get("courses", list())

            if len(records) == 0:
                break
            else:
                courses.extend(records)

        return courses
