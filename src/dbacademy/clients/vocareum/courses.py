__all__ = ["CoursesClient"]

from typing import Dict, Any, List
from dbacademy.clients.rest.common import ApiContainer


class CourseClient(ApiContainer):
    from dbacademy.clients.vocareum import VocareumRestClient

    def __init__(self, client: VocareumRestClient, course_id: str):
        self.client = client
        self.__course_id = course_id
        self.course_url = f"{self.client.url[:-1]}/courses/{self.course_id}"

        from dbacademy.clients.vocareum.users import UsersClient
        self.users = UsersClient(self.client, self.course_id)

        from dbacademy.clients.vocareum.assignments import AssignmentsClient
        self.assignments = AssignmentsClient(self.client, self.course_id)

    @property
    def course_id(self):
        return self.__course_id

    def clone_course(self) -> Dict[str, Any]:
        raise Exception("This function is not yet implemented")

    def get(self) -> Dict[str, Any]:
        response = self.client.api("GET", self.course_url)
        return response.get("courses")[0]


class CoursesClient(ApiContainer):
    from dbacademy.clients.vocareum import VocareumRestClient

    def __init__(self, client: VocareumRestClient):
        self.client = client
        self.courses_url = f"{self.client.url[:-1]}/courses"

    def id(self, course_id: str) -> CourseClient:
        return CourseClient(self.client, course_id)

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

