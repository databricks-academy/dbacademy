__all__ = ["AssignmentsClient"]

from typing import Dict, Any, List
from dbacademy.clients.rest.common import ApiContainer


class AssignmentClient(ApiContainer):
    from dbacademy.clients.vocareum import VocareumRestClient

    def __init__(self, client: VocareumRestClient, course_id: str, assignment_id: str):
        self.client = client
        self.__course_id = course_id
        self.__assignment_id = assignment_id
        self.course_url = f"{self.client.url[:-1]}/courses/{self.course_id}/assignments/{self.assignment_id}"

    @property
    def course_id(self):
        return self.__course_id

    @property
    def assignment_id(self):
        return self.__assignment_id

    def clone(self) -> Dict[str, Any]:
        raise Exception("This function is not yet implemented")

    def get(self) -> Dict[str, Any]:
        response = self.client.api("GET", self.course_url)
        return response.get("assignments")[0]


class AssignmentsClient(ApiContainer):
    from dbacademy.clients.vocareum import VocareumRestClient

    def __init__(self, client: VocareumRestClient, course_id: str):
        self.client = client
        self.__course_id = course_id
        self.assignments_url = f"{self.client.url[:-1]}/courses/{self.course_id}/assignments"

    @property
    def course_id(self):
        return self.__course_id

    def list(self) -> List[Dict[str, Any]]:
        import sys

        assignments = list()

        for page in range(0, sys.maxsize):
            response = self.client.api("GET", f"{self.assignments_url}?page={page}")
            records = response.get("assignments", list())

            if len(records) == 0:
                break
            else:
                assignments.extend(records)

        return assignments

    def id(self, assignment_id: str) -> AssignmentClient:
        return AssignmentClient(self.client, self.course_id, assignment_id)
