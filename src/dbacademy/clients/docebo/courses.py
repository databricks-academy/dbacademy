__all__ = ["CoursesClient"]

from typing import Dict, Any
from dbacademy.clients.docebo import DoceboRestClient
from dbacademy.clients.rest.common import ApiContainer


class CoursesClient(ApiContainer):
    def __init__(self, client: DoceboRestClient):
        self.client = client

    def get_course_by_id(self, course_id: Any) -> Dict[str, Any]:
        assert course_id is not None, f"The course_id parameter must be specified."

        response = self.client.api("GET", f"/course/v1/courses/{course_id}")
        return response.get("data", None)
