__all__ = ["CoursesAPI"]

from typing import Dict, Any
from dbacademy.clients.rest.common import ApiClient, ApiContainer


class CoursesAPI(ApiContainer):

    def __init__(self, client: ApiClient):
        self.client = client

    def get_course_by_id(self, course_id: Any) -> Dict[str, Any]:
        assert course_id is not None, f"The course_id parameter must be specified."

        response = self.client.api("GET", f"/course/v1/courses/{course_id}", _expected=[200, 404])
        return None if response is None else response.get("data", None)
