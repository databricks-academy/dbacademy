__all__ = ["SessionsClient"]

from typing import List, Dict, Any
from dbacademy.clients.docebo import DoceboRestClient
from dbacademy.clients.rest.common import ApiContainer


class SessionsClient(ApiContainer):
    def __init__(self, client: DoceboRestClient):
        self.client = client

    def get_sessions_by_course_id(self, course_id: Any) -> List[Dict[str, Any]]:
        assert course_id is not None, f"The course_id parameter must be specified."

        # TODO - make sure that this dataset is not paged
        response = self.client.api("GET", f"/course/v1/courses/{course_id}/sessions")
        data = response.get("data", dict())
        items = data.get("items", list())
        return items
