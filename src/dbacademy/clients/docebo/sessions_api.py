__all__ = ["SessionsAPI"]

from typing import List, Dict, Any
from dbacademy.clients.rest.common import ApiClient, ApiContainer


class SessionsAPI(ApiContainer):

    def __init__(self, client: ApiClient):
        self.client = client

    def get_sessions_by_course_id(self, course_id: Any) -> List[Dict[str, Any]]:
        from dbacademy.clients.rest.common import DatabricksApiException

        assert course_id is not None, f"The course_id parameter must be specified."

        # TODO - make sure that this dataset is not paged
        try:
            response = self.client.api("GET", f"/course/v1/courses/{course_id}/sessions")
            data = response.get("data", dict())
            items = data.get("items", list())
            return items

        except DatabricksApiException as e:
            # This is a hack to a bad REST API that returns 400 when it should have returned 404
            if e.http_code == 400 and " does not have sessions" in str(e.message):
                return list()
