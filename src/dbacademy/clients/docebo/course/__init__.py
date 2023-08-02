from dbacademy.clients.docebo import DoceboRestClient
from dbacademy.clients.rest.common import ApiContainer


class CourseClient(ApiContainer):

    def __init__(self, client: DoceboRestClient):
        self.client = client

        self.courses = CoursesClient(self.client)


class CoursesClient(ApiContainer):
    def __init__(self, client: DoceboRestClient):
        self.client = client
        url = self.client.url.rstrip("/")
        self.base_uri = f"{url}/course/v1/courses"

    def get_events_by_session(self, session_id):
        # TODO - make sure that this dataset is not paged
        response = self.client.api("GET", f"/course/v1/sessions/{session_id}/events")
        data = response.get("data", dict())
        items = data.get("items", list())
        return items

    def get_sessions_by_course(self, course_id):
        # TODO - make sure that this dataset is not paged
        response = self.client.api("GET", f"/course/v1/courses/{course_id}/sessions")
        data = response.get("data", dict())
        items = data.get("items", list())
        return items

    def get_course(self, course_id):
        response = self.client.api("GET", f"/course/v1/courses/{course_id}")
        return response.get("data")
