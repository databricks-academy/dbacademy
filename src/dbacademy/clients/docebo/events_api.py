__all__ = ["EventsAPI"]

from typing import List, Dict, Any
from dbacademy.clients.rest.common import ApiClient, ApiContainer


class EventsAPI(ApiContainer):

    def __init__(self, client: ApiClient):
        self.client = client

    def get_events_by_session_id(self, session_id: Any) -> List[Dict[str, Any]]:
        assert session_id is not None, f"The session_id parameter must be specified."

        # TODO - make sure that this dataset is not paged
        response = self.client.api("GET", f"/course/v1/sessions/{session_id}/events")
        data = response.get("data", dict())
        items = data.get("items", list())
        return items
