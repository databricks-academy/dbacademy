__all__ = ["EventsClient"]

from typing import List, Dict, Any
from dbacademy.clients.docebo import DoceboRestClient
from dbacademy.clients.rest.common import ApiContainer


class EventsClient(ApiContainer):
    def __init__(self, client: DoceboRestClient):
        self.client = client

    def get_events_by_session_id(self, session_id: Any) -> List[Dict[str, Any]]:
        assert session_id is not None, f"The session_id parameter must be specified."

        # TODO - make sure that this dataset is not paged
        response = self.client.api("GET", f"/course/v1/sessions/{session_id}/events")
        data = response.get("data", dict())
        items = data.get("items", list())
        return items
