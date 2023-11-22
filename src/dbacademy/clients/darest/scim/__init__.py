__all__ = ["ScimClient"]

from typing import Dict, Any
from dbacademy.clients.rest.common import ApiClient, ApiContainer


class ScimClient(ApiContainer):
    def __init__(self, client: ApiClient):
        self.client = client      # Client API exposing other operations to this class

        from dbacademy.clients.darest.scim.users import ScimUsersClient
        self.users = ScimUsersClient(self.client)

        from dbacademy.clients.darest.scim.service_principals import ScimServicePrincipalsClient
        self.service_principals = ScimServicePrincipalsClient(self.client)

        from dbacademy.clients.darest.scim.groups import ScimGroupsClient
        self.groups = ScimGroupsClient(self.client)

    def me(self) -> Dict[str, Any]:
        return self.client.api("GET", f"{self.client.endpoint}/api/2.0/preview/scim/v2/Me")
