__all__ = ["ScimClient"]

from typing import Dict, Any
from dbacademy.clients.rest.common import ApiClient, ApiContainer
from dbacademy.clients.darest.scim.users import ScimUsersClient
from dbacademy.clients.darest.scim.service_principals import ScimServicePrincipalsClient
from dbacademy.clients.darest.scim.groups import ScimGroupsClient
from dbacademy.clients.darest import DBAcademyRestClient


class ScimClient(ApiContainer):
    def __init__(self, client: ApiClient):
        from dbacademy.common import validate
        self.__client: DBAcademyRestClient = validate(client=client).required.as_type(DBAcademyRestClient)

    @property
    def client(self) -> DBAcademyRestClient:
        return self.__client

    @property
    def users(self) -> ScimUsersClient:
        return ScimUsersClient(self.client)

    @property
    def service_principals(self) -> ScimServicePrincipalsClient:
        return ScimServicePrincipalsClient(self.client)

    @property
    def groups(self) -> ScimGroupsClient:
        return ScimGroupsClient(self.client)

    def me(self) -> Dict[str, Any]:
        return self.client.api("GET", f"{self.client.endpoint}/api/2.0/preview/scim/v2/Me")
