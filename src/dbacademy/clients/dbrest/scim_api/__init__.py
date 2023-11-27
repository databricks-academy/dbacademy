__all__ = ["ScimApi"]
# Code Review: JDP on 11-27-2023

from typing import Dict, Any
from dbacademy.common import validate
from dbacademy.clients.rest.common import ApiContainer, ApiClient
from dbacademy.clients.dbrest.scim_api.users_api import ScimUsersApi
from dbacademy.clients.dbrest.scim_api.service_principals_api import ScimServicePrincipalsApi
from dbacademy.clients.dbrest.scim_api.groups_api import ScimGroupsApi


class ScimApi(ApiContainer):

    def __init__(self, client: ApiClient):
        self.__client = validate(client=client).required.as_type(ApiClient)

    @property
    def users(self) -> ScimUsersApi:
        return ScimUsersApi(self.__client)

    @property
    def service_principals(self) -> ScimServicePrincipalsApi:
        return ScimServicePrincipalsApi(self.__client)

    @property
    def groups(self) -> ScimGroupsApi:
        return ScimGroupsApi(self.__client)

    def me(self) -> Dict[str, Any]:
        return self.__client.api("GET", f"{self.__client.endpoint}/api/2.0/preview/scim/v2/Me")
