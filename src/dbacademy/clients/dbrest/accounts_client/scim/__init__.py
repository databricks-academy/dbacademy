__all__ = ["AccountScimApi"]

from dbacademy.clients.rest.common import ApiContainer
from dbacademy.clients.dbrest.accounts_client.scim.users import AccountScimUsersApi
from dbacademy.clients.rest.common import ApiClient


class AccountScimApi(ApiContainer):

    def __init__(self, client: ApiClient, account_id: str):
        from dbacademy.common import validate

        self.__client = validate(client=client).required.as_type(ApiClient)
        self.__account_id = account_id

        # from dbacademy.clients.dbrest.accounts.scim.service_principals import ScimServicePrincipalsClient
        # self.service_principals = ScimServicePrincipalsClient(self.__client)
        #
        # from dbacademy.clients.dbrest.accounts.scim.groups import ScimGroupsClient
        # self.groups = ScimGroupsClient(self.__client)

    @property
    def account_id(self) -> str:
        return self.__account_id

    @property
    def users(self) -> AccountScimUsersApi:
        return AccountScimUsersApi(self.__client, self.account_id)
