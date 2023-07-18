from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer


class AccountScimClient(ApiContainer):
    def __init__(self, client: DBAcademyRestClient, account_id: str):
        self.client = client      # Client API exposing other operations to this class
        self.account_id = account_id

        from dbacademy.dbrest.accounts.scim.users import AccountScimUsersClient
        self.users = AccountScimUsersClient(self.client, account_id)

        # from dbacademy.dbrest.accounts.scim.service_principals import ScimServicePrincipalsClient
        # self.service_principals = ScimServicePrincipalsClient(self.client)
        #
        # from dbacademy.dbrest.accounts.scim.groups import ScimGroupsClient
        # self.groups = ScimGroupsClient(self.client)
