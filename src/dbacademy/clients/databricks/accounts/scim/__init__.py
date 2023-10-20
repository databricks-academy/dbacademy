__all__ = ["AccountScimClient"]

from dbacademy.clients.rest.common import ApiContainer


class AccountScimClient(ApiContainer):
    from dbacademy.clients.rest.common import ApiClient

    def __init__(self, client: ApiClient, account_id: str):
        self.client = client      # Client API exposing other operations to this class
        self.account_id = account_id

        from dbacademy.clients.databricks.accounts.scim.users import AccountScimUsersClient
        self.users = AccountScimUsersClient(self.client, account_id)

        # from dbacademy.clients.databricks.accounts.scim.service_principals import ScimServicePrincipalsClient
        # self.service_principals = ScimServicePrincipalsClient(self.client)
        #
        # from dbacademy.clients.databricks.accounts.scim.groups import ScimGroupsClient
        # self.groups = ScimGroupsClient(self.client)
