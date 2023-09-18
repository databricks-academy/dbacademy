__all__ = ["AccountsClient"]

from dbacademy.clients.rest.common import ApiContainer


class AccountsClient(ApiContainer):
    from dbacademy.clients.rest.common import ApiClient

    def __init__(self, client: ApiClient, account_id: str):
        self.client = client
        self.account_id = account_id

        from dbacademy.dbrest.accounts.scim import AccountScimClient
        self.scim = AccountScimClient(self.client, account_id=account_id)

        from dbacademy.dbrest.accounts.workspaces import WorkspacesClient
        self.workspaces = WorkspacesClient(self, self.account_id)
