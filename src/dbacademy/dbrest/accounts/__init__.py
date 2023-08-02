from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.clients.rest.common import ApiContainer


class AccountsClient(ApiContainer):
    def __init__(self, client: DBAcademyRestClient, account_id: str):
        self.client = client
        self.account_id = account_id

        from dbacademy.dbrest.accounts.scim import AccountScimClient
        self.scim = AccountScimClient(self.client, account_id=account_id)
