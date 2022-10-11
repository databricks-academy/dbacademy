from dbacademy.dougrest.accounts.crud import AccountsCRUD


class CustomerManagedKeys(AccountsCRUD):
    def __init__(self, client):
        super().__init__(client, "/customer-managed-keys", "customer_managed_key")

    def history(self):
        """Get a list of records of how key configurations were associated with workspaces."""
        return self.client.api("GET", "/customer-managed-key-history")
