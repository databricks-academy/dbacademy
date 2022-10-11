from dbacademy.dougrest.accounts.crud import AccountsCRUD


class NetworkConfigurations(AccountsCRUD):
    def __init__(self, client):
        super().__init__(client, "/networks", "network")
