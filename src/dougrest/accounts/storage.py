from dbacademy.dougrest.accounts.crud import AccountsCRUD


class StorageConfigurations(AccountsCRUD):
    def __init__(self, client):
        super().__init__(client, "/storage-configurations", "storage_configuration")
