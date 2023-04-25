from dbacademy.dougrest.accounts.crud import AccountsCRUD


class Metastores(AccountsCRUD):
    def __init__(self, client):
        super().__init__(client, "/metastores", "metastores")
