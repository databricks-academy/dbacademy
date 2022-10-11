from dbacademy.dougrest.accounts.crud import AccountsCRUD


class Credentials(AccountsCRUD):
    def __init__(self, client):
        super().__init__(client, "/credentials", "credentials", singular="credential")
