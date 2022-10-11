from dbacademy.dougrest.accounts.crud import AccountsCRUD


class Budgets(AccountsCRUD):
    def __init__(self, client):
        super().__init__(client, "/budget", "budget")
