from dbacademy.dougrest.accounts.crud import AccountsCRUD


class LogDeliveryConfigurations(AccountsCRUD):
    def __init__(self, client):
        super().__init__(client, "/log-delivery", "config")
