from dbacademy.dougrest.accounts.crud import AccountsCRUD


class PrivateAccessSettings(AccountsCRUD):
    def __init__(self, client):
        # The unusual plural form private_access_settingses is intentional.
        super().__init__(client, "/private-access-settings", "private_access_settings",
                         plural="private_access_settingses")
