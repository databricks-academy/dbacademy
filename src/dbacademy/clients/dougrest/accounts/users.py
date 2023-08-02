from dbacademy.clients.dougrest.scim.users import Users as UsersBase


class Users(UsersBase):
    def __init__(self, accounts: "AccountsApi"):
        super().__init__(accounts)
        self.path = ""
