from dbacademy.dougrest.accounts.crud import AccountsCRUD


class VpcEndpoints(AccountsCRUD):
    def __init__(self, client):
        super().__init__(client, "/vpc-endpoints", "vpc_endpoint")
