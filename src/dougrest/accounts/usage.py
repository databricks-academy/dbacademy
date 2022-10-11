from dbacademy.rest.common import ApiContainer


class Usage(ApiContainer):
    def __init__(self, accounts):
        super().__init__()
        self.accounts = accounts

    def download(self, start_month, end_month, personal_data=False):
        return self.accounts.api("GET", "/usage/download", data={
            "start_month": start_month,
            "end_month": end_month,
            "personal_data": personal_data,
        })["message"]
