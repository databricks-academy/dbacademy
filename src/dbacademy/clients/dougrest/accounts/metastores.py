from typing import List

from dbacademy.clients.dougrest.accounts.crud import AccountsCRUD
from dbacademy.rest.common import HttpStatusCodes, Item


class Metastores(AccountsCRUD):
    def __init__(self, client):
        super().__init__(client, "/metastores", "metastores")

    def _list(self, *, _expected: HttpStatusCodes = None) -> List[Item]:
        return super()._list().get("metastores", [])
