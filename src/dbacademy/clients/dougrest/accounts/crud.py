from typing import Union, List

from dbacademy.common import overrides
from dbacademy.clients.rest.common import *
from dbacademy.clients.rest.crud import CRUD


class AccountsCRUD(CRUD):
    @overrides
    def _list(self, *, _expected: HttpStatusCodes = None) -> List[Item]:
        """Returns a list of all {plural}."""
        return self.client.api("GET", self.path, _expected=_expected)

    @overrides
    def _get(self, item_id: ItemId, *, _expected: HttpStatusCodes = None) -> Item:
        """Perform API call for get item"""
        return self.client.api("GET", f"{self.path}/{item_id}", _expected=_expected)

    @overrides
    def _create(self, item: Item, *, _expected: HttpStatusCodes = None) -> Union[Item, str]:
        return self.client.api("POST", f"{self.path}", item, _expected=_expected)

    @overrides
    def _update(self, item: Item, *, _expected: HttpStatusCodes = None) -> Union[Item, str]:
        item_id = item[self.id_key]
        return self.client.api("PUT", f"{self.path}/{item_id}", item, _expected=_expected)

    @overrides
    def _delete(self, item_id, *, _expected: HttpStatusCodes = None):
        return self.client.api("DELETE", f"{self.path}/{item_id}", _expected=_expected)
