from typing import Union, List

from overrides import overrides

from dbacademy.rest.common import *
from dbacademy.rest.crud import CRUD


class AccountsCRUD(CRUD):
    @overrides
    def _list(self, *, expected: HttpErrorCodes = None) -> List[Item]:
        """Returns a list of all {plural}."""
        return self.client.api("GET", self.path, expected=expected)

    @overrides
    def _get(self, item_id: ItemId, *, expected: HttpErrorCodes = None) -> Item:
        """Perform API call for get item"""
        return self.client.api("GET", f"{self.path}/{item_id}", expected=expected)

    @overrides
    def _create(self, item: Item, *, expected: HttpErrorCodes = None) -> Union[Item, str]:
        return self.client.api("POST", f"{self.path}", item, expected=expected)

    @overrides
    def _update(self, item: Item, *, expected: HttpErrorCodes = None) -> Union[Item, str]:
        item_id = item[self.id_key]
        return self.client.api("PUT", f"{self.path}/{item_id}", item, expected=expected)

    @overrides
    def _delete(self, item_id, *, expected: HttpErrorCodes = None):
        return self.client.api("DELETE", f"{self.path}/{item_id}", expected=expected)
