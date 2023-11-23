__all__ = ["MetastoresApi"]

from typing import List, Dict, Any
from dbacademy.clients.rest.common import ApiContainer, ApiClient


class MetastoresApi(ApiContainer):

    def __init__(self, client: ApiClient, account_id: str):
        from dbacademy.common import validate

        self.__client = validate(client=client).required.as_type(ApiClient)
        self.__account_id = validate(account_id=account_id).required.str()

        self.__base_url = f"{self.__client.endpoint}/api/2.0/accounts/{self.account_id}/metastores"

    @property
    def account_id(self) -> str:
        return self.__account_id

    def list(self) -> List[Dict[str, Any]]:
        response = self.__client.api("GET", self.__base_url)
        return response.get("metastores", list())
