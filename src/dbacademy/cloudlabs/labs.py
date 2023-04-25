from typing import List, Dict, Any, Union

from dbacademy.cloudlabs import CloudlabsApi, Tenant
from dbacademy.rest.common import ApiContainer, DatabricksApiException

Lab = Dict[str, Any]


class Labs(ApiContainer):
    def __init__(self, tenant: Tenant):
        self.client = tenant

    @staticmethod
    def to_id(lab: Union[int, Lab]) -> int:
        if isinstance(lab, int):
            return lab
        elif isinstance(lab, dict):
            return lab["Id"]
        else:
            raise ValueError(f"lab must be int or dict, found {type(lab)}")

    def list(self) -> List[Lab]:
        labs = self.client.api("POST", "/api/OnDemandLab/GetOnDemandLabs", {
            "State": "1",
            "InstructorId": None,
            "StartIndex": 1000,  # Page size
            "PageCount": 1,
        })
        return labs

    def find_one_by_key(self, key: str, value: Union[str, int]) -> Lab:
        labs = self.list()
        for lab in labs:
            if lab[key] == value:
                return lab
        else:
            raise DatabricksApiException(f"No lab found with {key}={value!r}", 404)

    def find_all_by_key(self, key: str, value: Union[str, int]) -> List[Lab]:
        labs = self.list()
        return [lab for lab in labs if lab[key] == value]

    def get_by_id(self, lab_id: int) -> Lab:
        return self.find_one_by_key("Id", lab_id)

    def get_by_bitly(self, bitly_url: str) -> Lab:
        return self.find_one_by_key("BitLink", bitly_url)

    def get_by_title(self, title: str) -> Lab:
        return self.find_one_by_key("Title", title)
