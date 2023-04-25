from typing import List, Dict, Any, Union

from dbacademy.cloudlabs import CloudlabsApi, Tenant
from dbacademy.cloudlabs.labs import Lab, Labs
from dbacademy.rest.common import ApiContainer, DatabricksApiException

Template = Dict[str, Any]


class Templates(ApiContainer):
    def __init__(self, tenant: Tenant):
        self.tenant = tenant

    @staticmethod
    def to_id(template: Union[int, Template]) -> int:
        if isinstance(template, int):
            return template
        elif isinstance(template, dict):
            return template["Id"]
        else:
            raise ValueError(f"template must be int or dict, found {type(template)}")

    def list(self, **filters) -> List[Template]:
        return self.tenant.api("POST", "/api/WorkshopTemplates/GetTemplatesByFilter", {
            "State": "8",
            "StartIndex": 500,  # This is the page size
            "PageCount": 1,
        } | filters)

    def find_one_by_key(self, key: str, value: Union[str, int], **filters) -> Template:
        labs = self.list()
        for lab in labs:
            if lab[key] == value:
                return lab
        else:
            raise DatabricksApiException(f"No lab found with {key}={value!r}", 404)

    def find_all_by_key(self, key: str, value: Union[str, int], **filters) -> List[Template]:
        labs = self.list()
        return [lab for lab in labs if lab[key] == value]

    def get_by_id(self, template_id: int) -> Template:
        return self.find_one_by_key("Id", template_id, TemplateId=template_id)

    def get_by_name(self, name: str) -> Template:
        return self.find_one_by_key("Name", name, TemplateName=name)
