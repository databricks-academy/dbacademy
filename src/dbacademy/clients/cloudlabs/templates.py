from typing import List, Dict, Any, Union

from dbacademy.clients.cloudlabs import Tenant
from dbacademy.clients.rest.common import ApiContainer, DatabricksApiException

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

    def list(self, page=1, **filters) -> List[Template]:
        return self.tenant.api("POST", "/api/WorkshopTemplates/GetTemplatesByFilter", {
            "State": "8",
            "StartIndex": 500,  # This is the page size
            "PageCount": page,
        } | filters)

    # TODO doug.bateman@databricks.com: Calling code may produce a bug from this.
    # noinspection PyUnusedLocal
    def list_all(self, **filters) -> List[Template]:
        """
        Returns an iterator that will loop through all pages and return all templates.
        """
        from itertools import count
        for page in count(start=1):
            templates = self.list(page)
            if not templates:
                break
            yield from templates

    # TODO doug.bateman@databricks.com: Calling code may produce a bug from this.
    # noinspection PyUnusedLocal
    def find_one_by_key(self, key: str, value: Union[str, int], **filters) -> Template:
        labs = self.list_all()
        for lab in labs:
            if lab[key] == value:
                return lab
        else:
            raise DatabricksApiException(f"No lab found with {key}={value!r}", 404)

    # TODO doug.bateman@databricks.com: Calling code may produce a bug from this.
    # noinspection PyUnusedLocal
    def find_all_by_key(self, key: str, value: Union[str, int], **filters) -> List[Template]:
        labs = self.list_all()
        return [lab for lab in labs if lab[key] == value]

    def get_by_id(self, template_id: int) -> Template:
        result = self.find_one_by_key("Id", template_id, TemplateId=template_id)
        result["ExcludingOutputParameters"] = ",".join(self.tenant.api(
            "GET",
            f"https://api.cloudlabs.ai/api/WorkshopTemplates/GetExcludingOutputParameters/{template_id}"))
        return result

    def get_by_name(self, name: str) -> Template:
        return self.find_one_by_key("Name", name, TemplateName=name)

    def patch(self, template_id: int, changes: Dict):
        template = self.get_by_id(template_id)
        template.update(changes)
        self.update(template)

    def update(self, template: Dict):
        template_id = template["Id"]
        self.tenant.api("PUT", f"https://api.cloudlabs.ai/api/WorkshopTemplates?templateId={template_id}", template)
