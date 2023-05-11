from typing import List, Dict, Any, Union

from dbacademy.cloudlabs import Tenant
from dbacademy.cloudlabs.labs import Lab, Labs
from dbacademy.rest.common import ApiContainer

Instructor = Dict[str, Any]


class Instructors(ApiContainer):
    def __init__(self, tenant: Tenant):
        self.tenant = tenant

    @staticmethod
    def to_id(instructor: Union[int, Instructor]) -> int:
        if isinstance(instructor, int):
            return instructor
        elif isinstance(instructor, dict):
            return instructor["Id"]
        else:
            raise ValueError(f"instructor must be int or dict, found {type(instructor)}")

    def get_instructors_for_lab(self, lab: Union[int, Lab], fetch_creds=False) -> List[Instructor]:
        lab_id = Labs.to_id(lab)
        instructors = self.tenant.api("GET", "/api/Instructor/GetODLInstructors", eventId=lab_id)
        if fetch_creds:
            creds_list = self.tenant.do_batch(lambda i: self.get_instructor_creds(lab, i), instructors)
            for instructor, creds in zip(instructors, creds_list):
                instructor["UserName"] = creds["AADEmail"]
                instructor["Password"] = creds["TempPassword"]
        return instructors

    def get_instructor_creds(self, lab: Union[int, Lab], instructor: Union[int, Instructor]) -> Dict[str, Any]:
        lab_id = Labs.to_id(lab)
        instructor_id = Instructors.to_id(instructor)
        return self.tenant.api("GET", f"/api/Instructor/GetEventInstructorCloudUser/{lab_id}/{instructor_id}")
