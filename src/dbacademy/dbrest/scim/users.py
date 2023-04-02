from typing import Dict, Any, Union, List, Optional
from dbacademy.rest.common import ApiContainer


class ScimUsersClient(ApiContainer):
    from dbacademy.dbrest import DBAcademyRestClient

    def __init__(self, client: DBAcademyRestClient):
        self.client = client      # Client API exposing other operations to this class

    def list(self) -> List[Dict[str, Any]]:
        response = self.client.api("GET", f"{self.client.endpoint}/api/2.0/preview/scim/v2/Users?excludedAttributes=roles")

        users = response.get("Resources", list())
        total_results = response.get("totalResults")

        assert len(users) == int(total_results), f"""The returned value "totalResults" ({total_results}) does not match the number of records ({len(users)}) returned."""

        return users

    def get_by_id(self, user_id: str) -> Dict[str, Any]:
        url = f"{self.client.endpoint}/api/2.0/preview/scim/v2/Users/{user_id}"
        return self.client.api("GET", url)

    def get_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        import urllib.parse
        # return self.get_by_name(username)

        name = urllib.parse.quote(username)

        response = self.client.api("GET", f"""{self.client.endpoint}/api/2.0/preview/scim/v2/Users?excludedAttributes=roles&filter=userName eq "{name}""")
        users = response.get("Resources", list())
        total_results = response.get("totalResults")
        assert len(users) == int(total_results), f"""The returned value "totalResults" ({total_results}) does not match the number of records ({len(users)}) returned."""

        for user in users:
            if username == user.get("userName"):
                return user

        return None

    def get_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        for user in self.list():
            if name == user.get("userName"):
                return user

        return None

    def delete_by_id(self, user_id: str) -> None:
        url = f"{self.client.endpoint}/api/2.0/preview/scim/v2/Users/{user_id}"
        return self.client.api("DELETE", url, _expected=204)

    def delete_by_username(self, username: str) -> None:
        for user in self.list():
            if username == user.get("userName"):
                return self.delete_by_id(user.get("id"))

        return None

    def create(self, username: str) -> Dict[str, Any]:
        from dbacademy.rest.common import DatabricksApiException

        payload = {
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
            "userName": username,
            "groups": [],
            "entitlements": []
        }

        try:
            url = f"{self.client.endpoint}/api/2.0/preview/scim/v2/Users"
            return self.client.api("POST", url, payload, _expected=(200, 201))
        except DatabricksApiException as e:
            if e.http_code == 409:
                # TODO Remove this once ES-645542 is fixed
                # 100% convinced this is throttling masking itself as a 409 & the list here will help that a bit.
                user = self.get_by_name(username)
                if user is not None:
                    return user

            # If we didn't mitigate, re-raise the exception.
            raise Exception(f"""Exception creating the user "{username}".""") from e

    def to_users_list(self, users: Union[None, str, Dict[str, Any]]) -> List[Dict[str, Any]]:

        # One way or the other, we will use the full list
        all_users = self.list()

        if users is None:
            users = all_users
        elif type(users) == str or type(users) == dict:
            users = [users]  # Convert single argument users to a list
        else:
            assert type(users) == list, f"Expected the parameter \"users\" to be None, str or Dict, found {type(users)}"

        new_users = list()

        for user in users:
            if type(user) == dict:
                new_users.append(user)

            elif type(user) == str:
                if "@" in user:
                    for u in all_users:
                        if u.get("userName") == user: 
                            new_users.append(u)
                else:
                    for u in all_users:
                        if u.get("id") == user: 
                            new_users.append(u)

        return new_users
    
    def add_entitlement(self, user_id: str, entitlement: str) -> Dict[str, Any]:
        payload = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [
                {
                    "op": "add",
                    "path": "entitlements",
                    "value": [
                        {
                            "value": entitlement
                        }
                    ]
                }
            ]
        }
        url = f"{self.client.endpoint}/api/2.0/preview/scim/v2/Users/{user_id}"
        return self.client.api("PATCH", url, payload)

    def remove_entitlement(self, user_id: str, entitlement: str) -> Dict[str, Any]:
        params = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [
                {
                    "op": "remove",
                    "path": f"""entitlements[value eq "{entitlement}"]""",
                }
            ]
        }
        url = f"{self.client.endpoint}/api/2.0/preview/scim/v2/Users/{user_id}"
        return self.client.api("PATCH", url, params)
