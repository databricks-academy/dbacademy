__all__ = ["AccountScimUsersApi", "Operation"]

from typing import Dict, Any, Union, List, Optional, Literal
from dbacademy.clients.rest.common import ApiContainer, ApiClient


class Operation:

    ADD = "add"
    REMOVE = "remove"
    REPLACE = "replace"
    Command = Literal[(ADD, REMOVE, REPLACE)]

    def __init__(self, command: Command, path: str, value: Any):
        self.command = command
        self.path = path
        self.value = value

    def to_dict(self) -> Dict[str, Any]:
        return {
            "op": self.command,
            "path": self.path,
            "value": self.value
        }


class AccountScimUsersApi(ApiContainer):

    def __init__(self, client: ApiClient, account_id: str):
        self.client = client      # Client API exposing other operations to this class
        self.account_id = account_id
        self.base_url = f"{self.client.endpoint}/api/2.0/accounts/{account_id}/scim/v2/Users"

    def list(self, users: List[Dict[str, Any]] = None, start_index: int = 1, users_per_request: int = 1000) -> List[Dict[str, Any]]:
        users = users or list()

        response = self.client.api("GET", self.base_url, startIndex=start_index, count=users_per_request, excludedAttributes="roles")
        new_users = response.get("Resources", list())
        users.extend(new_users)

        if len(new_users) > 0:
            return self.list(users, len(users)+1)

        return users

    def get_by_id(self, user_id: str) -> Dict[str, Any]:
        url = f"{self.base_url}/{user_id}"
        return self.client.api("GET", url)

    def get_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        import urllib.parse
        # return self.get_by_name(username)

        name = urllib.parse.quote(username)

        response = self.client.api("GET", f"""{self.base_url}?excludedAttributes=roles&filter=userName eq "{name}""")
        users = response.get("Resources", list())
        total_results = response.get("totalResults")
        assert len(users) == int(total_results), f"""The returned value "totalResults" ({total_results}) does not match the number of records ({len(users)}) returned."""

        for user in users:
            if username == user.get("userName"):
                return user

        return None

    def update_by_id(self, user_id: str, *, first_name: str = None, last_name: str = None, operations: List[Operation] = None) -> Dict[str, Any]:
        from dbacademy.common import validate

        operations = operations or list()

        if first_name is not None or last_name is not None:
            validate.str_value(first_name=first_name, required=True)
            validate.str_value(last_name=first_name, required=True)

            operations.append(Operation(Operation.REPLACE, "name.familyName", first_name))
            operations.append(Operation(Operation.REPLACE, "name.givenName", last_name))
            operations.append(Operation(Operation.REPLACE, "displayName", f"{first_name} {last_name}"))

        validate.list_value(operations=operations, min_length=1)
        validate.element_type(operations, "operations", Operation)

        assert len(operations) > 0, f"No changes where specified; please provide at least one parameter."

        operations_list: List[Dict[str, Any]] = list()

        for operation in operations:
            operations_list.append(operation.to_dict())

        payload = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": operations_list
        }

        url = f"{self.base_url}/{user_id}"
        self.client.api("PATCH", url, payload)

        return self.get_by_id(user_id)

    def delete_by_id(self, user_id: str) -> None:
        url = f"{self.base_url}/{user_id}"
        self.client.api("DELETE", url, _expected=204)
        return None

    def delete_by_username(self, username: str) -> None:
        for user in self.list():
            if username == user.get("userName"):
                return self.delete_by_id(user.get("id"))

        return None

    def create(self, username: str) -> Dict[str, Any]:
        try:
            payload = {
                "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
                "userName": username,
                "groups": [],
                "entitlements": []
            }

            return self.client.api("POST", self.base_url, payload, _expected=(200, 201))
        except Exception as e:
            raise e

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
        url = f"{self.base_url}/{user_id}"
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
        url = f"{self.base_url}/{user_id}"
        return self.client.api("PATCH", url, params)
