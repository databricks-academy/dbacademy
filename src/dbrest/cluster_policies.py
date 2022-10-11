from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer


class ClustersPolicyClient(ApiContainer):
    def __init__(self, client: DBAcademyRestClient):
        self.client = client
        self.base_uri = f"{self.client.endpoint}/api/2.0/policies/clusters"

    def get_by_id(self, policy_id):
        return self.client.execute_get_json(f"{self.base_uri}/get?policy_id={policy_id}")

    def get_by_name(self, name):
        policies = self.list()
        for policy in policies:
            if policy.get("name") == name:
                return self.get_by_id(policy.get("policy_id"))
        return None

    def list(self):
        # Does not support pagination
        return self.client.execute_get_json(f"{self.base_uri}/list").get("policies", [])

    def create(self, name: str, definition: dict):
        import json
        assert type(name) == str, f"Expected name to be of type str, found {type(name)}"
        assert type(definition) == dict, f"Expected definition to be of type dict, found {type(definition)}"

        params = {
            "name": name,
            "definition": json.dumps(definition)
        }
        response = self.client.execute_post_json(f"{self.base_uri}/create", params=params)
        policy_id = response.get("policy_id")
        return self.get_by_id(policy_id)

    def update_by_name(self, name: str, definition: dict):
        policy = self.get_by_name(name)
        assert policy is not None, f"A policy named \"{name}\" was not found."

        policy_id = policy.get("policy_id")

        return self.update_by_id(policy_id, name, definition)

    def update_by_id(self, policy_id: str, name: str, definition: dict):
        import json
        assert type(policy_id) == str, f"Expected id to be of type str, found {type(policy_id)}"
        assert type(name) == str, f"Expected name to be of type str, found {type(name)}"
        assert type(definition) == dict, f"Expected definition to be of type dict, found {type(definition)}"

        params = {
            "policy_id": policy_id,
            "name": name,
            "definition": json.dumps(definition)
        }
        self.client.execute_post_json(f"{self.base_uri}/edit", params=params)
        return self.get_by_id(policy_id)

    def create_or_update(self, name, definition):
        policy = self.get_by_name(name)

        if policy is None:
            return self.create(name, definition)
        else:
            policy_id = policy.get("policy_id")
            return self.update_by_id(policy_id, name, definition)

    def delete_by_id(self, policy_id):
        return self.client.execute_post_json(f"{self.base_uri}/delete", params={"policy_id": policy_id})

    def delete_by_name(self, name):
        policy = self.get_by_name(name)
        assert policy is not None, f"A policy named \"{name}\" was not found."

        policy_id = policy.get("policy_id")
        return self.delete_by_id(policy_id)
