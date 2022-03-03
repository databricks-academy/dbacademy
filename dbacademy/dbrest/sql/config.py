from dbacademy.dbrest import DBAcademyRestClient

DATA_ACCESS_CONTROL = "DATA_ACCESS_CONTROL"
SECURITY_POLICIES = [DATA_ACCESS_CONTROL]

class SqlConfigClient:

    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client
        self.token = token
        self.endpoint = endpoint

    def get(self):
        return self.client.execute_get_json(f"{self.endpoint}/api/2.0/sql/config/endpoints")

    def edit(self, security_policy:str, instance_profile_arn:str, data_access_config:dict, sql_configuration_parameters:dict):

        assert security_policy in SECURITY_POLICIES, f"Expected security_policy to be one of {SECURITY_POLICIES}, found {security_policy}"

        params = {
            "security_policy": security_policy,
            "data_access_config": [],
            "sql_configuration_parameters": {"configuration_pairs": []}
        }

        for key in data_access_config:
            value = data_access_config[key]
            params.get("data_access_config").append({
                "key": key,
                "value": value
            })

        for key in sql_configuration_parameters:
            value = sql_configuration_parameters[key]
            params.get("sql_configuration_parameters").get("configuration_pairs").append({
                "key": key,
                "value": value
            })

        return params
        # return self.client.execute_post_json(f"{self.endpoint}/api/2.0/sql/config/endpoints")
