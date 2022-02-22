from dbacademy.dbrest import DBAcademyRestClient


class UcClient:
    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client
        self.token = token
        self.endpoint = endpoint

    def metastore_summary(self):
        return self.client.execute_get_json(f"{self.endpoint}/api/2.0/unity-catalog/metastore_summary")

    def set_default_metastore(self, workspace_id, catalog_name, metastore_id) -> str:
        payload = {
            "default_catalog_name": catalog_name,
            "metastore_id": metastore_id
        }
        return self.client.execute_put_json(f"{self.endpoint}/api/2.0/unity-catalog/workspaces/{workspace_id}/metastore", payload)
