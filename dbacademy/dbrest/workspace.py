# Databricks notebook source
from dbacademy.dbrest import DBAcademyRestClient


class WorkspaceClient:
    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client
        self.token = token
        self.endpoint = endpoint

    def ls(self, path):
        return self.client.execute_get_json(f"{self.endpoint}/api/2.0/workspace/list?path={path}")["objects"]

    def ls_pd(self, path):
        # I don't have Pandas and I don't want to have to add Pandas.
        # Use local import so as to not require project dependencies
        # noinspection PyPackageRequirements
        import pandas as pd

        objects = pd.DataFrame(self.ls(path))
        objects["object"] = objects["path"].apply(lambda p: p.split("/")[-1])
        return_cols = ["object", "object_type", "object_id", "language", "path"]
        return objects[return_cols].sort_values("object")

    def mkdirs(self, path) -> dict:
        return self.client.execute_post_json(f"{self.endpoint}/api/2.0/workspace/mkdirs", {"path": path})

    def delete_path(self, path) -> dict:
        print("-" * 80)
        print(f"Deleting {path}")
        payload = {"path": path, "recursive": True}

        return self.client.execute_post_json(f"{self.endpoint}/api/2.0/workspace/delete", payload, expected=[200, 404])

    def import_notebook(self, language, notebook_path, content) -> dict:
        import base64

        payload = {
            "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
            "path": notebook_path,
            "language": language,
            "overwrite": True,
            "format": "SOURCE",
        }

        return self.client.execute_post_json(f"{self.endpoint}/api/2.0/workspace/import", payload)

    def export_notebook(self, notebook_path) -> str:
        return self.client.execute_get(f"{self.endpoint}/api/2.0/workspace/export?path={notebook_path}&direct_download=true").text

    def get_status(self, notebook_path) -> dict:
        response = self.client.execute_get(f"{self.endpoint}/api/2.0/workspace/get-status?path={notebook_path}", expected=[200,404])
        if response.status_code == 404:
            return None
        else:
            assert response.status_code in expected, f"({response.status_code}): {response.text}"
            return response.json()
