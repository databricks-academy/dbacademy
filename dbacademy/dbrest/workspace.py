# Databricks notebook source
from dbacademy.dbrest import DBAcademyRestClient


class WorkspaceClient:
    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client
        self.token = token
        self.endpoint = endpoint

    def ls(self, path, recursive=False):
        if not recursive:
            try:
                results = self.client.execute_get_json(f"{self.endpoint}/api/2.0/workspace/list?path={path}", expected=[200, 404])
                if results is None:
                    return None
                else:
                    return results["objects"] if "objects" in results else []

            except Exception as e:
                raise Exception(f"Unexpected exception listing {path}") from e
        else:
            entities = []
            queue = self.ls(path)
            
            if queue is None:
                return None

            while len(queue) > 0:
                next = queue.pop()
                object_type = next["object_type"]
                if object_type == "NOTEBOOK":
                    entities.append(next)
                elif object_type == "DIRECTORY":
                    queue.extend(self.ls(next["path"]))
                    
            return entities

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
        payload = {"path": path, "recursive": True}
        return self.client.execute_post_json(f"{self.endpoint}/api/2.0/workspace/delete", payload, expected=[200, 404])

    def import_html_file(self, html_path:str, content:str, overwrite=True) -> dict:
        import base64

        payload = {
            "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
            "path": html_path,
            "overwrite": overwrite,
            "format": "HTML",
        }
        return self.client.execute_post_json(f"{self.endpoint}/api/2.0/workspace/import", payload)

    def import_notebook(self, language:str, notebook_path:str, content:str, overwrite=True) -> dict:
        import base64

        payload = {
            "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
            "path": notebook_path,
            "language": language,
            "overwrite": overwrite,
            "format": "SOURCE",
        }
        return self.client.execute_post_json(f"{self.endpoint}/api/2.0/workspace/import", payload)

    def export_notebook(self, notebook_path) -> str:
        from urllib.parse import urlencode
        params = urlencode({
            "path" : notebook_path, 
            "direct_download" : "true"
        })
        return self.client.execute_get(f"{self.endpoint}/api/2.0/workspace/export?{params}").text

    def get_status(self, notebook_path) -> dict:
        from urllib.parse import urlencode
        params = urlencode({
            "path" : notebook_path
        })
        response = self.client.execute_get(f"{self.endpoint}/api/2.0/workspace/get-status?{params}", expected=[200,404])
        if response.status_code == 404:
            return None
        else:
            assert response.status_code == 200, f"({response.status_code}): {response.text}"
            return response.json()