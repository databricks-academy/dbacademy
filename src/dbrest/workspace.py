from typing import Union
from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer


class WorkspaceClient(ApiContainer):
    def __init__(self, client: DBAcademyRestClient):
        self.client = client

    def ls(self, path, recursive=False, object_types=None):

        object_types = object_types or ["NOTEBOOK"]

        if not recursive:
            try:
                results = self.client.execute_get_json(f"{self.client.endpoint}/api/2.0/workspace/list?path={path}", expected=[200, 404])
                if results is None:
                    return None
                else:
                    return results.get("objects", [])

            except Exception as e:
                raise Exception(f"Unexpected exception listing {path}") from e
        else:
            entities = []
            queue = self.ls(path)
            
            if queue is None:
                return None

            while len(queue) > 0:
                next_item = queue.pop()
                object_type = next_item["object_type"]
                if object_type in object_types:
                    entities.append(next_item)
                elif object_type == "DIRECTORY":
                    result = self.ls(next_item["path"])
                    if result is not None: queue.extend(result)

            return entities

    def mkdirs(self, path) -> dict:
        return self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/workspace/mkdirs", {"path": path})

    def delete_path(self, path) -> dict:
        payload = {"path": path, "recursive": True}
        return self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/workspace/delete", payload, expected=[200, 404])

    def import_html_file(self, html_path: str, content: str, overwrite=True) -> dict:
        import base64

        payload = {
            "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
            "path": html_path,
            "language": "PYTHON",
            "overwrite": overwrite,
            "format": "SOURCE",
        }
        return self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/workspace/import", payload)

    def import_dbc_files(self, target_path, source_url=None, overwrite=True, local_file_path=None):
        import os, base64, urllib

        if local_file_path is None and source_url is None:
            raise AssertionError(f"Either the local_file_path ({local_file_path}) or source_url ({source_url}) parameter must be specified")

        if local_file_path is None:
            file_name = source_url.split("/")[-1]
            local_file_path = f"/tmp/{file_name}"

        if source_url is not None:
            if os.path.exists(local_file_path): os.remove(local_file_path)

            # noinspection PyUnresolvedReferences
            urllib.request.urlretrieve(source_url, local_file_path)

        with open(local_file_path, mode='rb') as file:
            content = file.read()

        if overwrite:
            self.delete_path(target_path)

        self.mkdirs("/".join(target_path.split("/")[:-1]))

        payload = {
            "content": base64.b64encode(content).decode("utf-8"),
            "path": target_path,
            "overwrite": False,
            "format": "DBC",
        }
        return self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/workspace/import", payload)

    def import_notebook(self, language: str, notebook_path: str, content: str, overwrite=True) -> dict:
        import base64

        payload = {
            "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
            "path": notebook_path,
            "language": language,
            "overwrite": overwrite,
            "format": "SOURCE",
        }
        return self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/workspace/import", payload)

    def export_notebook(self, path: str) -> str:
        from urllib.parse import urlencode
        params = urlencode({
            "path": path,
            "direct_download": "true"
        })
        return self.client.execute_get(f"{self.client.endpoint}/api/2.0/workspace/export?{params}").text

    def export_dbc(self, path):
        from urllib.parse import urlencode
        params = urlencode({
            "path": path,
            "format": "DBC",
            "direct_download": "true"
        })
        return self.client.execute_get(f"{self.client.endpoint}/api/2.0/workspace/export?{params}").content

    def get_status(self, path) -> Union[None, dict]:
        from urllib.parse import urlencode
        params = urlencode({
            "path": path
        })
        response = self.client.execute_get(f"{self.client.endpoint}/api/2.0/workspace/get-status?{params}", expected=[200, 404])
        if response.status_code == 404:
            return None
        else:
            assert response.status_code == 200, f"({response.status_code}): {response.text}"
            return response.json()
