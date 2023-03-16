from typing import Union, Dict, Any, List, Optional
from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer


class WorkspaceClient(ApiContainer):
    def __init__(self, client: DBAcademyRestClient):
        self.client = client

    def ls(self, path: str, recursive: bool = False, object_types: List[str] = None) -> Optional[List[Dict[str, Any]]]:

        object_types = object_types or ["NOTEBOOK"]

        if not recursive:
            try:
                results = self.client.api("GET", f"{self.client.endpoint}/api/2.0/workspace/list", _expected=(200, 404), path=path)

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
                    if result is not None:
                        queue.extend(result)

            return entities

    def mkdirs(self, path: str) -> Dict[str, Any]:
        params = {"path": path}
        return self.client.api("POST", f"{self.client.endpoint}/api/2.0/workspace/mkdirs", params)

    def delete_path(self, path: str) -> Dict[str, Any]:
        payload = {"path": path, "recursive": True}
        expected = [200, 404]
        return self.client.api("POST", f"{self.client.endpoint}/api/2.0/workspace/delete", payload, _expected=expected)

    def import_html_file(self, html_path: str, content: str, overwrite: bool = True) -> Dict[str, Any]:
        import base64

        payload = {
            "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
            "path": html_path,
            "language": "PYTHON",
            "overwrite": overwrite,
            "format": "SOURCE",
        }
        return self.client.api("POST", f"{self.client.endpoint}/api/2.0/workspace/import", payload)

    def import_dbc_files(self, target_path: str, source_url: str, overwrite: bool = True, local_file_path: str = None) -> Dict[str, Any]:
        import os, base64
        from urllib import request

        if local_file_path is None and source_url is None:
            raise AssertionError(f"Either the local_file_path ({local_file_path}) or source_url ({source_url}) parameter must be specified")

        if local_file_path is None:
            file_name = source_url.split("/")[-1]
            local_file_path = f"/tmp/{file_name}"

        if source_url is not None:
            if os.path.exists(local_file_path):
                os.remove(local_file_path)

            request.urlretrieve(source_url, local_file_path)

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
        return self.client.api("POST", f"{self.client.endpoint}/api/2.0/workspace/import", payload)

    def import_notebook(self, language: str, notebook_path: str, content: str, overwrite: bool = True) -> Dict[str, Any]:
        import base64

        payload = {
            "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
            "path": notebook_path,
            "language": language,
            "overwrite": overwrite,
            "format": "SOURCE",
        }
        return self.client.api("POST", f"{self.client.endpoint}/api/2.0/workspace/import", payload)

    def export_notebook(self, path: str) -> str:
        return self.client.api("GET", "2.0/workspace/export",
                               path=path,
                               format="SOURCE",
                               direct_download=True, _result_type=str)

    def export_dbc(self, path: str) -> bytes:
        return self.client.api("GET", "2.0/workspace/export",
                               path=path,
                               format="DBC",
                               direct_download=True, _result_type=bytes)

    def get_status(self, path: str) -> Union[None, dict]:
        return self.client.api("GET", "2.0/workspace/get-status", path=path, _expected=[200, 404])
