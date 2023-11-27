__all__ = ["WorkspaceApi"]
# Code Review: JDP on 11-26-2023

from typing import Union, Dict, Any, List, Optional
from enum import Enum
from dbacademy.common import validate
from dbacademy.clients.rest.common import ApiClient, ApiContainer


class Language(Enum):
    SCALA = "SCALA"
    PYTHON = "PYTHON"
    SQL = "SQL"
    R = "R"


class ImportType(Enum):
    SOURCE = "SOURCE"
    HTML = "HTML"
    JUPYTER = "JUPYTER"
    DBC = "DBC"
    R_MARKDOWN = "R_MARKDOWN"
    AUTO = "AUTO"


class WorkspaceApi(ApiContainer):
    def __init__(self, client: ApiClient):
        from dbacademy.common import validate

        self.__client = validate(client=client).required.as_type(ApiClient)
        self.__base_url = f"{self.__client.endpoint}/api/2.0/workspace"

    def ls(self, path: str, *,
           recursive: bool = False,
           object_types: List[str] = None) -> Optional[List[Dict[str, Any]]]:

        path = validate(path=path).required.str()
        object_types = validate(object_types=object_types or ["NOTEBOOK"]).required.list(str)

        if not recursive:
            try:
                results = self.__client.api("GET", f"{self.__base_url}/list", _expected=(200, 404), path=path)

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
        params = {"path": validate(path=path).required.str()}
        return self.__client.api("POST", f"{self.__base_url}/mkdirs", params)

    def delete_path(self, path: str, *, recursive: bool) -> Dict[str, Any]:
        payload = {
            "path": validate(path=path).required.str(),
            "recursive": validate(recursive=recursive).required.bool()
        }
        expected = [200, 404]
        return self.__client.api("POST", f"{self.__base_url}/delete", payload, _expected=expected)

    def import_html_file(self, *,
                         path: str,
                         content: str,
                         language: Union[str, Language],
                         overwrite: bool) -> Dict[str, Any]:
        import base64

        payload = {
            "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
            "path": validate(path=path).required.str(),
            "language": validate(language=language).required.enum(Language, auto_convert=True).value,
            "overwrite": validate(overwrite=overwrite).required.bool(),
            "format": ImportType.SOURCE.value,
        }
        return self.__client.api("POST", f"{self.__base_url}/import", payload)

    def import_dbc_files(self, *,
                         path: str,
                         source_url: Optional[str],
                         overwrite: bool,
                         local_file_path: Optional[str] = None) -> Dict[str, Any]:
        import os, base64
        from urllib import request

        path = validate(path=path).required.str()
        source_url = validate(source_url=source_url).optional.str()
        local_file_path = validate(local_file_path=local_file_path).optional.str()

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

        if validate(overwrite=overwrite).required.bool():
            self.delete_path(path, recursive=True)

        self.mkdirs("/".join(path.split("/")[:-1]))

        payload = {
            "content": base64.b64encode(content).decode("utf-8"),
            "path": path,
            "overwrite": False,
            "format": "DBC",
        }
        return self.__client.api("POST", f"{self.__base_url}/import", payload)

    def import_notebook(self, *,
                        language: Union[str, Language],
                        path: str,
                        content: str,
                        overwrite: bool) -> Dict[str, Any]:
        import base64
        content = validate(content=content).required.str()

        payload = {
            "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
            "path": validate(path=path).required.str(),
            "language": validate(language=language).required.enum(Language, auto_convert=True),
            "overwrite": validate(overwrite=overwrite).required.bool(),
            "format": ImportType.SOURCE.value,
        }
        return self.__client.api("POST", f"{self.__base_url}/import", payload)

    def export_notebook(self, path: str) -> str:
        return self.__client.api("GET", f"{self.__base_url}/export",
                                 path=validate(path=path).required.str(),
                                 format=ImportType.SOURCE.value,
                                 direct_download=True,
                                 _result_type=str)

    def export_dbc(self, path: str) -> bytes:
        return self.__client.api("GET", f"{self.__base_url}/export",
                                 path=validate(path=path).required.str(),
                                 format=ImportType.DBC.value,
                                 direct_download=True,
                                 _result_type=bytes)

    def get_status(self, path: str) -> Optional[Dict[str, Any]]:
        return self.__client.api("GET",
                                 f"{self.__base_url}/get-status",
                                 path=validate(path=path).required.str(),
                                 _expected=[200, 404])
