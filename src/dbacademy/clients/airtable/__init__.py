from typing import Dict, Any

__all__ = ["AirTable"]


class AirTable(object):
    from requests import Response

    def __init__(self, *, access_token: str, base_id: str, table_id: str, error_handler: ErrorHandler = ErrorHandler()):
        self.__base_id = base_id
        self.__table_id = table_id
        self.__access_token = access_token
        self.__url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
        self.__error_handler = error_handler

        self.__headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    @property
    def access_token(self) -> str:
        return self.__access_token

    @property
    def base_id(self) -> str:
        return self.__base_id

    @property
    def table_id(self) -> str:
        return self.__table_id

    def read(self, view_id: str = None, filter_by_formula: str = None, sort_by: str = None, sort_asc: bool = True):
        import requests

        url = f"{self.__url}"

        if view_id is not None:
            url += "?" if url == self.__url else "&"
            url += f"""view={view_id}"""

        if filter_by_formula is not None:
            url += "?" if url == self.__url else "&"
            url += f"""filterByFormula={filter_by_formula}"""

        if sort_by is not None:
            url += "?" if url == self.__url else "&"
            url += f"""sort[0][field]={sort_by}"""

            if sort_asc:
                url += f"""&sort[0][direction]=asc"""
            else:
                url += f"""&sort[0][direction]=desc"""

        response = requests.get(url, headers=self.__headers)
        self.assert_response(response, f"""Exception reading records from the database: {response.status_code}""")

        json_object = response.json()
        return json_object.get("records", list())

    def update(self, record_id: str, *, fields: Dict[str, Any]):
        import requests

        url = f"{self.__url}/{record_id}"

        payload = {
            "typecast": True,
            "fields": fields
        }
        response = requests.patch(url, headers=self.__headers, json=payload)
        self.assert_response(response, f"""Exception writing records to the database: {response.status_code}""")

    def assert_response(self, response: Response, message: str) -> None:
        import json

        if response.status_code != 200:
            self.__error_handler.on_error(message, json.dumps(response.json(), indent=4))
            raise AssertionError(message)
