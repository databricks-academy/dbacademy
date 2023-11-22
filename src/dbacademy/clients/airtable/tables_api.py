__all__ = ["TablesAPI"]

from typing import List, Dict, Any
from dbacademy.clients.rest.common import ApiClient
from dbacademy.clients.airtable.at_utils import AirTableUtils
from dbacademy.clients.rest.common import ApiContainer


class TablesAPI(ApiContainer):

    def __init__(self, client: ApiClient, table_id: str, at_utils: AirTableUtils):
        from dbacademy.common import validate
        from dbacademy.clients.airtable import AirTableRestClient

        self.__client: AirTableRestClient = validate(client=client).required.as_type(AirTableRestClient)
        self.__table_id = validate(table_id=table_id).str()
        self.__at_utils: AirTableUtils = validate(at_utils=at_utils).required.as_type(AirTableUtils)
        self.__table_url = f"{client.endpoint}/{table_id}"

    @property
    def at_utils(self) -> AirTableUtils:
        return self.__at_utils

    @property
    def table_id(self) -> str:
        return self.__table_id

    def query(self, view_id: str = None, filter_by_formula: str = None, sort_by: str = None, sort_asc: bool = True, records: List[Dict[str, Any]] = None, offset: str = None) -> List[Dict[str, Any]]:
        import requests

        records = records or list()

        error_message = "Exception reading records from the database"
        url = self.__build_query(view_id=view_id, filter_by_formula=filter_by_formula, sort_by=sort_by, sort_asc=sort_asc, offset=offset)

        for attempt in self.at_utils.ATTEMPTS:
            response = self.__client.api("GET", url, _result_type=requests.Response)

            if not self.at_utils.is_rate_limited(response, attempt):
                # We are not being rate limited, go ahead and validate the response.
                self.at_utils.validate_response(response, error_message, response.status_code)

                response_data = response.json()
                records.extend(response_data.get("records", list()))
                next_offset = response_data.get("offset")
                if next_offset is None:
                    return records
                else:
                    return self.query(view_id=view_id, filter_by_formula=filter_by_formula, sort_by=sort_by, sort_asc=sort_asc, records=records, offset=next_offset)

        self.at_utils.raise_rate_limit_failure(error_message)

    def __build_query(self, *, view_id: str = None, filter_by_formula: str = None, sort_by: str = None, sort_asc: bool = True, offset: str = None):
        from urllib.parse import quote_plus

        url = f"{self.__table_url}"

        if view_id is not None:
            url += "?" if url == self.__table_url else "&"
            url += f"""view={view_id}"""

        if filter_by_formula is not None:
            url += "?" if url == self.__table_url else "&"
            url += f"""filterByFormula={quote_plus(filter_by_formula)}"""

        if sort_by is not None:
            url += "?" if url == self.__table_url else "&"
            url += f"""sort[0][field]={sort_by}"""

            if sort_asc:
                url += f"""&sort[0][direction]=asc"""
            else:
                url += f"""&sort[0][direction]=desc"""

        if offset is not None:
            url += "?" if url == self.__table_url else "&"
            url += f"""offset={offset}"""

        return url

    def update_by_id(self, record_id: str, *, fields: Dict[str, Any]):
        import requests

        error_message = "Exception updating database record"

        payload = {
            "typecast": True,
            "fields": fields
        }

        for attempt in self.at_utils.ATTEMPTS:
            response = self.__client.api("PATCH", f"{self.__table_url}/{record_id}", _data=payload, _result_type=requests.Response)

            if not self.at_utils.is_rate_limited(response, attempt):
                # We are not being rate limited, go ahead and validate the response.
                self.at_utils.validate_response(response, error_message, response.status_code)
                return response.json()

        self.at_utils.raise_rate_limit_failure(error_message)

    def insert(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        import requests

        payload = {
            "typecast": True,
            "fields": fields
        }
        error_message = "Exception inserting records to the database"

        for attempt in self.at_utils.ATTEMPTS:
            response = self.__client.api("POST", self.__table_url, payload, _result_type=requests.Response)

            if not self.at_utils.is_rate_limited(response, attempt):
                # We are not being rate limited, go ahead and validate the response.
                self.at_utils.validate_response(response, error_message, response.status_code)
                return response.json()

        self.at_utils.raise_rate_limit_failure(error_message)

    def delete_by_id(self, record_id: str):
        import requests

        error_message = "Exception deleting records from the database"
        url = f"{self.__table_url}/{record_id}"

        for attempt in self.at_utils.ATTEMPTS:
            response = self.__client.api("DELETE", url, _result_type=requests.Response)

            if not self.at_utils.is_rate_limited(response, attempt):
                # We are not being rate limited, go ahead and validate the response.
                self.at_utils.validate_response(response, error_message, response.status_code)
                return response.json()

        self.at_utils.raise_rate_limit_failure(error_message)
