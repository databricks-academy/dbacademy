from typing import Dict, Any, List

__all__ = ["AirTableClient"]


class AirTableClient(object):
    from requests import Response
    from dbacademy.clients import ClientErrorHandler

    ATTEMPTS = range(1, 10 + 1)  # 1-10 inclusive

    def __init__(self, *, access_token: str, base_id: str, table_id: str, error_handler: ClientErrorHandler = ClientErrorHandler()):
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

    def read(self, view_id: str = None, filter_by_formula: str = None, sort_by: str = None, sort_asc: bool = True, records: List[Dict[str, Any]] = None, offset: str = None) -> List[Dict[str, Any]]:
        import requests

        if records is None:
            records = list()

        error_message = "Exception reading records from the database"
        url = self.__build_url(view_id=view_id, filter_by_formula=filter_by_formula, sort_by=sort_by, sort_asc=sort_asc, offset=offset)

        for attempt in self.ATTEMPTS:
            response = requests.get(url, headers=self.__headers)
            if not self.__rate_limited(response, attempt):
                # We are not being rate limited, go ahead and validate the response.
                self.__validate_response(response, error_message, response.status_code)
                response_data = response.json()
                records.extend(response_data.get("records", list()))
                next_offset = response_data.get("offset")
                if next_offset is None:
                    return records
                else:
                    return self.read(view_id=view_id, filter_by_formula=filter_by_formula, sort_by=sort_by, sort_asc=sort_asc, records=records, offset=next_offset)

        self.__raise_rate_limit_failure(error_message)

    def __build_url(self, *, view_id: str = None, filter_by_formula: str = None, sort_by: str = None, sort_asc: bool = True, offset: str = None):
        from urllib.parse import quote_plus

        url = f"{self.__url}"

        if view_id is not None:
            url += "?" if url == self.__url else "&"
            url += f"""view={view_id}"""

        if filter_by_formula is not None:
            url += "?" if url == self.__url else "&"
            url += f"""filterByFormula={quote_plus(filter_by_formula)}"""

        if sort_by is not None:
            url += "?" if url == self.__url else "&"
            url += f"""sort[0][field]={sort_by}"""

            if sort_asc:
                url += f"""&sort[0][direction]=asc"""
            else:
                url += f"""&sort[0][direction]=desc"""

        if offset is not None:
            url += "?" if url == self.__url else "&"
            url += f"""offset={offset}"""

        return url

    def update(self, record_id: str, *, fields: Dict[str, Any]):
        import requests

        error_message = "Exception updating database record"
        url = f"{self.__url}/{record_id}"

        payload = {
            "typecast": True,
            "fields": fields
        }

        for attempt in self.ATTEMPTS:
            response = requests.patch(url, headers=self.__headers, json=payload)
            if not self.__rate_limited(response, attempt):
                # We are not being rate limited, go ahead and validate the response.
                self.__validate_response(response, error_message, response.status_code)
                return response.json()

        self.__raise_rate_limit_failure(error_message)

    def insert(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        import requests

        payload = {
            "typecast": True,
            "fields": fields
        }
        error_message = "Exception inserting records to the database"

        for attempt in self.ATTEMPTS:
            response = requests.post(self.__url, headers=self.__headers, json=payload)
            if not self.__rate_limited(response, attempt):
                # We are not being rate limited, go ahead and validate the response.
                self.__validate_response(response, error_message, response.status_code)
                return response.json()

        self.__raise_rate_limit_failure(error_message)

    def delete(self, record_id: str):
        import requests

        error_message = "Exception deleting records from the database"
        url = f"{self.__url}/{record_id}"

        for attempt in self.ATTEMPTS:
            response = requests.delete(url, headers=self.__headers)
            if not self.__rate_limited(response, attempt):
                # We are not being rate limited, go ahead and validate the response.
                self.__validate_response(response, error_message, response.status_code)
                return response.json()

        self.__raise_rate_limit_failure(error_message)

    @staticmethod
    def __rate_limited(response: Response, attempt: int) -> bool:
        import time, random

        rate_limited = response.text is not None and "RATE_LIMIT_REACHED" in response.text
        if rate_limited:
            time.sleep(attempt * random.random())

        return rate_limited

    def __validate_response(self, response: Response, error_message: str, status_code: int) -> None:
        import json

        if response.status_code != 200:
            # Almost a "perfect" API except all successful calls return 200 <groan>
            message = f"""{error_message}: {status_code}"""
            self.__error_handler.on_error(message, json.dumps(response.json(), indent=4))
            raise AssertionError(message)

    def __raise_rate_limit_failure(self, what: str) -> None:
        error_message = f"""{what}; failed after {self.ATTEMPTS}."""
        self.__error_handler.on_error(error_message)
        raise AssertionError(error_message)
