from typing import get_args, Literal, Dict, Any
from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer

DISPOSITION_TYPE = Literal["INLINE", "EXTERNAL_LINKS"]
FORMAT_TYPE = Literal["JSON_ARRAY", "ARROW_STREAM"]
WAIT_TIMEOUT_TYPE = Literal["CONTINUE", "CANCEL"]


class StatementsClient(ApiContainer):

    def __init__(self, client: DBAcademyRestClient):
        self.client = client
        self.base_url = f"{self.client.endpoint}/api/2.0/sql/statements"

    def get_statement(self, statement_id: str) -> Dict[str, Any]:
        return self.client.api("GET", f"{self.base_url}/{statement_id}")

    def get_chunk_index(self, statement_id: str, chunk_index: int) -> Dict[str, Any]:
        return self.client.api("POST", f"{self.base_url}/{statement_id}/result/chunks/{chunk_index}")

    def cancel_statement(self, statement_id: str) -> Dict[str, Any]:
        return self.client.api("POST", f"{self.base_url}/{statement_id}/cancel")

    def execute(self, *, warehouse_id: str, catalog: str, schema: str, statement: str, byte_limit: int = -1, disposition: DISPOSITION_TYPE = "INLINE", results_format: FORMAT_TYPE = "JSON_ARRAY", on_wait_timeout: WAIT_TIMEOUT_TYPE = "CANCEL", wait_timeout: int = 50):
        from dbacademy.common import validate_type

        validate_type(catalog, "catalog", str)
        validate_type(schema, "schema", str)
        validate_type(statement, "statement", str)
        validate_type(warehouse_id, "warehouse_id", str)

        validate_type(byte_limit, "byte_limit", int)
        validate_type(wait_timeout, "wait_timeout", int)

        validate_type(disposition, "disposition", str)
        validate_type(results_format, "results_format", str)
        validate_type(on_wait_timeout, "on_wait_timeout", str)

        assert wait_timeout == 0 or (5 <= wait_timeout <= 50), f"Expected wait_timeout to be 0 or between 5 & 50 inclusive"
        assert disposition in get_args(DISPOSITION_TYPE)
        assert results_format in get_args(FORMAT_TYPE)
        assert on_wait_timeout in get_args(WAIT_TIMEOUT_TYPE)

        params = {
            "warehouse_id": warehouse_id,
            "catalog": catalog,
            "schema": schema,
            "statement": statement,
            "disposition": disposition,
            "results_format": results_format,
            "on_wait_timeout": on_wait_timeout,
            # "wait_timeout": wait_timeout,
        }
        if byte_limit >= 0:
            params["byte_limit"] = byte_limit

        return self.client.api("POST", self.base_url, _data=params)
