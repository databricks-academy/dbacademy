from typing import Literal, Dict, Any, get_args
from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.clients.rest.common import ApiContainer

DISPOSITION_TYPE = Literal["INLINE", "EXTERNAL_LINKS"]
FORMAT_TYPE = Literal["JSON_ARRAY", "ARROW_STREAM"]
WAIT_TIMEOUT_TYPE = Literal["CONTINUE", "CANCEL"]
WAIT_TIMEOUT_SECONDS = Literal[
    "0s", "5s", "6s", "7s", "8s", "9s",
    "10s", "11s", "12s", "13s", "14s", "15s", "16s", "17s", "18s", "19s",
    "20s", "21s", "22s", "23s", "24s", "25s", "26s", "27s", "28s", "29s",
    "30s", "31s", "32s", "33s", "34s", "35s", "36s", "37s", "38s", "39s",
    "40s", "41s", "42s", "43s", "44s", "45s", "46s", "47s", "48s", "49s",
    "50s"]


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

    def execute(self, *, warehouse_id: str, catalog: str, schema: str, statement: str, byte_limit: int = -1, disposition: DISPOSITION_TYPE = "INLINE", results_format: FORMAT_TYPE = "JSON_ARRAY", on_wait_timeout: WAIT_TIMEOUT_TYPE = "CANCEL", wait_timeout: WAIT_TIMEOUT_SECONDS = "50s"):
        from dbacademy.common import validate_type

        validate_type(catalog, "catalog", str)
        validate_type(schema, "schema", str)
        validate_type(statement, "statement", str)
        validate_type(warehouse_id, "warehouse_id", str)

        validate_type(byte_limit, "byte_limit", int)
        validate_type(wait_timeout, "wait_timeout", str)

        validate_type(disposition, "disposition", str)
        validate_type(results_format, "results_format", str)
        validate_type(on_wait_timeout, "on_wait_timeout", str)

        assert wait_timeout in get_args(WAIT_TIMEOUT_SECONDS), f"Expected wait_timeout to be 0s or between 5s & 50s inclusive"
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
            "wait_timeout": wait_timeout,
        }
        if byte_limit >= 0:
            params["byte_limit"] = byte_limit

        return self.client.api("POST", self.base_url, _data=params)
