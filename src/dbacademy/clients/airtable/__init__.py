__all__ = ["AirTableRestClient", "from_args", "from_environ", "from_workspace"]

from dbacademy.clients.rest.common import ApiClient
from dbacademy.clients.airtable.tables_api import TablesAPI
from dbacademy.clients import ClientErrorHandler
from dbacademy.clients.airtable.at_utils import AirTableUtils

DEFAULT_SCOPE = "AIRTABLE"


class AirTableRestClient(ApiClient):
    """"
    AirTable Rest API AirTableRestClient
    """

    def __init__(self, *,
                 base_id: str,
                 access_token: str,
                 verbose: bool,
                 throttle_seconds: int,
                 error_handler: ClientErrorHandler):

        from dbacademy.clients.airtable.at_utils import AirTableUtils
        from dbacademy.common import validate

        validate.str_value(access_token=access_token, required=True)
        validate.str_value(base_id=base_id, required=True)
        validate.any_value(ClientErrorHandler, error_handler=error_handler, required=True)

        super().__init__(endpoint=f"https://api.airtable.com/v0/{base_id}",
                         authorization_header=f"Bearer {access_token}",
                         verbose=verbose,
                         throttle_seconds=throttle_seconds,
                         error_handler=error_handler)

        self.session.headers["Content-Type"] = "application/json"

        self.__base_id = base_id
        self.__at_utils = AirTableUtils(error_handler)

    @property
    def at_utils(self) -> AirTableUtils:
        return self.__at_utils

    @property
    def base_id(self) -> str:
        return self.__base_id

    def table(self, table_id: str) -> TablesAPI:
        return TablesAPI(self, table_id, self.at_utils)


def from_args(*,
              base_id: str,
              access_token: str,
              verbose: bool = False,
              throttle_seconds: int = 0,
              error_handler: ClientErrorHandler = ClientErrorHandler()) -> AirTableRestClient:

    return AirTableRestClient(base_id=base_id,
                              access_token=access_token,
                              verbose=verbose,
                              throttle_seconds=throttle_seconds,
                              error_handler=error_handler)


def from_environ(*,
                 base_id: str,
                 access_token: str = None,
                 # Common parameters
                 scope: str = DEFAULT_SCOPE,
                 verbose: bool = False,
                 throttle_seconds: int = 0,
                 error_handler: ClientErrorHandler = ClientErrorHandler()) -> AirTableRestClient:
    import os

    return AirTableRestClient(base_id=base_id or os.environ.get(f"{scope}_BASE_ID") or os.environ.get("BASE_ID"),
                              access_token=access_token or os.environ.get(f"{scope}_TOKEN") or os.environ.get("TOKEN"),
                              verbose=verbose,
                              throttle_seconds=throttle_seconds,
                              error_handler=error_handler)


def from_workspace(*,
                   base_id: str,
                   access_token: str,
                   # Common parameters
                   scope: str = DEFAULT_SCOPE,
                   verbose: bool = False,
                   throttle_seconds: int = 0,
                   error_handler: ClientErrorHandler = ClientErrorHandler()) -> AirTableRestClient:

    from dbacademy import dbgems

    return AirTableRestClient(access_token=access_token,
                              base_id=base_id or dbgems.dbutils.secrets.get(scope, "base_id"),
                              verbose=verbose,
                              throttle_seconds=throttle_seconds,
                              error_handler=error_handler)
