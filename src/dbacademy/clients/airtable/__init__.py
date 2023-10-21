__all__ = ["from_args", "from_environment", "from_workspace", "TableConfig"]

from typing import Optional
from dbacademy.clients.rest.common import ApiClient
from dbacademy.clients.airtable.tables_api import TablesAPI
from dbacademy.clients import ClientErrorHandler
from dbacademy.clients.airtable.at_utils import AirTableUtils

DEFAULT_SCOPE = "AIRTABLE"


class TableConfig:
    def __init__(self, base_id: Optional[str], table_id: Optional[str]):
        self.__base_id = base_id
        self.__table_id = table_id

    @property
    def base_id(self) -> str:
        return self.__base_id

    @property
    def table_id(self) -> str:
        return self.__table_id


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
              access_token: str,
              base_id: str = None,
              # Common parameters
              verbose: bool = False,
              throttle_seconds: int = 0,
              error_handler: ClientErrorHandler = ClientErrorHandler()) -> AirTableRestClient:

    return AirTableRestClient(access_token=access_token,
                              base_id=base_id,
                              verbose=verbose,
                              throttle_seconds=throttle_seconds,
                              error_handler=error_handler)


def from_environment(*,
                     access_token: str = None,
                     base_id: str = None,
                     # Common parameters
                     scope: str = DEFAULT_SCOPE,
                     verbose: bool = False,
                     throttle_seconds: int = 0,
                     error_handler: ClientErrorHandler = ClientErrorHandler()) -> AirTableRestClient:
    import os

    return from_args(access_token=access_token or os.environ.get(f"{scope}_TOKEN") or os.environ.get("TOKEN"),
                     base_id=base_id,
                     verbose=verbose,
                     throttle_seconds=throttle_seconds,
                     error_handler=error_handler)


def from_workspace(*,
                   access_token: str = None,
                   base_id: str = None,
                   # Common parameters
                   scope: str = DEFAULT_SCOPE,
                   verbose: bool = False,
                   throttle_seconds: int = 0,
                   error_handler: ClientErrorHandler = ClientErrorHandler()) -> AirTableRestClient:

    from dbacademy import dbgems

    return from_args(access_token=access_token or dbgems.dbutils.secrets.get(scope, "token"),
                     base_id=base_id,
                     verbose=verbose,
                     throttle_seconds=throttle_seconds,
                     error_handler=error_handler)


def from_table_config(table_config: TableConfig,
                      *,
                      access_token: str = None,
                      # Common parameters
                      scope: str = DEFAULT_SCOPE,
                      verbose: bool = False,
                      throttle_seconds: int = 0,
                      error_handler: ClientErrorHandler = ClientErrorHandler()) -> TablesAPI:

    import os

    if access_token is None:
        # Load from the notebook before the environment
        from dbacademy import dbgems
        access_token = dbgems.dbutils.secrets.get(scope, "token")

    if access_token is None:
        # Load from environment last
        access_token = os.environ.get(f"{scope}_TOKEN") or os.environ.get("TOKEN")

    client = from_args(access_token=access_token,
                       base_id=table_config.base_id,
                       verbose=verbose,
                       throttle_seconds=throttle_seconds,
                       error_handler=error_handler)

    return client.table(table_config.table_id)
