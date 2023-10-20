__all__ = ["from_args", "from_workspace"]

from typing import Dict, Any
from dbacademy.clients.google.google_client_exception import GoogleClientException

__validated_google_apis = False
try:
    # noinspection PyPackageRequirements
    import google
    # noinspection PyPackageRequirements
    import googleapiclient

except ModuleNotFoundError:
    if not __validated_google_apis:
        raise Exception("The following libraries are required, runtime-dependencies not bundled with the dbacademy library: google-api-python-client google-auth-httplib2 google-auth-oauthlib")

finally:
    __validated_google_apis = True


class GoogleClient:

    def __init__(self, service_account_info: Dict[str, Any]):
        from dbacademy.clients.google.drive_api import DriveApi
        self.__drive = DriveApi(service_account_info)

    @property
    def drive(self):
        return self.__drive


def from_args(*, service_account_info: Dict[str, Any]) -> GoogleClient:
    return GoogleClient(service_account_info=service_account_info)


def from_workspace() -> GoogleClient:
    import json
    from dbacademy import dbgems

    service_account_info = dbgems.dbutils.secrets.get("gcp-prod-curriculum", "service-account-info")
    service_account_info = json.loads(service_account_info)

    return from_args(service_account_info=service_account_info)
