__all__ = ["from_args", "from_environ", "from_workspace"]

from dbacademy.clients.rest.common import ApiClient

DEFAULT_SCOPE = "VOCAREUM"


class VocareumRestClient(ApiClient):
    """
    See also the Vocareum API v2 at https://documenter.getpostman.com/view/6736336/S11Exg4b?version=latest#intro
    """

    def __init__(self, *,
                 endpoint: str,
                 throttle_seconds: int,
                 token: str):

        assert endpoint is not None, "The parameter \"endpoint\" must be specified"
        assert token is not None, "The parameter \"token\" must be specified"

        super().__init__(endpoint=endpoint,
                         authorization_header=f"Token {token}",
                         throttle_seconds=throttle_seconds)

        from dbacademy.clients.vocareum.courses_api import CoursesApi
        self.courses = CoursesApi(self)


def from_args(*,
              endpoint: str = None,
              throttle_seconds: int = 0,
              token: str) -> VocareumRestClient:

    return VocareumRestClient(throttle_seconds=throttle_seconds,
                              endpoint=endpoint,
                              token=token)


def from_environ(*,
                 endpoint: str = None,
                 # Common parameters
                 scope: str = DEFAULT_SCOPE,
                 token: str = None,
                 throttle_seconds: int = 0) -> VocareumRestClient:
    import os

    return VocareumRestClient(throttle_seconds=throttle_seconds,
                              endpoint=endpoint or os.environ.get(f"{scope}_ENDPOINT") or os.environ.get("ENDPOINT"),
                              token=token or os.environ.get(f"{scope}_TOKEN") or os.environ.get("TOKEN"))


def from_workspace(*,
                   endpoint: str = None,
                   # Common parameters
                   scope: str = DEFAULT_SCOPE,
                   token: str = None,
                   throttle_seconds: int = 0) -> VocareumRestClient:

    from dbacademy import dbgems

    return VocareumRestClient(throttle_seconds=throttle_seconds,
                              endpoint=endpoint or dbgems.dbutils.secrets.get(scope, "endpoint"),
                              token=token or dbgems.dbutils.secrets.get(scope, "token"))
