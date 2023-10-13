__all__ = ["VocareumRestClient"]

from dbacademy.clients.rest.common import ApiClient


class VocareumRestClient(ApiClient):
    """
    See also the Vocareum API v2 at https://documenter.getpostman.com/view/6736336/S11Exg4b?version=latest#intro
    """

    @staticmethod
    def from_environ(*, endpoint: str = None, token: str = None, throttle_seconds: int = 0) -> "VocareumRestClient":
        import os

        return VocareumRestClient(throttle_seconds=throttle_seconds,
                                  endpoint=endpoint or os.environ.get("VOCAREUM_ENDPOINT"),
                                  token=token or os.environ.get("VOCAREUM_TOKEN"))

    @staticmethod
    def from_workspace(scope: str, *, endpoint: str = None, token: str = None, throttle_seconds: int = 0) -> "VocareumRestClient":
        from dbacademy import dbgems
        return VocareumRestClient(throttle_seconds=throttle_seconds,
                                  endpoint=endpoint or dbgems.dbutils.secrets.get(scope, "endpoint"),
                                  token=token or dbgems.dbutils.secrets.get(scope, "token"))

    def __init__(self,
                 endpoint: str = None,
                 throttle_seconds: int = 0,
                 *,
                 token: str):

        assert endpoint is not None, "The parameter \"endpoint\" must be specified"
        assert token is not None, "The parameter \"token\" must be specified"

        super().__init__(url=endpoint,
                         authorization_header=f"Token {token}",
                         throttle_seconds=throttle_seconds)

        from dbacademy.clients.vocareum.courses import CoursesClient
        self.courses = CoursesClient(self)
