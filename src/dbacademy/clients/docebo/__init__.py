__all__ = ["from_args", "from_environ", "from_workspace"]

from dbacademy.clients.rest.common import ApiClient
from dbacademy.clients import ClientErrorHandler

DEFAULT_SCOPE = "DOCEBO"


class DoceboRestClient(ApiClient):
    """Docebo REST API client."""

    def __init__(self,
                 *,
                 endpoint: str,
                 username: str,
                 password: str,
                 consumer_key: str,
                 consumer_secret: str,
                 verbose: bool,
                 throttle_seconds: int,
                 error_handler: ClientErrorHandler):

        from dbacademy.common import validate

        validate.str_value(endpoint=endpoint, required=True)
        validate.str_value(username=username, required=True)
        validate.str_value(consumer_key=consumer_key, required=True)
        validate.str_value(consumer_secret=consumer_secret, required=True)

        access_token = DoceboRestClient.authenticate(endpoint=endpoint,
                                                     consumer_key=consumer_key,
                                                     consumer_secret=consumer_secret,
                                                     username=username,
                                                     password=password)
        super().__init__(endpoint=endpoint,
                         authorization_header=f"Bearer {access_token}",
                         verbose=verbose,
                         throttle_seconds=throttle_seconds,
                         error_handler=error_handler)

        from dbacademy.clients.docebo.manage_api import ManageAPI
        self.manage = ManageAPI(self)

        from dbacademy.clients.docebo.courses_api import CoursesAPI
        self.courses = CoursesAPI(self)

        from dbacademy.clients.docebo.events_api import EventsAPI
        self.events = EventsAPI(self)

        from dbacademy.clients.docebo.sessions_api import SessionsAPI
        self.sessions = SessionsAPI(self)

    @property
    def error_handler(self) -> ClientErrorHandler:
        return self.__error_handler

    @staticmethod
    def authenticate(*, endpoint: str, consumer_key: str, consumer_secret: str, username: str, password: str) -> str:
        import requests

        # Format the Payload
        payload = {
            'grant_type': 'password',
            'client_id': consumer_key,
            'client_secret': consumer_secret,
            'username': username,
            'password': password
        }

        # Request an OAuth Token
        url = f"{endpoint}/oauth2/token"
        response = requests.post(url, data=payload, headers={"Content-Type": "application/x-www-form-urlencoded"})
        assert response.status_code == 200, f"Expected the HTTP status code 200, found {response.status_code}: {response.text}"

        return response.json().get("access_token")


def from_args(*,
              endpoint: str = None,
              username: str = None,
              password: str = None,
              consumer_key: str = None,
              consumer_secret: str = None,
              verbose=False,
              throttle_seconds: int = 0,
              error_handler: ClientErrorHandler = ClientErrorHandler()) -> DoceboRestClient:

    return DoceboRestClient(endpoint=endpoint,
                            username=username,
                            password=password,
                            consumer_key=consumer_key,
                            consumer_secret=consumer_secret,
                            verbose=verbose,
                            throttle_seconds=throttle_seconds,
                            error_handler=error_handler)


def from_environ(*,
                 endpoint: str = None,
                 username: str = None,
                 password: str = None,
                 consumer_key: str = None,
                 consumer_secret: str = None,
                 # Common parameters
                 scope: str = DEFAULT_SCOPE,
                 verbose=False,
                 throttle_seconds: int = 0,
                 error_handler: ClientErrorHandler = ClientErrorHandler()) -> DoceboRestClient:
    import os

    return DoceboRestClient(endpoint=endpoint or os.environ.get(f"{scope}_ENDPOINT") or os.environ.get("ENDPOINT"),
                            username=username or os.environ.get(f"{scope}_USERNAME") or os.environ.get("USERNAME"),
                            password=password or os.environ.get(f"{scope}_PASSWORD") or os.environ.get("PASSWORD"),
                            consumer_key=consumer_key or os.environ.get(f"{scope}_CONSUMER_KEY") or os.environ.get("CONSUMER_KEY"),
                            consumer_secret=consumer_secret or os.environ.get(f"{scope}_CONSUMER_SECRET") or os.environ.get("CONSUMER_SECRET"),
                            verbose=verbose,
                            throttle_seconds=throttle_seconds,
                            error_handler=error_handler)


def from_workspace(*,
                   endpoint: str = None,
                   username: str = None,
                   password: str = None,
                   consumer_key: str = None,
                   consumer_secret: str = None,
                   # Common parameters
                   scope: str = DEFAULT_SCOPE,
                   verbose=False,
                   throttle_seconds: int = 0,
                   error_handler: ClientErrorHandler = ClientErrorHandler()) -> DoceboRestClient:

    from dbacademy import dbgems

    return DoceboRestClient(endpoint=endpoint or dbgems.dbutils.secrets.get(scope, "endpoint"),
                            username=username or dbgems.dbutils.secrets.get(scope, "username"),
                            password=password or dbgems.dbutils.secrets.get(scope, "password"),
                            consumer_key=consumer_key or dbgems.dbutils.secrets.get(scope, "consumer_key"),
                            consumer_secret=consumer_secret or dbgems.dbutils.secrets.get(scope, "consumer_secret"),
                            verbose=verbose,
                            throttle_seconds=throttle_seconds,
                            error_handler=error_handler)
