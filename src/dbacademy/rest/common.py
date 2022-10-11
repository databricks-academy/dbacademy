from __future__ import annotations
from typing import Container, Dict, Literal, TypeVar, Union

from deprecated.classic import deprecated
from pprint import pformat
from requests import HTTPError, Response

__all__ = ["CachedStaticProperty", "ApiContainer", "ApiClient", "DatabricksApiException",
           "HttpErrorCodes", "IfNotExists", "IfExists", "Item", "ItemId", "ItemOrId"]

HttpErrorCodes = Union[int, Container[int]]
IfNotExists = Literal["error", "ignore"]
IfExists = Literal["create", "error", "ignore", "overwrite", "update"]
Item = Dict
ItemId = Union[int, str]
ItemOrId = Union[int, str, Dict]


class CachedStaticProperty:
    """Works like @property and @staticmethod combined"""

    def __init__(self, func):
        self.func = func

    def __get__(self, inst, owner):
        result = self.func()
        setattr(owner, self.func.__name__, result)
        return result


class ApiContainer(object):
    T = TypeVar('T')

    def __call__(self: T) -> T:
        """Returns itself.  Provided for backwards compatibility."""
        return self
    pass

    def help(self):
        for member_name in dir(self):
            if member_name.startswith("__"):
                continue
            member = getattr(self, member_name)
            if isinstance(ApiContainer, member):
                print(f"{member_name}")
            if isinstance(deprecated, member):
                pass
            elif callable(member):
                print(f"{member_name}()")

    def _register_api(self, client, **modules: str) -> None:
        """
        Convenience method to add APIs to an API Container

        >>> self._register_api("budgets", "dbacademy.dougrest.accounts.budgets:Budgets")
        >>> self.budgets
        """
        import importlib
        for name, spec in modules.items():
            module_name, class_name = spec.split(":")
            cls = getattr(importlib.import_module(module_name), class_name)
            self.__dict__[name] = cls(client)


class ApiClient(ApiContainer):

    url: str = None

    def __init__(self,
                 url: str,
                 *,
                 token: str = None,
                 user: str = None,
                 password: str = None,
                 authorization_header: str = None,
                 client: ApiClient = None,
                 throttle_seconds: int = 0,
                 verbose: bool = False):
        """
        Create a Databricks REST API client.

        Args:
            url: The common base URL to the API endpoints.  e.g. https://workspace.cloud.databricks.com/API/
            token: The API authentication token.  Defaults to None.
            user: The authentication username.  Defaults to None.
            password: The authentication password.  Defaults to None.
            authorization_header: The header to use for authentication.
                By default, it's generated from the token or password.
            client: A parent ApiClient from which to clone settings.
            throttle_seconds: Number of seconds to sleep between requests.
        """
        super().__init__()
        import requests
        from urllib3.util.retry import Retry
        from requests.adapters import HTTPAdapter

        # if verbose: print("ApiClient.__init__, url: " + url)
        # if verbose: print("ApiClient.__init__, client: " + str(client))

        if client and "://" not in url:
            url = client.url.lstrip("/") + "/" + url.rstrip("/")

        if authorization_header:
            pass
        elif token is not None:
            authorization_header = 'Bearer ' + token
        elif user is not None and password is not None:
            import base64
            encoded_auth = (user + ":" + password).encode()
            authorization_header = "Basic " + base64.standard_b64encode(encoded_auth).decode()
        elif client is not None:
            authorization_header = client.session.headers["Authorization"]
        else:
            raise ValueError("Must specify one of token, password, or authorization_header")

        if not url.endswith("/"):
            url += "/"

        if throttle_seconds > 0:
            s = "" if throttle_seconds == 1 else "s"
            print(f"** WARNING ** Requests are being throttled by {throttle_seconds} second{s} per request.")

        self.url = url
        self.user = user
        self.throttle_seconds = throttle_seconds
        self.read_timeout = 300   # seconds
        self.connect_timeout = 5  # seconds
        self._last_request_timestamp = 0
        self.verbose = verbose

        backoff_factor = self.connect_timeout

        # retry = Retry(connect=0, backoff_factor=backoff_factor)
        retry = Retry(connect=Retry.BACKOFF_MAX / backoff_factor, backoff_factor=backoff_factor)

        self.session = requests.Session()
        self.session.headers = {'Authorization': authorization_header, 'Content-Type': 'text/json'}
        # noinspection HttpUrlsUsage
        self.session.mount('http://', HTTPAdapter(max_retries=retry))
        self.session.mount('https://', HTTPAdapter(max_retries=retry))

    def api_simple(self, http_method: str, endpoint_path: str, *,
                   expected: HttpErrorCodes = None, **data) -> Union[str, Dict]:
        """
        Invoke the Databricks REST API.

        Args:
            http_method: 'GET', 'PUT', 'POST', or 'DELETE'
            endpoint_path: The path to append to the URL for the API endpoint, excluding the leading '/'.
                For example: path="2.0/secrets/put"
            expected: HTTP error codes to treat as expected rather than as an error.
            **data: Payload to attach to the HTTP request.  GET requests encode as params, all others as json.

        Returns:
            The return value of the API call as parsed JSON.  If the result is invalid JSON then the
            result will be returned as plain text.

        Raises:
            DatabricksApiException: If the API returns a 4xx error an error and on_error='raise'.
            requests.HTTPError: If the API returns any other error and on_error='raise'.
        """
        return self.api(http_method, endpoint_path, data, expected=expected)

    def api(self, http_method: str, endpoint_path: str, data=None, *,
            expected: HttpErrorCodes = None) -> Union[None, str, Dict]:
        """
        Invoke the Databricks REST API.

        Args:
            http_method: 'GET', 'PUT', 'POST', or 'DELETE'
            endpoint_path: The path to append to the URL for the API endpoint, excluding the leading '/'.
                For example: path="2.0/secrets/put"
            data: Payload to attach to the HTTP request.  GET requests encode as params, all others as json.
            expected: HTTP error codes to treat as expected rather than as an error.

        Returns:
            The return value of the API call as parsed JSON.  If the result is invalid JSON then the
            result will be returned as plain text.

        Raises:
            requests.HTTPError: If the API returns an error and on_error='raise'.
        """
        response = self.api_raw(http_method, endpoint_path, data, expected=expected)
        if not (200 <= response.status_code < 300):
            return None
        try:
            return response.json()
        except ValueError:
            return response.text

    def api_raw(self, http_method: str, endpoint_path: str, data=None, *,
                expected: HttpErrorCodes = None) -> Response:
        """
        Invoke the Databricks REST API.

        Args:
            http_method: 'GET', 'PUT', 'POST', or 'DELETE'
            endpoint_path: The path to append to the URL for the API endpoint, excluding the leading '/'.
                For example: path="2.0/secrets/put"
            data: Payload to attach to the HTTP request.  GET requests encode as params, all others as json.
            expected: HTTP error codes to treat as expected rather than as an error.

        Returns:
            The return value of the API call as parsed JSON.  If the result is invalid JSON then the
            result will be returned as plain text.

        Raises:
            requests.HTTPError: If the API returns an error and on_error='raise'.
        """
        import json
        if data is None:
            data = {}
        self._throttle_calls()
        if endpoint_path.startswith(self.url):
            endpoint_path = endpoint_path[len(self.url):]
        elif endpoint_path.startswith("http"):
            raise ValueError(f"endpoint_path must be relative url, not {endpoint_path!r}.")
        url = self.url + endpoint_path.lstrip("/")
        timeout = (self.connect_timeout, self.read_timeout)
        response: Response
        if http_method == 'GET':
            params = {k: str(v).lower() if isinstance(v, bool) else v for k, v in data.items()}
            response = self.session.request(http_method, url, params=params, timeout=timeout)
        else:
            # if self.verbose: print(json.dumps(data, indent=4))
            response = self.session.request(http_method, url, data=json.dumps(data), timeout=timeout)
        self._raise_for_status(response, expected)
        return response

    def _throttle_calls(self):
        if self.throttle_seconds <= 0:
            return
        import time
        now = time.time()
        elapsed = now - self._last_request_timestamp
        sleep_seconds = self.throttle_seconds - elapsed
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
        self._last_request_timestamp = time.time()

    @staticmethod
    def _raise_for_status(response: Response, expected: Union[int, Container[int]] = None) -> None:
        """
        If response.status_code is `2xx` or in `expected`, do nothing.
        Raises :class:`DatabricksApiException` for 4xx Client Error, :class:`HTTPError`, for all other status codes.
        """
        if 200 <= response.status_code < 300:
            return

        if expected is None:
            expected = ()
        elif type(expected) == str:
            expected = (int(expected),)
        elif type(expected) == int:
            expected = (expected,)
        elif not isinstance(expected, Container):
            raise ValueError(
                f"The parameter was expected to be of type str, int, tuple, list or set, found {type(expected)}")
        if response.status_code in expected:
            return

        if isinstance(response.reason, bytes):
            try:
                reason = response.reason.decode('utf-8')
            except UnicodeDecodeError:
                reason = response.reason.decode('iso-8859-1')
        else:
            reason = response.reason

        if 100 <= response.status_code < 200:
            error_type = 'Informational'
        elif 300 <= response.status_code < 400:
            error_type = 'Redirection'
        elif 400 <= response.status_code < 500:
            error_type = 'Client Error'
        elif 500 <= response.status_code < 600:
            error_type = 'Server Error'
        else:
            error_type = 'Unknown Error'

        try:
            body = pformat(response.json(), indent=2)
        except ValueError:
            body = response.text

        http_error_msg = f'{response.status_code} {error_type}: {reason} for url: {response.url}'
        http_error_msg += '\n Response from server: \n {}'.format(body)
        e = HTTPError(http_error_msg, response=response)
        if 400 <= response.status_code < 500:
            e = DatabricksApiException(http_exception=e)
        raise e

    @deprecated(reason="Use ApiClient.api instead", action="ignore")
    def execute_patch_json(self, url: str, params: dict, expected=200) -> dict:
        return self.api("PATCH", url, params, expected=expected)

    @deprecated(reason="Use ApiClient.api instead", action="ignore")
    def execute_patch(self, url: str, params: dict, expected=200):
        return self.api_raw("PATCH", url, params, expected=expected)

    @deprecated(reason="Use ApiClient.api instead", action="ignore")
    def execute_post_json(self, url: str, params: dict, expected=200) -> dict:
        return self.api("POST", url, params, expected=expected)

    @deprecated(reason="Use ApiClient.api instead", action="ignore")
    def execute_post(self, url: str, params: dict, expected=200):
        return self.api_raw("POST", url, params, expected=expected)

    @deprecated(reason="Use ApiClient.api instead", action="ignore")
    def execute_put_json(self, url: str, params: dict, expected=200) -> dict:
        return self.api("PUT", url, params, expected=expected)

    @deprecated(reason="Use ApiClient.api instead", action="ignore")
    def execute_put(self, url: str, params: dict, expected=200):
        return self.api_raw("PUT", url, params, expected=expected)

    @deprecated(reason="Use ApiClient.api instead", action="ignore")
    def execute_get_json(self, url: str, expected=200) -> Union[dict, None]:
        return self.api("GET", url, expected=expected)

    @deprecated(reason="Use ApiClient.api instead", action="ignore")
    def execute_get(self, url: str, expected=200):
        return self.api_raw("GET", url, expected=expected)

    @deprecated(reason="Use ApiClient.api instead", action="ignore")
    def execute_delete_json(self, url: str, expected=(200, 404)) -> dict:
        return self.api("DELETE", url, expected=expected)

    @deprecated(reason="Use ApiClient.api instead", action="ignore")
    def execute_delete(self, url: str, expected=(200, 404)):
        return self.api_raw("DELETE", url, expected=expected)


class DatabricksApiException(Exception):
    def __init__(self, message=None, http_code=None, http_exception=None):
        import json
        if http_exception:
            self.__cause__ = http_exception
            self.cause = http_exception
            try:
                self.body = json.loads(http_exception.response.text)
                self.message = self.body.get("message", self.body.get("error", http_exception.response.text))
                self.error_code = self.body.get("error_code", -1)
            except ValueError:
                self.body = http_exception.response.text
                self.message = self.body
                self.error_code = -1
            self.method = http_exception.request.method
            self.endpoint = http_exception.request.path_url
            self.http_code = http_exception.response.status_code
            self.request = http_exception.request
            self.response = http_exception.response
        else:
            self.__cause__ = None
            self.cause = None
            self.body = message
            self.method = None
            self.endpoint = None
            self.http_code = http_code
            self.error_code = -1
            self.request = None
            self.response = None
        if message:
            self.message = message
        self.args = (self.method, self.endpoint, self.http_code, self.error_code, self.message)

    def __repr__(self):
        return (f"DatabricksApiException(message={self.message!r}, "
                f"http_code={self.http_code!r}, "
                f"http_exception={self.__cause__!r})")

    def __str__(self):
        return repr(self)
