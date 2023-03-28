from __future__ import annotations

from typing import Any, Container, Dict, Type, TypeVar, Union

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

from pprint import pformat
import requests

__all__ = ["ApiContainer", "ApiClient", "DatabricksApiException",
           "HttpStatusCodes", "HttpMethod", "HttpReturnType", "IfNotExists", "IfExists",
           "Item", "ItemId", "ItemOrId"]

HttpStatusCodes = Union[int, Container[int]]
HttpMethod = Literal["GET", "PUT", "POST", "DELETE", "PATCH", "HEAD", "OPTIONS"]
HttpReturnType = TypeVar("HttpReturnType", bound=Union[dict, str, bytes, requests.Response, None])

IfNotExists = Literal["error", "ignore"]
IfExists = Literal["create", "error", "ignore", "overwrite", "update"]

Item = Dict
ItemId = Union[int, str]
ItemOrId = Union[int, str, Dict]


class ApiContainer(object):

    T = TypeVar('T')

    def __call__(self: T) -> T:
        """Returns itself.  Provided for backwards compatibility."""
        return self
    pass

    def help(self):
        for member_name in dir(self):
            
            if member_name.startswith("__") or member_name in ["T"]:
                continue
            
            member = getattr(self, member_name)

            if isinstance(member, ApiContainer):
                print(f"{member_name}")

            # if isinstance(member, deprecated):
            #     pass
            # elif callable(member):
            #    print(f"{member_name}()")

            if callable(member):
                print(f"{member_name}()")


class ApiClient(ApiContainer):

    url: str = None
    dns_verify: bool = True

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
        # from urllib3.util.retry import Retry
        from requests.adapters import HTTPAdapter

        # Precluding python warning.
        # TODO add type parameters
        self.session = None
        self.credentials = None
        self.networks = None
        self.storage = None
        self.private_access = None

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
            pass  # This is an unauthenticated clients

        if not url.endswith("/"):
            url += "/"

        # TODO @jacob: We are having production issues where Databricks is rate limiting us. As a result,
        #   we need to apply a throttle, but we don't want to be advertising that to students.
        # if throttle_seconds > 0:
        #     s = "" if throttle_seconds == 1 else "s"
        #     print_warning("WARNING", f"Requests are being throttled by {throttle_seconds} second{s} per request.")

        self.url = url
        self.token = token
        self.user = user
        self.password = password
        self.throttle_seconds = throttle_seconds
        self.read_timeout = 300   # seconds
        self.connect_timeout = 5  # seconds
        self._last_request_timestamp = 0
        self.verbose = verbose
        self.authorization_header = authorization_header
        self.retries = 24  # 24 to match the previous max

        # Reference information for this backoff/retry issues
        # https://stackoverflow.com/questions/47675138/how-to-override-backoff-max-while-working-with-requests-retry

        # backoff_factor = self.connect_timeout                            # Default is 5

        # noinspection PyUnresolvedReferences
        # BACKOFF_MAX is not a real parameter but the documented parameter advertised DEFAULT_BACKOFF_MAX doesn't actually exist.
        # connection_retries = Retry.BACKOFF_MAX / backoff_factor  # Should be 24

        # retry = Retry(connect=connection_retries,                # Retry Connect errors N times
        #               backoff_factor=backoff_factor,             # A backoff factor to apply between attempts after the second try
        #               total=connection_retries,                  # Overrides all other retry counts
        #               allowed_methods=["GET", "DELETE", "PUT"],  # Only retry for idempotent http-verbs
        #               status=connection_retries,                 # Retry for status_forcelist errors N times
        #               status_forcelist=[429])                    # list of codes to force a retry for
        #
        # retry = Retry(connect=connection_retries, backoff_factor=backoff_factor, status=connection_retries)

        # retry = Retry(connect=connection_retries,
        #               backoff_factor=backoff_factor)

        self.session = requests.Session()
        self.session.headers = {'Authorization': self.authorization_header, 'Content-Type': 'text/json'}
        
        self.http_adapter = HTTPAdapter()
        # self.http_adapter = HTTPAdapter(max_retries=retry)

        # noinspection HttpUrlsUsage
        self.session.mount('http://', self.http_adapter)
        self.session.mount('https://', self.http_adapter)

    def api(self, _http_method: HttpMethod, _endpoint_path: str, _data: dict = None, *,
            _expected: HttpStatusCodes = None, _result_type: Type[HttpReturnType] = dict,
            _base_url: str = None, **data: Any) -> HttpReturnType:
        """
        Invoke the Databricks REST API.

        Args:
            _http_method: 'GET', 'PUT', 'POST', 'DELETE', 'PATCH', 'HEAD', or 'OPTIONS'
            _endpoint_path: The path to append to the URL for the API endpoint, excluding the leading '/'.
                For example: path="2.0/secrets/put"
            _data: Payload to attach to the HTTP request.  GET requests encode as params, all others as json.
            _expected: HTTP response status codes to treat as expected rather than as an error.
            _result_type: Determines what type of result is returned.  It may be any of the following:
               str: Return the body as a text str.
               dict: Parse the body as json and return a dict.
               bytes: Return the body as binary data.
               requests.Response: Return the HTTP response object.
               None: Return None.
            _base_url: Overrides self.url, allowing alternative URL paths.
            **data: Any kwargs are appended to the _data payload.  Values here take priority over values
               specified in _data.

        Returns:
            The return value varies depending on the requested _return_type.  See above.

        Raises:
            requests.HTTPError: If the API returns an error and on_error='raise'.
        """
        import json, time, math
        from urllib.parse import urljoin

        if _data is None:
            _data = {}

        if data:
            _data = _data.copy()
            _data.update(data)

        _base_url: str = urljoin(self.url, _base_url)

        if self.dns_verify:
            self._verify_hostname(_base_url)

        self._throttle_calls()

        if _endpoint_path.startswith(_base_url):
            _endpoint_path = _endpoint_path[len(_base_url):]

        elif _endpoint_path.startswith("http"):
            raise ValueError(f"endpoint_path must be relative url, not {_endpoint_path !r}.")
        
        url = _base_url + _endpoint_path.lstrip("/")
        timeout = (self.connect_timeout, self.read_timeout)
        connection_errors = 0

        verbose = False  # Enabling debug prints
        response = None  # Precluding warning
        attempts = 0     # Counter for debugging

        for attempt in range(self.retries+1):
            try:
                if _http_method in ('GET', 'HEAD', 'OPTIONS'):
                    params = {k: str(v).lower() if isinstance(v, bool) else v for k, v in _data.items()}
                    response = self.session.request(_http_method, url, params=params, timeout=timeout)
                else:
                    response = self.session.request(_http_method, url, data=json.dumps(_data), timeout=timeout)

                if response.status_code == 500:
                    if "REQUEST_LIMIT_EXCEEDED" not in response.text:
                        attempts = attempt
                        break  # Don't retry, this is a hard fail, not rate-limited
                elif response.status_code not in [429]:
                    attempts = attempt
                    break  # Don't retry, either we passed or it's a hard fail.

            except requests.exceptions.ConnectionError as e:
                connection_errors += 1
                if connection_errors >= 2:
                    raise e

            # Attempt 1=1s, 2=1s, 3=5s, 4=16s, 5=13s, etc...
            duration = math.ceil(attempt * attempt / 2)
            if verbose:
                print(f"Retrying after {duration}s, attempt {attempt+1} of {self.retries+1}: {_http_method} {url}")
            time.sleep(duration)

        if response is None:  # "None" should never happen
            raise Exception("Unexpected processing error; the final response was None")
        else:  # Always validate the final response
            self._raise_for_status(response, _expected)

        if attempts > 0 and verbose:
            print(f"Success after {attempts} reties")

        # TODO: Should we really return None on errors?  Kept for now for backwards compatibility.
        if not (200 <= response.status_code < 300):
            return None
        if _result_type == requests.Response:
            return response
        elif _result_type == str:
            return response.text
        elif _result_type == bytes:
            return response.content
        elif _result_type is None:
            return None
        elif _result_type == dict:
            try:
                return response.json()
            except ValueError:
                return {
                    "_status": response.status_code,
                    "_response": response.text
                }
        # TODO @doug.bateman: missing else clause

    @staticmethod
    def _verify_hostname(url: str):
        """Verify the host for the url-endpoint exists.  Throws socket.gaierror if it does not."""
        from urllib.parse import urlparse
        from socket import gethostbyname, gaierror
        from requests.exceptions import ConnectionError

        url = urlparse(url)
        try:
            gethostbyname(url.hostname)
        except gaierror as e:
            raise ConnectionError(f"""DNS lookup for hostname failed for "{url.hostname}".""") from e

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
    def _raise_for_status(response: requests.Response, expected: Union[int, Container[int]] = None) -> None:
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
        e = requests.HTTPError(http_error_msg, response=response)
        if 400 <= response.status_code < 500:
            e = DatabricksApiException(http_exception=e)
        raise e


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
