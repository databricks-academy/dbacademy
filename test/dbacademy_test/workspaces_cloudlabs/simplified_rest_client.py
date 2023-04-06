from typing import Literal, Union, Container, Type, TypeVar, Any
import base64, requests
HttpMethod = Literal["GET", "PUT", "POST", "DELETE", "PATCH", "HEAD", "OPTIONS"]
HttpStatusCodes = Union[int, Container[int]]
HttpReturnType = TypeVar("HttpReturnType", bound=Union[dict, str, bytes, requests.Response, None])


class DatabricksApiException(Exception):
    """
    This class is used by Databricks Edu's rest clients to parse out common errors from the Databricks REST endpoints.
    """
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


class SimpleRestClient:
    """
    Simplified version of Databricks Edu's rest client, included here only for demonstration purposes.
    """

    def __init__(self, *, username, password, url):
        from requests.adapters import HTTPAdapter

        self.url = url
        self.username = username
        self.password = password

        encoded_auth = f"{username}:{password}".encode()
        self.authorization_header = "Basic " + base64.standard_b64encode(encoded_auth).decode()

        self.session = requests.Session()
        self.session.headers = {'Authorization': self.authorization_header, 'Content-Type': 'text/json'}

        self.http_adapter = HTTPAdapter()

        # noinspection HttpUrlsUsage
        self.session.mount('http://', self.http_adapter)
        self.session.mount('https://', self.http_adapter)

        self.read_timeout = 300   # seconds
        self.connect_timeout = 5  # seconds
        self.retries = 20         # attempts

    def call(self,
             _http_method: HttpMethod,
             _endpoint_path: str,
             _data: dict = None,
             *,
             _expected: HttpStatusCodes = None,
             _result_type: Type[HttpReturnType] = dict,
             _base_url: str = None, **data: Any) -> HttpReturnType:

        import json, time, math
        from urllib.parse import urljoin

        if _data is None:
            _data = {}

        if data:
            _data = _data.copy()
            _data.update(data)

        _base_url: str = urljoin(self.url, _base_url)

        self._verify_hostname(_base_url)

        if _endpoint_path.startswith(_base_url):
            _endpoint_path = _endpoint_path[len(_base_url):]
        elif _endpoint_path.startswith("http"):
            raise ValueError(f"endpoint_path must be relative url, not {_endpoint_path !r}.")

        url = f"""{_base_url.rstrip("/")}/{_endpoint_path.lstrip("/")}"""
        timeout = (self.connect_timeout, self.read_timeout)
        connection_errors = 0

        response = None  # Precluding warning

        for attempt in range(self.retries+1):
            try:
                if _http_method in ('GET', 'HEAD', 'OPTIONS'):
                    params = {k: str(v).lower() if isinstance(v, bool) else v for k, v in _data.items()}
                    response = self.session.request(_http_method, url, params=params, timeout=timeout)
                else:
                    json_data = json.dumps(_data)
                    response = self.session.request(_http_method, url, data=json_data, timeout=timeout)

                if response.status_code == 500:
                    if "REQUEST_LIMIT_EXCEEDED" not in response.text:
                        break  # Don't retry, this is a hard fail, not rate-limited
                elif response.status_code not in [429]:
                    break  # Don't retry, either we passed or it's a hard fail.

            except requests.exceptions.ConnectionError as e:
                connection_errors += 1
                if connection_errors >= 2:
                    raise e

            # Attempt 1=1s, 2=1s, 3=5s, 4=16s, 5=13s, etc...
            duration = math.ceil(attempt * attempt / 2)
            time.sleep(duration)

        if response is None:  # "None" should never happen
            raise Exception("Unexpected processing error; the final response was None")
        else:  # Always validate the final response
            self._raise_for_status(response, _expected)

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
    def _verify_hostname(url: str) -> None:
        """Verify the host for the url-endpoint exists.  Throws socket.gaierror if it does not."""
        from urllib.parse import urlparse
        from socket import gethostbyname, gaierror
        from requests.exceptions import ConnectionError

        url = urlparse(url)
        try:
            gethostbyname(url.hostname)
        except gaierror as e:
            raise ConnectionError(f"""DNS lookup for hostname failed for "{url.hostname}".""") from e

    @staticmethod
    def _raise_for_status(response: requests.Response, expected: Union[int, Container[int]] = None) -> None:
        """
        If response.status_code is `2xx` or in `expected`, do nothing.
        Raises :class:`DatabricksApiException` for 4xx Client Error, :class:`HTTPError`, for all other status codes.
        """
        from pprint import pformat

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
