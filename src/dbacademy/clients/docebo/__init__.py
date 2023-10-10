__all__ = ["DoceboRestClient"]

from dbacademy.clients.rest.common import ApiClient


class DoceboRestClient(ApiClient):
    """Docebo REST API client."""

    @staticmethod
    def from_environ() -> "DoceboRestClient":
        import os
        consumer_secret = os.environ.get("DOCEBO_CONSUMER_SECRET")
        consumer_key = os.environ.get("DOCEBO_CONSUMER_KEY")
        username = os.environ.get("DOCEBO_USERNAME")
        password = os.environ.get("DOCEBO_PASSWORD")
        endpoint = os.environ.get("DOCEBO_ENDPOINT")

        return DoceboRestClient(endpoint=endpoint,
                                username=username,
                                password=password,
                                consumer_key=consumer_key,
                                consumer_secret=consumer_secret)

    @staticmethod
    def from_workspace(scope: str) -> "DoceboRestClient":
        from dbacademy import dbgems

        return DoceboRestClient(endpoint=dbgems.dbutils.secrets.get(scope, "endpoint"),
                                username=dbgems.dbutils.secrets.get(scope, "username"),
                                password=dbgems.dbutils.secrets.get(scope, "password"),
                                consumer_key=dbgems.dbutils.secrets.get(scope, "consumer_key"),
                                consumer_secret=dbgems.dbutils.secrets.get(scope, "consumer_secret"))

    def __init__(self,
                 endpoint: str = None,
                 throttle_seconds: int = 0,
                 *,
                 username: str,
                 password: str,
                 consumer_key: str,
                 consumer_secret: str):

        assert endpoint is not None, "The parameter \"endpoint\" must be specified"
        assert username is not None, "The parameter \"username\" must be specified"
        assert consumer_key is not None, "The parameter \"consumer_key\" must be specified"
        assert consumer_secret is not None, "The parameter \"consumer_secret\" must be specified"

        access_token = DoceboRestClient.authenticate(endpoint=endpoint,
                                                     consumer_key=consumer_key,
                                                     consumer_secret=consumer_secret,
                                                     username=username,
                                                     password=password)
        super().__init__(url=endpoint,
                         authorization_header=F"Bearer {access_token}",
                         throttle_seconds=throttle_seconds)

        from dbacademy.clients.docebo.manage import ManageClient
        self.manage = ManageClient(self)

        from dbacademy.clients.docebo.courses import CoursesClient
        self.courses = CoursesClient(self)

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
