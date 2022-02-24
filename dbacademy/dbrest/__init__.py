# Databricks notebook source
import configparser
import os


class DBAcademyRestClient:
  
    def __init__(self, local=False, config_file=None, profile="DEFAULT", throttle=0, endpoint=None):
        import requests
        from urllib3.util.retry import Retry
        from requests.adapters import HTTPAdapter

        self.throttle = throttle

        self.read_timeout = 300 # seconds
        self.connect_timeout = 5  # seconds

        backoff_factor = self.connect_timeout
        retry = Retry(connect=Retry.BACKOFF_MAX / backoff_factor, backoff_factor=backoff_factor)

        self.session = requests.Session()
        self.session.mount('https://', HTTPAdapter(max_retries=retry))

        if not local:
            from dbacademy import dbgems

            self.token = dbgems.get_notebooks_api_token()
            if endpoint:
                self.endpoint = endpoint
            else:
                self.endpoint = dbgems.get_notebooks_api_endpoint()
        else:
            self._get_local_credentials(config_file, profile)
        
        if self.throttle > 0:
            s = "" if self.throttle == 1 else "s"
            print(f"** WARNING ** Requests are being throttled by {self.throttle} second{s} per request.")

    def help(self):
        methods = [func for func in dir(self) if callable(getattr(self, func)) and not func.startswith("__")]
        for method in methods:
            print(f"{method}()")

    def throttle_calls(self):
        import time
        # if self.throttle > 0:
        #     s = "" if self.throttle == 1 else "s"
        #     print(f"** Throttling requests by {self.throttle} second{s} per request.")

        time.sleep(self.throttle)

    def clusters(self):
        from dbacademy.dbrest.clusters import ClustersClient
        return ClustersClient(self, self.token, self.endpoint)

    def jobs(self):
        from dbacademy.dbrest.jobs import JobsClient
        return JobsClient(self, self.token, self.endpoint)

    def permissions(self):
        from dbacademy.dbrest.permissions import PermissionsClient
        return PermissionsClient(self, self.token, self.endpoint)

    def repos(self):
        from dbacademy.dbrest.repos import ReposClient
        return ReposClient(self, self.token, self.endpoint)

    def runs(self):
        from dbacademy.dbrest.runs import RunsClient
        return RunsClient(self, self.token, self.endpoint)

    def sql(self):
        from dbacademy.dbrest.sql import SqlClient
        return SqlClient(self, self.token, self.endpoint)

    def uc(self):
        from dbacademy.dbrest.uc import UcClient
        return UcClient(self, self.token, self.endpoint)

    def workspace(self):
        from dbacademy.dbrest.workspace import WorkspaceClient

        return WorkspaceClient(self, self.token, self.endpoint)

    def _get_local_credentials(self, config_file, profile):
        if config_file is None:
            config_file = os.environ["HOME"] + "/.databrickscfg"
        config = configparser.ConfigParser()
        config.read(config_file)

        self.endpoint = config.get(profile, "host")
        self.username = config.get(profile, "username")
        self.token = config.get(profile, "token")

    def execute_patch_json(self, url: str, params: dict, expected=200) -> dict:
        return self.execute_patch(url, params, expected).json()

    def execute_patch(self, url: str, params: dict, expected=200):
        import json
        expected = self.expected_to_list(expected)

        response = self.session.patch(url, headers={"Authorization": "Bearer " + self.token}, data=json.dumps(params), timeout=(self.connect_timeout, self.read_timeout))
        assert response.status_code in expected, f"({response.status_code}): {response.text}"

        self.throttle_calls()
        return response

    def execute_post_json(self, url: str, params: dict, expected=200) -> dict:
        return self.execute_post(url, params, expected).json()

    def execute_post(self, url: str, params: dict, expected=200):
        import json
        expected = self.expected_to_list(expected)

        response = self.session.post(url, headers={"Authorization": "Bearer " + self.token}, data=json.dumps(params), timeout=(self.connect_timeout, self.read_timeout))
        assert response.status_code in expected, f"({response.status_code}): {response.text}"

        self.throttle_calls()
        return response

    def execute_put_json(self, url: str, params: dict, expected=200) -> dict:
        return self.execute_put(url, params, expected).json()

    def execute_put(self, url: str, params: dict, expected=200):
        import json
        expected = self.expected_to_list(expected)

        response = self.session.put(url, headers={"Authorization": "Bearer " + self.token}, data=json.dumps(params), timeout=(self.connect_timeout, self.read_timeout))
        assert response.status_code in expected, f"({response.status_code}): {response.text}"

        self.throttle_calls()
        return response

    def execute_get_json(self, url: str, expected=200) -> dict:
        response = self.execute_get(url, expected)

        if response.status_code == 200:
            return response.json()
        else: # For example, expected includes 404
            return None

    def execute_get(self, url: str, expected=200):
        expected = self.expected_to_list(expected)

        response = self.session.get(url, headers={"Authorization": f"Bearer {self.token}"}, timeout=(self.connect_timeout, self.read_timeout))
        assert response.status_code in expected, f"({response.status_code}): {response.text}"

        self.throttle_calls()
        return response

    def execute_delete_json(self, url: str, expected=[200,404]) -> dict:
        response = self.execute_delete(url, expected)
        return response.json()

    def execute_delete(self, url: str, expected=[200,404]):
        expected = self.expected_to_list(expected)

        response = self.session.delete(url, headers={"Authorization": f"Bearer {self.token}"}, timeout=(self.connect_timeout, self.read_timeout))
        assert response.status_code in expected, f"({response.status_code}): {response.text}"

        self.throttle_calls()
        return response

    @staticmethod
    def expected_to_list(expected) -> list:
        if type(expected) == str: expected = int(expected)
        if type(expected) == int: expected = [expected]
        assert type(expected) == list, f"The parameter was expected to be of type str, int or list, found {type(expected)}"
        return expected
