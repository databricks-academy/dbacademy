# Databricks notebook source
import configparser
import os


class DBAcademyRestClient:
  
    def __init__(self, local=False, config_file=None, profile="DEFAULT", throttle=0, endpoint=None):
        self.throttle = throttle
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

    def workspace(self):
        from dbacademy.dbrest.workspace import WorkspaceClient

        return WorkspaceClient(self, self.token, self.endpoint, self.throttle)

    def jobs(self):
        from dbacademy.dbrest.jobs import JobsClient
        return JobsClient(self, self.token, self.endpoint, self.throttle)

    def repos(self):
        from dbacademy.dbrest.repos import ReposClient

        return ReposClient(self, self.token, self.endpoint)

    def runs(self):
        from dbacademy.dbrest.runs import RunsClient

        return RunsClient(self, self.token, self.endpoint)

    def _get_local_credentials(self, config_file, profile):
        if config_file is None:
            config_file = os.environ["HOME"] + "/.databrickscfg"
        config = configparser.ConfigParser()
        config.read(config_file)

        self.endpoint = config.get(profile, "host")
        self.username = config.get(profile, "username")
        self.token = config.get(profile, "token")
