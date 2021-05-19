# Databricks notebook source
import json
import requests
from dbacademy.dbrest import DBAcademyRestClient


class ReposClient:

    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client
        self.token = token
        self.endpoint = endpoint

    def update(self, repo_id, branch):
        response = requests.patch(
            f"{self.endpoint}/api/2.0/repos/{repo_id}",
            headers={"Authorization": "Bearer " + self.token},
            data=json.dumps({
                "branch": branch
            })
        )
        assert response.status_code == 200, f"({response.status_code}): {response.text}"
        return response.json()
