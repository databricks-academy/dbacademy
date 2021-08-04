# Databricks notebook source
import json
import requests
from dbacademy.dbrest import DBAcademyRestClient


class ReposClient:

    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client
        self.token = token
        self.endpoint = endpoint

    def update(self, repo_id, branch) -> dict:
        return self.client.execute_patch_json(f"{self.endpoint}/api/2.0/repos/{repo_id}", {"branch": branch})
