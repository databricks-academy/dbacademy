# Databricks notebook source
from dbacademy.dbrest import DBAcademyRestClient


class PipelinesClient:
    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client
        self.token = token
        self.endpoint = endpoint

        self.base_uri = f"{self.endpoint}/api/2.0/pipelines"

    def get_by_id(self, pipelines):
        return self.client.execute_get_json(base_uri)
