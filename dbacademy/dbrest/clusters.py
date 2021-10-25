# Databricks notebook source
from dbacademy.dbrest import DBAcademyRestClient


class ClustersClient:
    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client
        self.token = token
        self.endpoint = endpoint

    def get(self, cluster_id):
        return self.client.execute_get_json(f"{self.endpoint}/api/2.0/clusters/get?cluster_id={cluster_id}")
