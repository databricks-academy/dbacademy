import unittest
from typing import Dict

from dbacademy.cloudlabs import CloudlabsApi


class TestCloudlabsApi(unittest.TestCase):

    @property
    def curl_login(self) -> CloudlabsApi:
        import os
        return os.environ["CLOUDLABS_CURL"]

    def testGetLabs(self):
        cloudlabs = CloudlabsApi.curl_auth(self.curl_login)
        tenant = cloudlabs.tenants["Databricks – User Success"]
        labs = tenant.labs.list()
        self.assertIsInstance(labs, list)
        from pprint import pprint
        pprint(labs[0])

    def testGetLabById(self):
        cloudlabs = CloudlabsApi.curl_auth(self.curl_login)
        tenant = cloudlabs.tenants["Databricks – User Success"]
        labs = tenant.labs.get_by_id(31559)
        self.assertIsInstance(labs, dict)

    def testGetLabByBitly(self):
        cloudlabs = CloudlabsApi.curl_auth(self.curl_login)
        tenant = cloudlabs.tenants["Databricks – User Success"]
        labs = tenant.labs.get_by_bitly("https://bit.ly/3AtzWow")
        self.assertIsInstance(labs, dict)

    def testGetLabByTitle(self):
        cloudlabs = CloudlabsApi.curl_auth(self.curl_login)
        tenant = cloudlabs.tenants["Databricks – User Success"]
        labs = tenant.labs.get_by_title("SQL Analytics Lab on Azure | 2023-05-24 | United States")
        self.assertIsInstance(labs, dict)
