import unittest

from dbacademy.cloudlabs import CloudlabsApi


# TODO doug.bateman@databricks.com: Potential bugs
# noinspection PyTypeChecker
class TestCloudlabsApi(unittest.TestCase):

    @property
    def curl_login(self) -> CloudlabsApi:
        import os
        if "CLOUDLABS_URL" in os.environ:
            return os.environ["CLOUDLABS_CURL"]
        for path in (".curl_login", "../.curl_login", '~/.curl_login'):
            path = os.path.expanduser(path)
            if not os.path.exists(path):
                continue
            with open(path) as f:
                return f.read()
        self.skipTest(reason="Environment variable CLOUDLABS_CURL must be set, or the file .curl_login must exist")
        # raise ValueError("Environment variable CLOUDLABS_CURL must be set, or the file .curl_login must exist")

    def testGetLabs(self):
        cloudlabs = CloudlabsApi.curl_auth(self.curl_login)
        tenant = cloudlabs.tenants["Databricks – User Success"]
        result = tenant.labs.list()
        self.assertIsInstance(result, list)

    def testGetLabById(self):
        cloudlabs = CloudlabsApi.curl_auth(self.curl_login)
        tenant = cloudlabs.tenants["Databricks – User Success"]
        result = tenant.labs.get_by_id(31559)
        self.assertIsInstance(result, dict)

    def testGetLabByBitly(self):
        cloudlabs = CloudlabsApi.curl_auth(self.curl_login)
        tenant = cloudlabs.tenants["Databricks – User Success"]
        result = tenant.labs.get_by_bitly("https://bit.ly/3AtzWow")
        self.assertIsInstance(result, dict)

    def testGetLabByTitle(self):
        cloudlabs = CloudlabsApi.curl_auth(self.curl_login)
        tenant = cloudlabs.tenants["Databricks – User Success"]
        result = tenant.labs.get_by_title("SQL Analytics Lab on Azure | 2023-05-24 | United States")
        self.assertIsInstance(result, dict)

    def testGetInstructors(self):
        cloudlabs = CloudlabsApi.curl_auth(self.curl_login)
        tenant = cloudlabs.tenants["Databricks – User Success"]
        result = tenant.instructors.get_instructors_for_lab(31559, fetch_creds=True)
        self.assertIsInstance(result, list)
        if result:
            self.assertIsNotNone(result[0]["UserName"])
            self.assertIsNotNone(result[0]["Password"])

    def testGetWorkspaces(self):
        cloudlabs = CloudlabsApi.curl_auth(self.curl_login)
        tenant = cloudlabs.tenants["Databricks – User Success"]
        lab = tenant.labs.get_by_id(31559)
        workspaces = tenant.labs.workspaces(lab)
        self.assertTrue(len(workspaces) != 0)
        result = workspaces[0].workspace.list("/")
        self.assertTrue(len(result) != 0)

    def testGetWorkspacesEmpty(self):
        cloudlabs = CloudlabsApi.curl_auth(self.curl_login)
        tenant = cloudlabs.tenants["Databricks – User Success"]
        lab = tenant.labs.get_by_id(29900)
        workspaces = tenant.labs.workspaces(lab)
        self.assertTrue(len(workspaces) == 0)

    def testGetTemplates(self):
        cloudlabs = CloudlabsApi.curl_auth(self.curl_login)
        tenant = cloudlabs.tenants["Databricks – User Success"]
        result = tenant.templates.list()
        self.assertIsInstance(result, list)
        # Note: It returns at most 500 templates.  So older templates aren't being returned yet.

    def testGetTemplateByName(self):
        cloudlabs = CloudlabsApi.curl_auth(self.curl_login)
        tenant = cloudlabs.tenants["Databricks – User Success"]
        result = tenant.templates.get_by_name("Deep Learning with Databricks - AWS")
        self.assertIsInstance(result, dict)
        from pprint import pprint
        pprint(result)

    def testGetTemplateById(self):
        cloudlabs = CloudlabsApi.curl_auth(self.curl_login)
        tenant = cloudlabs.tenants["Databricks – User Success"]
        result = tenant.templates.get_by_id(5928)
        self.assertIsInstance(result, dict)
        from pprint import pprint
        pprint(result)
