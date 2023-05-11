import unittest

from dbacademy.cloudlabs import CloudlabsApi


# TODO doug.bateman@databricks.com: Potential bugs
# noinspection PyTypeChecker
class TestCloudlabsScratch(unittest.TestCase):

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

    def getLabs(self):
        cloudlabs = CloudlabsApi.curl_auth(self.curl_login)
        tenant = cloudlabs.tenants["Databricks – Training"]
        all_labs = tenant.labs.list(include_test=True)
        test_labs = [lab for lab in all_labs if "User Acceptance Environment" in lab["Title"]]

        def load_details(lab):
            lab = tenant.labs.refresh(lab)
            workspaces = tenant.labs.workspaces(lab)
            assert len(workspaces) == 1, "Invalid assumption of exactly 1 workspace"
            lab["Workspace"] = workspaces[0]
            lab["Instructors"] = tenant.instructors.get_instructors_for_lab(lab)
            lab["Title"] = lab["Title"][0:lab["Title"].find(" | ")]
            return lab

        test_labs = tenant.do_batch(load_details, test_labs)
        test_labs = [lab for lab in test_labs if "2023-04-25" in lab["CreatedTime"]]
        return test_labs

    def testWriteLabTokens(self):
        from pprint import pprint
        print()
        test_labs = self.getLabs()
        table_mapping = {
            "name": "Title",
            "url": lambda l: l["Workspace"].url[:-len("/api/")],
            "token": lambda l: l["Workspace"].token,
        }
        results = []
        for lab in test_labs:
            row = {}
            for title, field in table_mapping.items():
                if callable(field):
                    row[title] = field(lab)
                else:
                    row[title] = lab[field]
            results.append(row)
        pprint(results, indent=4, sort_dicts=False)

    def testWriteSpreadsheet(self):
        import csv
        from sys import stdout
        print()
        test_labs = self.getLabs()
        table_mapping = {
            "Created": "CreatedTime",
            "Cloud": lambda l: l["Workspace"].cloud,
            "Template": "TemplateId",
            "Lab ID": "Id",
            "Lab Name": "Title",
            "Bitly URL": "BitLink",
            "Databricks Workspace URL": lambda l: l["Workspace"].url[:-len("/api/")],
            "Databricks Workspace Token": lambda l: l["Workspace"].token,
            "Control Panel": lambda l: f"https://admin.cloudlabs.ai/#/home/odl/controlpanel/{l['UniqueName']}",
        }
        csvwriter = csv.writer(stdout, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(table_mapping.keys())
        for lab in test_labs:
            row = []
            for title, field in table_mapping.items():
                if callable(field):
                    row.append(field(lab))
                else:
                    row.append(lab[field])
            csvwriter.writerow(row)
