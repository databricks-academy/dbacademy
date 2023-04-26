import unittest

from dbacademy.cloudlabs import CloudlabsApi


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
        raise ValueError("Environment variable CLOUDLABS_CURL must be set, or the file .curl_login must exist")

    def testGetLabs(self):
        cloudlabs = CloudlabsApi.curl_auth(self.curl_login)
        tenant = cloudlabs.tenants["Databricks – Training"]
        all_labs = tenant.labs.list(include_test=True)
        test_labs = [lab for lab in all_labs if "User Acceptance Environment" in lab["Title"]]
        ml_labs = [lab["Title"] for lab in all_labs if "Scalable Machine" in lab["Title"]]
        print()
        for lab in ml_labs:
            print(lab)

        def load_details(lab):
            lab = tenant.labs.refresh(lab)
            workspaces = tenant.labs.workspaces(lab)
            assert len(workspaces) == 1, "Invalid assumption of exactly 1 workspace"
            lab["Workspace"] = workspaces[0]
            lab["Instructors"] = tenant.instructors.get_instructors_for_lab(lab)
            return lab

        test_labs = tenant.do_batch(load_details, test_labs)
        table_mapping = {
            "Created": "CreatedTime",
            "Cloud": lambda lab: lab["Workspace"].cloud,
            "Template": "TemplateId",
            "Lab ID": "Id",
            "Template Name": "TemplateName",
            "Lab Name": "Title",
            "Bitly URL": "BitLink",
            "Databricks Workspace URL": lambda lab: lab["Workspace"].url[:-len("/api/")],
            "Control Panel": lambda lab: f"https://admin.cloudlabs.ai/#/home/odl/controlpanel/{lab['UniqueName']}",
        }
        import csv
        from sys import stdout
        print()
        csvwriter = csv.writer(stdout, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(table_mapping.keys())
        for lab in test_labs:
            row = []
            for item in table_mapping.values():
                if callable(item):
                    row.append(item(lab))
                else:
                    row.append(lab[item])
            csvwriter.writerow(row)
