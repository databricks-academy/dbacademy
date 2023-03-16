import unittest, pytest
from dbacademy.workspaces_3_0.workspace_config_classe import WorkspaceConfig


class TestWorkspaceConfig(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def setup_test(self):
        from dbacademy.workspaces_3_0.event_config_class import EventConfig
        self.dbc_urls = [
            "https://labs.training.databricks.com/api/courses?course=apache-spark-programming-with-databricks&version=vCURRENT&artifact=lessons.dbc&token=abcd",
            "https://labs.training.databricks.com/api/courses?course=example-course&version=vLATEST&artifact=lessons.dbc&token=abcd",
            "https://labs.training.databricks.com/api/courses?course=data-engineering-with-databricks&version=V1.2.3&artifact=lessons.dbc&token=abcd",
        ]
        self.datasets = [
            "example-course",
            "machine-learning-in-production",
        ]
        self.courses = [
            "example-course",
            "machine-learning-in-production",
        ]
        self.event_config = EventConfig(event_id=1234, max_participants=100, description="Fun times!")

    def test_create_workspace_config_no_groups(self):
        from dbacademy_test.workspaces_3_0 import test_index_error

        groups = {
            "instructors": [0, 1],
            "analysts": ["analysts-1@databricks.com", "analysts-2@databricks.com"],
        }

        entitlements = {
            "allow-cluster-create": True,
            "allow-super-powers": False,
        }

        max_users = 150
        expected_users = max_users + 3  # script_account, analyst-1, analyst-2
        workspace = WorkspaceConfig(max_users=max_users, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=entitlements, groups=groups, workspace_name_pattern="classroom-{event_id}-{workspace_number}")

        self.assertEqual(expected_users, len(workspace.usernames))
        self.assertEqual("11.3.x-scala2.12", workspace.default_dbr)
        self.assertEqual("i3.xlarge", workspace.default_node_type_id)

        self.assertEqual(2, len(workspace.courses))
        self.assertEqual(self.courses[0], workspace.courses[0])
        self.assertEqual(self.courses[1], workspace.courses[1])

        self.assertEqual(2, len(workspace.datasets))
        self.assertEqual(self.datasets[0], workspace.datasets[0])
        self.assertEqual(self.datasets[1], workspace.datasets[1])

        self.assertEqual(3, len(workspace.dbc_urls))
        self.assertEqual(self.dbc_urls[0], workspace.dbc_urls[0])
        self.assertEqual(self.dbc_urls[1], workspace.dbc_urls[1])
        self.assertEqual(self.dbc_urls[2], workspace.dbc_urls[2])

        self.assertIsNone(workspace.event_config)
        self.assertIsNone(workspace.workspace_number)
        self.assertIsNone(workspace.name)

        self.assertEqual("default", workspace.credentials_name)
        self.assertEqual("us-west-2", workspace.storage_configuration)

        self.assertEqual(2, len(workspace.entitlements))
        self.assertTrue("allow-cluster-create" in workspace.entitlements)
        self.assertTrue(workspace.entitlements.get("allow-cluster-create"))

        self.assertTrue("allow-super-powers" in workspace.entitlements)
        self.assertFalse(workspace.entitlements.get("allow-super-powers"))

        self.assertTrue(2, len(workspace.groups))

        self.assertTrue("instructors" in workspace.groups)
        instructors = workspace.groups.get("instructors")
        self.assertEqual(["class+000@databricks.com", "class+001@databricks.com"], instructors)

        self.assertTrue("analysts" in workspace.groups)
        analysts = workspace.groups.get("analysts")
        self.assertEqual(["analysts-1@databricks.com", "analysts-2@databricks.com"], analysts)

        # Spotcheck users
        self.assertEqual(expected_users, len(workspace.usernames))
        self.assertEqual("class+000@databricks.com", workspace.usernames[0])   # The instructor
        self.assertEqual("class+020@databricks.com", workspace.usernames[20])
        self.assertEqual("class+040@databricks.com", workspace.usernames[40])
        self.assertEqual("class+060@databricks.com", workspace.usernames[60])
        self.assertEqual("class+080@databricks.com", workspace.usernames[80])
        self.assertEqual("class+100@databricks.com", workspace.usernames[100])
        self.assertEqual("class+120@databricks.com", workspace.usernames[120])
        self.assertEqual("class+149@databricks.com", workspace.usernames[149])

        self.assertEqual("class+150@databricks.com", workspace.usernames[150])

        self.assertEqual("analysts-1@databricks.com", workspace.usernames[151])  # Analyst #1
        self.assertEqual("analysts-2@databricks.com", workspace.usernames[152])  # Analyst #2

        test_index_error(self, lambda: workspace.usernames[153])

    def test_create_workspace_config_username_pattern(self):
        from dbacademy_test.workspaces_3_0 import test_assertion_error

        workspace = WorkspaceConfig(max_users=250, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}")
        self.assertEqual("class+{student_number}@databricks.com", workspace.username_pattern)

        for i, username in enumerate(workspace.usernames):
            if username != "class+analyst@databricks.com":
                self.assertEqual(f"class+{i:03d}@databricks.com", username)

        ###################################################

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "username_pattern" must be a string value, found <class 'int'>.""", lambda: WorkspaceConfig(max_users=250, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern=0, groups=dict(), entitlements=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))
        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "username_pattern" must be a string value, found <class 'NoneType'>.""", lambda: WorkspaceConfig(max_users=250, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern=None, groups=dict(), entitlements=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))
        test_assertion_error(self, """The parameter "username_pattern" must have a length > 0, found "".""", lambda: WorkspaceConfig(max_users=250, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="", groups=dict(), entitlements=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))

    def test_create_workspace_config_workspace_pattern(self):
        from dbacademy_test.workspaces_3_0 import test_assertion_error

        workspace = WorkspaceConfig(max_users=250, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}")
        self.assertEqual("classroom-{event_id}-{workspace_number}", workspace.workspace_name_pattern)

        workspace = WorkspaceConfig(max_users=250, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{workspace_number}")
        self.assertEqual("classroom-{workspace_number}", workspace.workspace_name_pattern)

        ###################################################

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "workspace_name_pattern" must be a string value, found <class 'int'>.""", lambda: WorkspaceConfig(max_users=250, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern=0))
        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "workspace_name_pattern" must be a string value, found <class 'NoneType'>.""", lambda: WorkspaceConfig(max_users=250, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern=None))
        test_assertion_error(self, """The parameter "workspace_name_pattern" must have a length > 0, found "".""", lambda: WorkspaceConfig(max_users=250, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern=""))

        test_assertion_error(self, """Expected the parameter "workspace_name_pattern" to contain "{workspace_number}", found "classroom-{event_id}".""", lambda: WorkspaceConfig(max_users=250, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}"))

    def test_create_workspace_config_workspace_number(self):
        from dbacademy_test.workspaces_3_0 import test_assertion_error

        workspace = WorkspaceConfig(max_users=250, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}")
        self.assertIsNone(workspace.workspace_number)

        workspace.init(event_config=self.event_config, workspace_number=1)
        self.assertEqual(1, workspace.workspace_number)

        ###################################################

        workspace = WorkspaceConfig(max_users=250, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}")
        self.assertIsNone(workspace.workspace_number)

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "workspace_number" must be an integral value, found <class 'str'>.""", lambda: workspace.init(event_config=self.event_config, workspace_number="0"))
        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "workspace_number" must be an integral value, found <class 'NoneType'>.""", lambda: workspace.init(event_config=self.event_config, workspace_number=None))
        test_assertion_error(self, """The parameter "workspace_number" must be greater than zero, found "0".""", lambda: workspace.init(event_config=self.event_config, workspace_number=0))

    def test_create_workspace_config_max_users(self):
        from dbacademy_test.workspaces_3_0 import test_assertion_error

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "max_users" must be an integral value, found <class 'str'>.""", lambda: WorkspaceConfig(max_users="0", courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))
        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "max_users" must be an integral value, found <class 'NoneType'>.""", lambda: WorkspaceConfig(max_users=None, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))
        test_assertion_error(self, """The parameter "max_users" must be greater than zero, found "0".""", lambda: WorkspaceConfig(max_users=0, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))

    def test_create_workspace_config_default_dbr(self):
        from dbacademy_test.workspaces_3_0 import test_assertion_error

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "default_dbr" must be a string value, found <class 'int'>.""", lambda: WorkspaceConfig(max_users=250, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr=0, dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))
        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "default_dbr" must be a string value, found <class 'NoneType'>.""", lambda: WorkspaceConfig(max_users=250, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr=None, dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))
        test_assertion_error(self, """Invalid DBR format, found "abc".""", lambda: WorkspaceConfig(max_users=250, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="abc", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))

    def test_create_workspace_config_default_node_type_id(self):
        from dbacademy_test.workspaces_3_0 import test_assertion_error

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "default_node_type_id" must be a string value, found <class 'int'>.""", lambda: WorkspaceConfig(max_users=250, courses=self.courses, datasets=self.datasets, default_node_type_id=0, default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))
        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "default_node_type_id" must be a string value, found <class 'NoneType'>.""", lambda: WorkspaceConfig(max_users=250, courses=self.courses, datasets=self.datasets, default_node_type_id=None, default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))
        test_assertion_error(self, """Invalid node type, found "abc".""", lambda: WorkspaceConfig(max_users=250, courses=self.courses, datasets=self.datasets, default_node_type_id="abc", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))

    def test_create_workspace_config_courses(self):
        from dbacademy_test.workspaces_3_0 import test_assertion_error

        workspace = WorkspaceConfig(max_users=250, courses=None, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}")
        self.assertEqual(list, type(workspace.courses))
        self.assertEqual([], workspace.courses)

        workspace = WorkspaceConfig(max_users=250, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}")
        self.assertEqual(list, type(workspace.courses))
        self.assertEqual(2, len(workspace.courses))
        self.assertEqual("example-course", workspace.courses[0])

        workspace = WorkspaceConfig(max_users=250, courses=["example-course", "whatever"], datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}")
        self.assertEqual(list, type(workspace.courses))
        self.assertEqual(2, len(workspace.courses))
        self.assertEqual("example-course", workspace.courses[0])
        self.assertEqual("whatever", workspace.courses[1])

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "courses" must be a string value, found <class 'int'>.""", lambda: WorkspaceConfig(max_users=250, courses=1, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))

    def test_create_workspace_config_datasets(self):
        from dbacademy_test.workspaces_3_0 import test_assertion_error

        workspace = WorkspaceConfig(max_users=250, courses=self.courses, datasets=None, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}")
        self.assertEqual(list, type(workspace.datasets))
        self.assertEqual([], workspace.datasets)

        workspace = WorkspaceConfig(max_users=250, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}")
        self.assertEqual(list, type(workspace.datasets))
        self.assertEqual(2, len(workspace.datasets))
        self.assertEqual("example-course", workspace.datasets[0])

        workspace = WorkspaceConfig(max_users=250, courses=self.courses, datasets=["example-course", "whatever"], default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}")
        self.assertEqual(list, type(workspace.datasets))
        self.assertEqual(2, len(workspace.datasets))
        self.assertEqual("example-course", workspace.datasets[0])
        self.assertEqual("whatever", workspace.datasets[1])

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "datasets" must be a string value, found <class 'int'>.""", lambda: WorkspaceConfig(max_users=250, courses=self.courses, datasets=1, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))

    def test_create_workspace_config_dbc_urls(self):
        from dbacademy_test.workspaces_3_0 import test_assertion_error

        workspace_config = WorkspaceConfig(max_users=250, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=None, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}")
        self.assertEqual(list, type(workspace_config.dbc_urls))
        self.assertEqual(0, len(workspace_config.dbc_urls))

        single_url = "https://labs.training.databricks.com/api/courses?course=example-course&version=vLATEST&artifact=lessons.dbc&token=abcd"
        workspace_config = WorkspaceConfig(max_users=250, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=single_url, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}")
        self.assertEqual(list, type(workspace_config.dbc_urls))
        self.assertEqual(1, len(workspace_config.dbc_urls))
        self.assertEqual([single_url], workspace_config.dbc_urls)

        # Missing prefix
        dbc_urls = ["example-course/vCURRENT?token=z7jimi88x8&expires_at_utc=2023-02-25T20:15:51"]
        test_assertion_error(self, f"""Item 0 for the parameter "dbc_urls" must start with "https://labs.training.databricks.com/api/courses?", found "{dbc_urls[0]}".""",
                             lambda: WorkspaceConfig(max_users=99, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))

        # Missing token
        dbc_urls = ["https://labs.training.databricks.com/api/courses?course=apache-spark-programming-with-databricks&version=vCURRENT&artifact=lessons.dbc"]
        test_assertion_error(self, f"""Item 0 for the parameter "dbc_url" is missing the "token" query parameter, found "{dbc_urls[0]}".""",
                             lambda: WorkspaceConfig(max_users=99, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))

        # Missing course
        dbc_urls = ["https://labs.training.databricks.com/api/courses?version=vCURRENT&artifact=lessons.dbc&token=abcd"]
        test_assertion_error(self, f"""Item 0 for the parameter "dbc_url" is missing the "course" query parameter, found "{dbc_urls[0]}".""",
                             lambda: WorkspaceConfig(max_users=99, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))

        # Missing version
        dbc_urls = ["https://labs.training.databricks.com/api/courses?course=apache-spark-programming-with-databricks&artifact=lessons.dbc&token=abcd"]
        test_assertion_error(self, f"""Item 0 for the parameter "dbc_url" is missing the "version" query parameter, found "{dbc_urls[0]}".""",
                             lambda: WorkspaceConfig(max_users=99, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))

        # Missing artifact
        dbc_urls = ["https://labs.training.databricks.com/api/courses?course=apache-spark-programming-with-databricks&version=vCURRENT&token=abcd"]
        test_assertion_error(self, f"""Item 0 for the parameter "dbc_url" is missing the "artifact" query parameter, found "{dbc_urls[0]}".""",
                             lambda: WorkspaceConfig(max_users=99, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=dbc_urls, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))

    def test_create_workspace_config_credentials_name(self):
        from dbacademy_test.workspaces_3_0 import test_assertion_error

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "credentials_name" must be a string value, found <class 'int'>.""", lambda: WorkspaceConfig(max_users=150, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name=0, storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))
        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "credentials_name" must be a string value, found <class 'NoneType'>.""", lambda: WorkspaceConfig(max_users=150, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name=None, storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))
        test_assertion_error(self, """The parameter "credentials_name" must be specified, found "".""", lambda: WorkspaceConfig(max_users=150, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))

    def test_create_workspace_config_storage_config(self):
        from dbacademy_test.workspaces_3_0 import test_assertion_error

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "storage_configuration" must be a string value, found <class 'int'>.""", lambda: WorkspaceConfig(max_users=150, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration=0, username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))
        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "storage_configuration" must be a string value, found <class 'NoneType'>.""", lambda: WorkspaceConfig(max_users=150, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration=None, username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))
        test_assertion_error(self, """The parameter "storage_configuration" must be specified, found "".""", lambda: WorkspaceConfig(max_users=150, courses=self.courses, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=self.dbc_urls, credentials_name="default", storage_configuration="", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), groups=dict(), workspace_name_pattern="classroom-{event_id}-{workspace_number}"))


if __name__ == '__main__':
    unittest.main()
