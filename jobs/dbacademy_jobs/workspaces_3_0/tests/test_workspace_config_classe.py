import unittest
import pytest
from dbacademy_jobs.workspaces_3_0.support.workspace_config_classe import WorkspaceConfig


class TestWorkspaceConfig(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def setup_test(self):
        self.dbc_urls = [
            "https://labs.training.databricks.com/api/courses?course=apache-spark-programming-with-databricks&version=vCURRENT&artifact=lessons.dbc&token=abcd",
            "https://labs.training.databricks.com/api/courses?course=example-course&version=vLATEST&artifact=lessons.dbc&token=abcd",
            "https://labs.training.databricks.com/api/courses?course=data-engineering-with-databricks&version=V1.2.3&artifact=lessons.dbc&token=abcd",
        ]
        self.datasets = [
            "example-course",
            "machine-learning-in-production",
        ]
        self.course_definitions = [
            "course=example-course",
            "course=machine-learning-in-production",
        ]

    def test_create_workspace_config_no_groups(self):
        from dbacademy_jobs.workspaces_3_0.tests import test_index_error

        workspace_group = {
            "instructors": [0, 1],
            "analysts": ["analysts-1@databricks.com", "analysts-2@databricks.com"],
        }

        entitlements = {
            "allow-cluster-create": True,
            "allow-super-powers": False,
        }

        max_participants = 150
        expected_users = max_participants + 3  # script_account, analyst-1, analyst-2
        workspace = WorkspaceConfig(max_participants=max_participants,
                                    course_definitions=self.course_definitions,
                                    cds_api_token="asdf123",
                                    datasets=self.datasets,
                                    default_node_type_id="i3.xlarge",
                                    default_dbr="11.3.x-scala2.12",
                                    credentials_name="default",
                                    storage_configuration="us-west-2",
                                    username_pattern="class+{student_number}@databricks.com",
                                    entitlements=entitlements,
                                    workspace_group=workspace_group,
                                    workspace_name_pattern="classroom-{workspace_number}")

        self.assertEqual(expected_users, len(workspace.usernames))
        self.assertEqual("11.3.x-scala2.12", workspace.default_dbr)
        self.assertEqual("i3.xlarge", workspace.default_node_type_id)

        self.assertEqual(2, len(workspace.course_definitions))
        self.assertEqual(self.course_definitions[0], workspace.course_definitions[0])
        self.assertEqual(self.course_definitions[1], workspace.course_definitions[1])

        self.assertEqual(2, len(workspace.datasets))
        self.assertEqual(self.datasets[0], workspace.datasets[0])
        self.assertEqual(self.datasets[1], workspace.datasets[1])

        self.assertIsNone(workspace.workspace_number)
        self.assertIsNone(workspace.name)

        self.assertEqual("default", workspace.credentials_name)
        self.assertEqual("us-west-2", workspace.storage_configuration)

        self.assertEqual(2, len(workspace.entitlements))
        self.assertTrue("allow-cluster-create" in workspace.entitlements)
        self.assertTrue(workspace.entitlements.get("allow-cluster-create"))

        self.assertTrue("allow-super-powers" in workspace.entitlements)
        self.assertFalse(workspace.entitlements.get("allow-super-powers"))

        self.assertTrue(2, len(workspace.workspace_group))

        self.assertTrue("instructors" in workspace.workspace_group)
        instructors = workspace.workspace_group.get("instructors")
        self.assertEqual(["class+000@databricks.com", "class+001@databricks.com"], instructors)

        self.assertTrue("analysts" in workspace.workspace_group)
        analysts = workspace.workspace_group.get("analysts")
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
        from dbacademy_jobs.workspaces_3_0.tests import test_assertion_error

        workspace = WorkspaceConfig(max_participants=250,
                                    course_definitions=self.course_definitions,
                                    cds_api_token="asdf123",
                                    datasets=self.datasets,
                                    default_node_type_id="i3.xlarge",
                                    default_dbr="11.3.x-scala2.12",
                                    credentials_name="default",
                                    storage_configuration="us-west-2",
                                    username_pattern="class+{student_number}@databricks.com",
                                    entitlements=None,
                                    workspace_group=None,
                                    workspace_name_pattern="classroom-{workspace_number}")
        self.assertEqual("class+{student_number}@databricks.com", workspace.username_pattern)

        for i, username in enumerate(workspace.usernames):
            if username != "class+analyst@databricks.com":
                self.assertEqual(f"class+{i:03d}@databricks.com", username)

        ###################################################

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "username_pattern" must be a string value, found <class 'int'>.""", lambda: WorkspaceConfig(max_participants=250, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern=0, workspace_group=None, entitlements=dict(), workspace_name_pattern="classroom-{workspace_number}"))
        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "username_pattern" must be a string value, found <class 'NoneType'>.""", lambda: WorkspaceConfig(max_participants=250, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern=None, workspace_group=None, entitlements=dict(), workspace_name_pattern="classroom-{workspace_number}"))
        test_assertion_error(self, """The parameter "username_pattern" must have a length > 0, found "".""", lambda: WorkspaceConfig(max_participants=250, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="", workspace_group=None, entitlements=dict(), workspace_name_pattern="classroom-{workspace_number}"))

    def test_create_workspace_config_workspace_pattern(self):
        from dbacademy_jobs.workspaces_3_0.tests import test_assertion_error

        workspace = WorkspaceConfig(max_participants=250, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
        self.assertEqual("classroom-{workspace_number}", workspace.workspace_name_pattern)

        workspace = WorkspaceConfig(max_participants=250, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
        self.assertEqual("classroom-{workspace_number}", workspace.workspace_name_pattern)

        ###################################################

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "workspace_name_pattern" must be a string value, found <class 'int'>.""", lambda: WorkspaceConfig(max_participants=250, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern=0))
        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "workspace_name_pattern" must be a string value, found <class 'NoneType'>.""", lambda: WorkspaceConfig(max_participants=250, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern=None))
        test_assertion_error(self, """The parameter "workspace_name_pattern" must have a length > 0, found "".""", lambda: WorkspaceConfig(max_participants=250, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern=""))

        test_assertion_error(self, """Expected the parameter "workspace_name_pattern" to contain "{workspace_number}", found "classroom-{whatever}".""", lambda: WorkspaceConfig(max_participants=250, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{whatever}"))

    def test_create_workspace_config_workspace_number(self):
        from dbacademy_jobs.workspaces_3_0.tests import test_assertion_error

        workspace = WorkspaceConfig(max_participants=250, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
        self.assertIsNone(workspace.workspace_number)

        ###################################################

        workspace = WorkspaceConfig(workspace_number=99, max_participants=250, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
        self.assertEqual(99, workspace.workspace_number)

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "workspace_number" must be an integral value, found <class 'str'>.""",
                             lambda: WorkspaceConfig(workspace_number="0", max_participants=250, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}"))

        test_assertion_error(self, """The parameter "workspace_number" must be greater than zero, found "0".""",
                             lambda: WorkspaceConfig(workspace_number=0, max_participants=250, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}"))

    def test_create_workspace_config_max_participants(self):
        from dbacademy_jobs.workspaces_3_0.tests import test_assertion_error
        from dbacademy.common import validator

        # noinspection PyTypeChecker
        test_assertion_error(self, validator.E_TYPE, lambda: WorkspaceConfig(max_participants="0", course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}"))
        # noinspection PyTypeChecker
        test_assertion_error(self, validator.E_NOT_NONE, lambda: WorkspaceConfig(max_participants=None, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}"))
        test_assertion_error(self, validator.E_MIN_V, lambda: WorkspaceConfig(max_participants=0, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}"))

    def test_create_workspace_config_default_dbr(self):
        from dbacademy_jobs.workspaces_3_0.tests import test_assertion_error

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "default_dbr" must be a string value, found <class 'int'>.""", lambda: WorkspaceConfig(max_participants=250, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr=0, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}"))
        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "default_dbr" must be a string value, found <class 'NoneType'>.""", lambda: WorkspaceConfig(max_participants=250, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr=None, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}"))
        test_assertion_error(self, """Invalid DBR format, found "abc".""", lambda: WorkspaceConfig(max_participants=250, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="abc", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}"))

    def test_create_workspace_config_default_node_type_id(self):
        from dbacademy_jobs.workspaces_3_0.tests import test_assertion_error

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "default_node_type_id" must be a string value, found <class 'int'>.""", lambda: WorkspaceConfig(max_participants=250, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id=0, default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}"))
        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "default_node_type_id" must be a string value, found <class 'NoneType'>.""", lambda: WorkspaceConfig(max_participants=250, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id=None, default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}"))
        test_assertion_error(self, """Invalid node type, found "abc".""", lambda: WorkspaceConfig(max_participants=250, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="abc", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}"))

    def test_create_workspace_config_courses(self):
        from dbacademy_jobs.workspaces_3_0.tests import test_assertion_error

        workspace = WorkspaceConfig(max_participants=250, course_definitions=None, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=None, workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
        self.assertEqual(list, type(workspace.course_definitions))
        self.assertEqual([], workspace.course_definitions)

        workspace = WorkspaceConfig(max_participants=250, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
        self.assertEqual(list, type(workspace.course_definitions))
        self.assertEqual(2, len(workspace.course_definitions))
        self.assertEqual("course=example-course", workspace.course_definitions[0])

        course_definitions = [
            "course=welcome",
            "course=example-course&version=v1.1.6",
            "course=template-course&version=v1.0.0&artifact=template-course.dbc",
            "https://labs.training.databricks.com/api/v1/courses/download.dbc?course=ml-in-production&version=v3.4.5&artifact=ml-in-production-v3.4.5-notebooks.dbc",
        ]

        workspace = WorkspaceConfig(max_participants=250, course_definitions=course_definitions, cds_api_token="asdf1234", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
        self.assertEqual(list, type(workspace.course_definitions))
        self.assertEqual(4, len(workspace.course_definitions))
        self.assertEqual("course=welcome", workspace.course_definitions[0])
        self.assertEqual("course=example-course&version=v1.1.6", workspace.course_definitions[1])
        self.assertEqual("course=template-course&version=v1.0.0&artifact=template-course.dbc", workspace.course_definitions[2])
        self.assertEqual("https://labs.training.databricks.com/api/v1/courses/download.dbc?course=ml-in-production&version=v3.4.5&artifact=ml-in-production-v3.4.5-notebooks.dbc", workspace.course_definitions[3])

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "course_definitions" must be a string value or a list of strings, found <class 'int'>.""", lambda: WorkspaceConfig(max_participants=250, course_definitions=1, datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}"))

        self.assertIsNotNone(workspace.dbc_urls)
        self.assertEqual(4, len(workspace.dbc_urls))
        self.assertEqual("https://labs.training.databricks.com/api/v1/courses/download.dbc?course=welcome&token=asdf1234", workspace.dbc_urls[0])
        self.assertEqual("https://labs.training.databricks.com/api/v1/courses/download.dbc?course=example-course&version=v1.1.6&token=asdf1234", workspace.dbc_urls[1])
        self.assertEqual("https://labs.training.databricks.com/api/v1/courses/download.dbc?course=template-course&version=v1.0.0&artifact=template-course.dbc&token=asdf1234", workspace.dbc_urls[2])
        self.assertEqual("https://labs.training.databricks.com/api/v1/courses/download.dbc?course=ml-in-production&version=v3.4.5&artifact=ml-in-production-v3.4.5-notebooks.dbc&token=asdf1234", workspace.dbc_urls[3])

        try:
            course_definitions = ["course=ml-in-production&token=asfd123"]
            WorkspaceConfig(max_participants=250, course_definitions=course_definitions, cds_api_token="asdf1234", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
        except AssertionError as e:
            self.assertEqual("""The CDS API token should not be specified in the courseware definition, please use the "cds_api_token" parameter" instead.""", str(e))

        try:
            course_definitions = ["https://dev.training.databricks.com/api/v1/courses/download.dbc?course=ml-in-production"]
            WorkspaceConfig(max_participants=250, course_definitions=course_definitions, cds_api_token="asdf1234", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
        except AssertionError as e:
            self.assertEqual("""Item 0 for the parameter "dbc_urls" must start with "https://labs.training.databricks.com/api/v1/courses/download.dbc?", found "https://dev.training.databricks.com/api/v1/courses/download.dbc?course=ml-in-production&token=asdf1234".""", str(e))

    def test_create_workspace_config_datasets(self):
        from dbacademy_jobs.workspaces_3_0.tests import test_assertion_error

        workspace = WorkspaceConfig(max_participants=250, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=None, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
        self.assertEqual(list, type(workspace.datasets))
        self.assertEqual(["example-course", "machine-learning-in-production"], workspace.datasets)

        workspace = WorkspaceConfig(max_participants=250, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
        self.assertEqual(list, type(workspace.datasets))
        self.assertEqual(2, len(workspace.datasets))
        self.assertEqual("example-course", workspace.datasets[0])
        self.assertEqual("machine-learning-in-production", workspace.datasets[1])

        workspace = WorkspaceConfig(max_participants=250, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=["example-course", "whatever"], default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
        self.assertEqual(list, type(workspace.datasets))
        self.assertEqual(3, len(workspace.datasets))
        self.assertEqual("example-course", workspace.datasets[0])
        self.assertEqual("whatever", workspace.datasets[1])
        self.assertEqual("machine-learning-in-production", workspace.datasets[2])

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "datasets" must be a string value, found <class 'int'>.""", lambda: WorkspaceConfig(max_participants=250, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=1, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}"))

    def test_create_workspace_config_credentials_name(self):
        from dbacademy_jobs.workspaces_3_0.tests import test_assertion_error

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "credentials_name" must be a string value, found <class 'int'>.""", lambda: WorkspaceConfig(max_participants=150, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name=0, storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}"))
        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "credentials_name" must be a string value, found <class 'NoneType'>.""", lambda: WorkspaceConfig(max_participants=150, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name=None, storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}"))
        test_assertion_error(self, """The parameter "credentials_name" must be specified, found "".""", lambda: WorkspaceConfig(max_participants=150, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}"))

    def test_create_workspace_config_storage_config(self):
        from dbacademy_jobs.workspaces_3_0.tests import test_assertion_error

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "storage_configuration" must be a string value, found <class 'int'>.""", lambda: WorkspaceConfig(max_participants=150, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration=0, username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}"))
        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "storage_configuration" must be a string value, found <class 'NoneType'>.""", lambda: WorkspaceConfig(max_participants=150, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration=None, username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}"))
        test_assertion_error(self, """The parameter "storage_configuration" must be specified, found "".""", lambda: WorkspaceConfig(max_participants=150, course_definitions=self.course_definitions, cds_api_token="asdf123", datasets=self.datasets, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}"))


if __name__ == '__main__':
    unittest.main()
